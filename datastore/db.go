package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	outFileName = "segment"
	bufSize     = 8192
)

var ErrNotFound = fmt.Errorf("record does not exist")
var maxFileSize = 10 * 1024 * 1024

type hashIndex map[string]int64

type Db struct {
	out             *os.File
	outPath         string
	outOffset       int64
	fileNumber      int
	segmentNumbers  []int
	dir             string
	lastChangedEl   string
	mergingSegments []string
	mergeMu         sync.Mutex
	putCh           chan entry
	getCh           chan string
	getOffsetCh     chan int64
	finishMergeCh   chan hashIndex
	index           hashIndex
}

func NewDb(dir string) (*Db, error) {
	outputPath := filepath.Join(dir, outFileName+"-1")
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db := &Db{
		outPath:         outputPath,
		out:             f,
		index:           make(hashIndex),
		segmentNumbers:  make([]int, 0),
		mergingSegments: make([]string, 0),
		fileNumber:      1,
		dir:             dir,
		putCh:           make(chan entry),
		getCh:           make(chan string),
		getOffsetCh:     make(chan int64),
		finishMergeCh:   make(chan hashIndex),
	}
	db.segmentNumbers = db.getSegmentNumbers()
	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	go db.OperationMonitor()

	return db, nil
}

func (db *Db) recover() error {
	segments, err := db.getAllSegments()
	if err != nil {
		return err
	}
	for i, segment := range segments {
		if err := db.processSegment(segment, i == len(segments)-1); err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) processSegment(segment string, isLastSegment bool) error {
	db.outOffset = 0
	fileNumber, err := strconv.Atoi(segment[8:])
	if err != nil {
		return err
	}
	input, err := os.Open(segment)
	if err != nil {
		return err
	}
	defer input.Close()
	in := bufio.NewReaderSize(input, bufSize)
	for {
		header, err := in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				if isLastSegment {
					return db.prepareLastSegment(segment, fileNumber)
				}
				return nil
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)
		data := make([]byte, size)
		n, err := in.Read(data)
		if err != nil {
			return err
		}
		if n != int(size) {
			return fmt.Errorf("corrupted file")
		}
		var e entry[string]
		e.Decode(data)
		db.index[e.key] = db.outOffset + int64((fileNumber-1)*maxFileSize)
		db.outOffset += int64(n)
	}
}

func (db *Db) prepareLastSegment(segment string, fileNumber int) error {
	err := db.out.Close()
	if err != nil {
		return err
	}
	db.fileNumber = fileNumber
	db.out, err = os.OpenFile(segment, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	return nil
}

func (db *Db) OperationMonitor() {
	for {
		select {
		case e := <-db.putCh:
			db.makeRecord(e)
		case key := <-db.getCh:
			offset, _ := db.getOffset(key)
			db.getOffsetCh <- offset
		case index := <-db.finishMergeCh:
			err := db.finishMergingSegments(index)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	db.getCh <- key
	offset := <-db.getOffsetCh
	reader, file, err := db.getReaderByOffset(offset)
	if err != nil {
		return "", err
	}
	defer file.Close()
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}

	stingValue, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("Value does not match expected type: string")
	}

	return stingValue, nil
}

func (db *Db) getReaderByOffset(offset int64) (*bufio.Reader, *os.File, error) {
	fileNumber := int(math.Floor(float64(offset/int64(maxFileSize)))) + 1
	if !db.checkFileExistence(fileNumber) {
		fileNumber = 1
	}
	position := offset - int64((fileNumber-1)*maxFileSize)
	outPath := filepath.Join(db.dir, outFileName+"-"+strconv.FormatInt(int64(fileNumber), 10))
	file, err := os.Open(outPath)
	if err != nil {
		return nil, file, err
	}
	_, err = file.Seek(position, 0)
	if err != nil {
		return nil, file, err
	}
	return bufio.NewReader(file), file, nil
}

func (db *Db) getOffset(key string) (int64, error) {
	offset, ok := db.index[key]
	if !ok {
		return offset, ErrNotFound
	}
	return offset, nil
}

func (db *Db) Put(key, value string) error {
	e := entry[string]{
		key:   key,
		value: value,
	}
	db.putCh <- e
	return nil
}

func (db *Db) makeRecord(e entry) {
	fileInfo, err := db.out.Stat()
	if err != nil {
		return
	}
	if int64(len(e.Encode())) > (int64(maxFileSize) - fileInfo.Size()) {
		err = db.createNewSegment()
		if err != nil {
			return
		}
	}
	n, err := db.out.Write(e.Encode())
	if err == nil {
		db.index[e.key] = db.outOffset + int64((db.fileNumber-1)*maxFileSize)
		db.outOffset += int64(n)
	}
}

func (db *Db) GetInt64(key string) (int64, error) {
	offset, ok := db.index[key]
	fileNumber := int(math.Floor(float64(db.index[key]/int64(maxFileSize)))) + 1
	if !db.checkFileExistence(fileNumber) {
		fileNumber = 1
	}
	outPath := filepath.Join(db.dir, outFileName+"-"+strconv.FormatInt(int64(fileNumber), 10))
	if !ok {
		return 0, ErrNotFound
	}
	position := offset - int64((fileNumber-1)*maxFileSize)
	file, err := os.Open(outPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return 0, err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return 0, err
	}

	int64Value, ok := value.(int64)
	if !ok {
		return 0, fmt.Errorf("Value does not match expected type: int64")
	}
	return int64Value, nil
}

func (db *Db) PutInt64(key string, value int64) error {
	e := entry[int64]{
		key:   key,
		value: value,
	}
	fileInfo, err := db.out.Stat()
	if err != nil {
		return err
	}
	if int64(len(e.Encode())) > (int64(maxFileSize) - fileInfo.Size()) {
		err = db.createNewSegment()
		if err != nil {
			return err
		}
	}
	n, err := db.out.Write(e.Encode())
	if err == nil {
		db.index[key] = db.outOffset + int64((db.fileNumber-1)*maxFileSize)
		db.outOffset += int64(n)
	}

	return err
}

func (db *Db) createNewSegment() error {
	err := db.out.Close()
	if err != nil {
		return err
	}
	db.fileNumber++
	db.segmentNumbers = append(db.segmentNumbers, db.fileNumber)
	db.outOffset = 0
	db.outPath = filepath.Join(db.dir, outFileName+"-"+strconv.FormatInt(int64(db.fileNumber), 10))
	f, err := os.OpenFile(db.outPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	db.out = f
	go db.startMergeProcess()
	return nil
}

func (db *Db) startMergeProcess() {
	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()
	segments, err := db.defineSegmentsToMerge()
	if err != nil {
		fmt.Println(err)
	}
	if segments != nil {
		db.mergingSegments = segments
		hashIndexCopy := db.createHashIndexCopy()
		delete(hashIndexCopy, db.lastChangedEl)
		if err = db.mergeSegments(hashIndexCopy); err != nil {
			fmt.Println(err)
		}
	}
}

func (db *Db) mergeSegments(index hashIndex) error {
	tempFileOutPath := filepath.Join(db.dir, "temp")
	tempFile, err := os.OpenFile(tempFileOutPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	outOffset := int64(0)
	for k, offset := range index {
		reader, file, err := db.getReaderByOffset(offset)
		record, err := readRecord(reader)
		if err != nil {
			return err
		}
		file.Close()
		n, err := tempFile.Write(record)
		if err == nil {
			index[k] = outOffset
			outOffset += int64(n)
		}
	}
	if err = tempFile.Close(); err != nil {
		return err
	}
	db.finishMergeCh <- index
	return err
}

func (db *Db) finishMergingSegments(index hashIndex) error {
	db.index = index
	for _, segment := range db.mergingSegments {
		if err := os.Remove(segment); err != nil {
			return err
		}
	}
	if err := db.recover(); err != nil {
		return err
	}
	if err := os.Rename("temp", "segment-1"); err != nil {
		return err
	}
	db.segmentNumbers = db.getSegmentNumbers()
	go db.startMergeProcess()
	return nil
}

func (db *Db) getAllSegments() ([]string, error) {
	err := os.Chdir(db.dir)
	if err != nil {
		return nil, err
	}
	segments, err := filepath.Glob("segment-*")
	if err != nil {
		return nil, err
	}
	return segments, err
}

func (db *Db) defineSegmentsToMerge() ([]string, error) {
	segments, err := db.getAllSegments()
	if err != nil {
		return nil, err
	}
	if len(segments) <= 2 {
		return nil, err
	}
	return segments[:len(segments)-1], err
}

func (db *Db) createHashIndexCopy() hashIndex {
	copiedIndex := make(hashIndex)
	for key, value := range db.index {
		copiedIndex[key] = value
	}
	return copiedIndex
}

func (db *Db) getSegmentNumbers() []int {
	files, err := db.getAllSegments()
	if err != nil {
		return nil
	}
	var segmentNumbers []int
	for _, file := range files {
		number, err := strconv.Atoi(file[8:])
		if err != nil {
			return nil
		}
		segmentNumbers = append(segmentNumbers, number)
	}
	return segmentNumbers
}

func (db *Db) checkFileExistence(number int) bool {
	for _, v := range db.segmentNumbers {
		if v == number {
			return true
		}
	}
	return false
}
