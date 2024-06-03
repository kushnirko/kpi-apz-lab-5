package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"reflect"
)

type entry[T any] struct {
	key   string
	value T
}

func (e *entry[T]) Encode() []byte {
	valueType := reflect.TypeOf(e.value).String()

	kl := len(e.key)
	tl := len(valueType)

	var vl int
	switch any(e.value).(type) {
	case string:
		vl = len(any(e.value).(string))
	case int64:
		vl = 8
	}

	size := kl + tl + vl + 16
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(tl))
	copy(res[kl+12:], valueType)
	binary.LittleEndian.PutUint32(res[kl+tl+12:], uint32(vl))

	switch any(e.value).(type) {
	case string:
		copy(res[kl+tl+16:], any(e.value).(string))
	case int64:
		binary.LittleEndian.PutUint64(res[kl+tl+16:], uint64(any(e.value).(int64)))
	}

	return res
}

func (e *entry[T]) Decode(input []byte) {
	kl := binary.LittleEndian.Uint32(input[4:])
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	e.key = string(keyBuf)

	tl := binary.LittleEndian.Uint32(input[kl+8:])

	vl := binary.LittleEndian.Uint32(input[kl+tl+12:])
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+tl+16:kl+tl+16+vl])

	e.value = any(e.value).(T)
}

func readValue(in *bufio.Reader) (any, error) {
	header, err := in.Peek(8)
	if err != nil {
		return "", err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return "", err
	}

	header, err = in.Peek(4)
	if err != nil {
		return "", err
	}
	valTypeSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}

	valueType := make([]byte, valTypeSize)
	n, err := in.Read(valueType)
	if err != nil {
		return "", err
	}
	if n != valTypeSize {
		return "", fmt.Errorf("can't read type bytes (read %d, expected %d)", n, valTypeSize)
	}

	header, err = in.Peek(4)
	if err != nil {
		return "", err
	}
	valSize := int(binary.LittleEndian.Uint32(header))

	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}

	data := make([]byte, valSize)
	n, err = in.Read(data)
	if err != nil {
		return "", err
	}
	if n != valSize {
		return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}

	if string(valueType) == "string" {
		return string(data), nil
	}

	return int64(binary.LittleEndian.Uint64(data)), nil
}

func readRecord(in *bufio.Reader) ([]byte, error) {
	header, err := in.Peek(4)
	if err != nil {
		return nil, err
	}
	recordSize := int(binary.LittleEndian.Uint32(header))
	data := make([]byte, recordSize)
	n, err := in.Read(data)
	if err != nil {
		return nil, err
	}
	if n != recordSize {
		return nil, fmt.Errorf("can't read record bytes (read %d, expected %d)", n, recordSize)
	}
	return data, nil
}
