package datastore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDb_Put(t *testing.T) {
	maxFileSize = 66 // Some tests depend on this value

	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	t.Run("put/get with one file", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("max segment size reached", func(t *testing.T) {
		err := db.Put("key2", "someOTHERvalue")
		if err != nil {
			t.Errorf("Cannot put in file: %s", err)
		}

		files, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		filesNum := len(files)
		if filesNum != 2 {
			t.Errorf("The number of created files is not as required. Expected 2, got %d", filesNum)
		} else if files[0].Name() != "segment-1" || files[1].Name() != "segment-2" {
			t.Errorf("Incorrectly created files")
		}

		value, err := db.Get("key2")
		if err != nil {
			t.Errorf("Cannot get key2: %s", err)
		}
		if value != "someOTHERvalue" {
			t.Errorf("Bad value returned expected valueX, got %s", value)
		}
	})

	t.Run("file sizes check", func(t *testing.T) {
		outFile1, err := os.Open(filepath.Join(dir, "segment-1"))
		if err != nil {
			t.Fatal(err)
		}
		outFile2, err := os.Open(filepath.Join(dir, "segment-2"))
		if err != nil {
			t.Fatal(err)
		}

		err = db.Put("newKey", "newValue")
		if err != nil {
			t.Errorf("Cannot put in file: %s", err)
		}

		outInfo1, err := outFile1.Stat()
		if err != nil {
			t.Fatal(err)
		}
		size1 := outInfo1.Size()

		outInfo2, err := outFile2.Stat()
		if err != nil {
			t.Fatal(err)
		}
		size2 := outInfo2.Size()

		if size1 != 66 {
			t.Errorf("Unexpected size (%d vs 66)", size1)
		}
		if size2 != 56 {
			t.Errorf("Unexpected size (%d vs 52)", size2)
		}

		err = outFile1.Close()
		if err != nil {
			fmt.Println(err)
		}
		err = outFile2.Close()
		if err != nil {
			fmt.Println(err)
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir)
		if err != nil {
			t.Fatal(err)
		}

		valueForCheck1, err := db.Get("key1")
		if err != nil {
			t.Errorf("Cannot get key1: %s", err)
		}
		if valueForCheck1 != "value1" {
			t.Errorf("Bad value returned expected value1, got %s", valueForCheck1)
		}

		valueForCheck2, err := db.Get("key2")
		if err != nil {
			t.Errorf("Cannot get key2: %s", err)
		}
		if valueForCheck2 != "someOTHERvalue" {
			t.Errorf("Bad value returned expected someOTHERvalue, got %s", valueForCheck2)
		}

		files, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		filesNum := len(files)
		if filesNum != 2 {
			t.Errorf("The number of created files is not as required. Expected 2, got %d", filesNum)
		} else if files[0].Name() != "segment-1" || files[1].Name() != "segment-2" {
			t.Errorf("Incorrectly created files")
		}
	})

	t.Run("merge segments", func(t *testing.T) {

		err := db.Put("specialKey", "VALUE_FOR_SPECIAL_KEY")
		if err != nil {
			t.Errorf("Cannot put specialKey: %s", err)
		}

		value1, err := db.Get("key2")
		if err != nil {
			t.Errorf("Cannot get key2: %s", err)
		}
		if value1 != "someOTHERvalue" {
			t.Errorf("Bad value returned expected someOTHERvalue, got %s", value1)
		}
		time.Sleep(1 * time.Second)
		filesAfterFirstMerge, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		filesNumAfterFirstMerge := len(filesAfterFirstMerge)
		if filesNumAfterFirstMerge != 2 {
			t.Errorf("First merge: The number of created files is not as required. Expected 2, got %d", filesNumAfterFirstMerge)
			fmt.Println(filesAfterFirstMerge)
		} else if filesAfterFirstMerge[0].Name() != "segment-1" || filesAfterFirstMerge[1].Name() != "segment-3" {
			t.Errorf("Incorrectly created files")
		}

		err = db.Put("TARAS", "Live in London")
		if err != nil {
			t.Errorf("Cannot put TARAS: %s", err)
		}

		value2, err := db.Get("TARAS")
		if err != nil {
			t.Errorf("Cannot get key2: %s", err)
		}
		if value2 != "Live in London" {
			t.Errorf("Bad value returned expected Live in London, got %s", value2)
		}
		time.Sleep(1 * time.Second)
		filesAfterSecondMerge, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		filesNumAfterSecondMerge := len(filesAfterSecondMerge)
		if filesNumAfterSecondMerge != 2 {
			t.Errorf("The number of created files is not as required. Expected 2, got %d", filesNumAfterSecondMerge)
			fmt.Println(filesAfterSecondMerge)
		} else if filesAfterSecondMerge[0].Name() != "segment-1" || filesAfterSecondMerge[1].Name() != "segment-4" {
			t.Errorf("Incorrectly created files")
		}
	})
}
