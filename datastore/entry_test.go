package datastore

import (
	"bufio"
	"bytes"
	"testing"
)

func TestEntry_Encode_String(t *testing.T) {
	e := entry[string]{"key", "value"}
	e.Decode(e.Encode())
	if e.key != "key" {
		t.Error("incorrect key")
	}
	if e.value != "value" {
		t.Error("incorrect value")
	}
}

func TestEntry_Encode_Int64(t *testing.T) {
	e := entry[int64]{"key", 42}
	e.Decode(e.Encode())
	if e.key != "key" {
		t.Error("incorrect key")
	}
	if e.value != 42 {
		t.Error("incorrect value")
	}
}

func TestReadValue_String(t *testing.T) {
	e := entry[string]{"key", "test-value"}
	data := e.Encode()
	v, err := readValue(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if v != "test-value" {
		t.Errorf("Got bad value [%s]", v)
	}
}

func TestReadValue_Int64(t *testing.T) {
	e := entry[int64]{"key", 42}
	data := e.Encode()
	v, err := readValue(bufio.NewReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if v != int64(42) {
		t.Errorf("Got bad value [%d]", v)
	}
}
