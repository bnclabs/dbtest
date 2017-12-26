package main

import "os"
import "fmt"
import "bytes"
import "unsafe"
import "reflect"
import "path/filepath"

func Fixbuffer(buffer []byte, size int64) []byte {
	if buffer == nil || int64(cap(buffer)) < size {
		buffer = make([]byte, size)
	}
	return buffer[:size]
}

func Bytes2str(bytes []byte) string {
	if bytes == nil {
		return ""
	}
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	st := &reflect.StringHeader{Data: sl.Data, Len: sl.Len}
	return *(*string)(unsafe.Pointer(st))
}

func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			size += f.Size()
		}
		return err
	})
	return size, err
}

func comparekeyvalue(key, value []byte, vlen int) bool {
	if vlen > 0 && len(value) > 0 {
		if bytes.Compare(key, value[:len(key)]) != 0 {
			panic(fmt.Errorf("expected %q, got %q", key, value))
		}
	}
	return true
}
