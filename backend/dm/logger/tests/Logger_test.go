package tests

import (
	logger2 "SimpleDB/backend/dm/logger"
	"os"
	"testing"
)

func TestLogger(t *testing.T) {
	t.Log("TestLogger")
	logger := logger2.CreateLogger("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/logger")
	logger.Log([]byte("aaa"))
	logger.Log([]byte("bbb"))
	logger.Log([]byte("ccc"))
	logger.Log([]byte("ddd"))
	logger.Log([]byte("eee"))
	logger.Close()

	logger = logger2.OpenLogger("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/logger")
	logger.Rewind()

	defer os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/logger" + logger2.LogSuffix)

	log := logger.Next()
	if log == nil {
		t.Fatalf("log is nil")
	}
	if string(log) != "aaa" {
		t.Fatalf("log not equal")
	}

	log = logger.Next()
	if log == nil {
		t.Fatalf("log is nil")
	}
	if string(log) != "bbb" {
		t.Fatalf("log not equal")
	}

	log = logger.Next()
	if log == nil {
		t.Fatalf("log is nil")
	}
	if string(log) != "ccc" {
		t.Fatalf("log not equal")
	}

	log = logger.Next()
	if log == nil {
		t.Fatalf("log is nil")
	}
	if string(log) != "ddd" {
		t.Fatalf("log not equal")
	}

	log = logger.Next()
	if log == nil {
		t.Fatalf("log is nil")
	}
	if string(log) != "eee" {
		t.Fatalf("log not equal")
	}

	log = logger.Next()
	if log != nil {
		t.Fatalf("log is not nil")
	}
	logger.Close()

}
