package main

import (
	"SimpleDB/backend/dm"
	"SimpleDB/backend/server"
	"SimpleDB/backend/tbm"
	"SimpleDB/backend/tm"
	"SimpleDB/backend/vm"
	"flag"
	"fmt"
	"strconv"
	"strings"
)

const (
	port        = 9998
	DEFAULT_MEM = 64 * MB // 64MB 默认内存大小
	KB          = 1 << 10 // 1KB
	MB          = 1 << 20 // 1MB
	GB          = 1 << 30 // 1GB
)

func main() {
	// 定义命令行参数
	openFlag := flag.String("open", "", "Open database at DBPath")
	createFlag := flag.String("create", "", "Create database at DBPath")
	memFlag := flag.String("mem", "64MB", "Memory size (e.g., 64MB, 1GB)")

	// 解析命令行参数
	flag.Parse()

	// 判断命令行参数，并调用相应的函数
	if *openFlag != "" {
		memSize := parseMem(*memFlag)
		openDB(*openFlag, memSize)
		return
	}
	if *createFlag != "" {
		createDB(*createFlag)
		return
	}
	fmt.Println("Usage: launcher -open DBPath | -create DBPath [-mem MemorySize]")
}

// createDB 创建新的数据库
func createDB(path string) {
	tm, err := tm.CreateTransactionManagerImpl(path)
	if err != nil {
		panic(err)
	}
	dm := dm.CreateDataManager(path, DEFAULT_MEM, tm)
	vm := vm.NewVersionManager(tm, dm)
	tbm.CreateTableManger(path, vm, dm)
	tm.Close()
	dm.Close()
}

// openDB 启动已有的数据库
func openDB(path string, memSize int64) {
	tm, err := tm.OpenTransactionManagerImpl(path)
	if err != nil {
		panic(err)
	}
	dm := dm.OpenDataManager(path, memSize, tm)
	vm := vm.NewVersionManager(tm, dm)
	tbm := tbm.OpenTableManager(path, vm, dm)
	server := server.NewServer(port, tbm)
	server.Start()
}

// parseMem 解析命令行参数中的内存大小
func parseMem(memStr string) int64 {
	if memStr == "" {
		return DEFAULT_MEM
	}
	unit := strings.ToUpper(memStr[len(memStr)-2:])
	memNum, err := strconv.ParseInt(memStr[:len(memStr)-2], 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Invalid memory size: %v", err))
	}

	switch unit {
	case "KB":
		return memNum * KB
	case "MB":
		return memNum * MB
	case "GB":
		return memNum * GB
	default:
		panic("Invalid memory unit")
	}
}
