package commons

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type ReentrantLock struct {
	mu    sync.Mutex
	owner int64
	count int
}

func (r *ReentrantLock) Lock() {
	id := getGoroutineID()
	//Logger.Debugf("Lock goroutine id: %d", id)
	r.mu.Lock()
	if r.owner == id {
		r.count++
		r.mu.Unlock()
		return
	}
	for r.owner != 0 {
		r.mu.Unlock()
		r.mu.Lock()
	}
	r.owner = id
	r.count = 1
	r.mu.Unlock()
}

func (r *ReentrantLock) Unlock() {
	id := getGoroutineID()
	//Logger.Debugf("Unlock goroutine id: %d", id)
	//Logger.Debugf("Lock owner id: %d", r.owner)
	r.mu.Lock()
	if r.owner != id {
		r.mu.Unlock()
		panic("unlock of a lock not owned by current goroutine")
	}
	r.count--
	if r.count == 0 {
		r.owner = 0
	}
	r.mu.Unlock()
}

func getGoroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	//Logger.Infof("stack trace: %s", string(buf[:n]))
	// 得到id字符串
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return int64(id)

}
