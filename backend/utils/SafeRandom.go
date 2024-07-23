package utils

import (
	"math/rand"
	"sync"
	"time"
)

var (
	mu sync.Mutex
	r  = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// SafeRandomInt 返回[0, n)内的随机整数，线程安全
func SafeRandomInt(n int) int {
	mu.Lock()
	defer mu.Unlock()
	return r.Intn(n)
}
