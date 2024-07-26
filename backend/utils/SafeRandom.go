package utils

import (
	"math/rand"
	"sync"
	"time"
)

// RandomUtil 定义一个结构体，包含一个互斥锁
type RandomUtil struct {
	mu sync.Mutex
}

// NewRandomUtil 创建一个新的 RandomUtil 实例
func NewRandomUtil() *RandomUtil {
	return &RandomUtil{}
}

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

func SafeRandomBytes(length int) []byte {
	mu.Lock()
	defer mu.Unlock()
	bytes := make([]byte, length)
	r.Read(bytes)
	return bytes
}
