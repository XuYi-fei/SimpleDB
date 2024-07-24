package common

import (
	"dbofmine/commons"
	"errors"
	"time"
)

type AbstractCache[T any] struct {
	// 实际缓存的数据
	cache map[int64]T
	// 元素的引用个数
	references map[int64]int
	// 正在获取某资源的线程
	getting map[int64]bool

	// 缓存的最大缓存资源数
	maxResource int
	// 缓存中元素的个数
	count int
	lock  commons.ReentrantLock

	// getForCache 获取缓存中的资源
	getForCache func(key int64) (T, error)
	// releaseForCache 从缓存中删除资源
	releaseForCache func(key T)
}

// NewAbstractCache 创建一个新的缓存
func NewAbstractCache[T any](maxResource int, getForCache func(key int64) (T, error), releaseForCache func(key T)) *AbstractCache[T] {
	return &AbstractCache[T]{
		cache:           make(map[int64]T),
		references:      make(map[int64]int),
		getting:         make(map[int64]bool),
		maxResource:     maxResource,
		count:           0,
		getForCache:     getForCache,
		releaseForCache: releaseForCache,
		//lock:            sync.Mutex{}, // 这里不初始化也行，默认的也有值
	}
}

// Get 获取缓存中的资源
func (cache *AbstractCache[T]) Get(key int64) (T, error) {
	for {
		cache.lock.Lock()
		// 判断是否有其他资源正在获取资源
		if cache.getting[key] {
			// 请求的资源正在被其他线程获取
			cache.lock.Unlock()
			time.Sleep(1 * time.Second)
			continue
		}

		obj, ok := cache.cache[key]

		if ok {
			// 资源在缓存中，直接返回
			cache.references[key]++
			cache.lock.Unlock()
			return obj, nil
		}

		// 不在缓存中，需要获取资源
		if cache.maxResource > 0 && cache.count >= cache.maxResource {
			// 缓存已满，需要删除一个资源
			cache.lock.Unlock()
			return obj, errors.New(commons.ErrorMessage.CacheIsFullError)
		}

		cache.count++
		cache.getting[key] = true
		cache.lock.Unlock()
		break
	}
	var obj T
	// 获取资源
	obj, err := cache.getForCache(key)
	if err != nil {
		cache.lock.Lock()
		cache.count--
		delete(cache.getting, key)
		cache.lock.Unlock()
		return obj, err
	}

	cache.lock.Lock()
	commons.Logger.Debug(cache.lock)

	cache.getting[key] = false
	cache.cache[key] = obj
	cache.references[key] = 1
	cache.lock.Unlock()
	return obj, nil
}

// Release 强行释放资源
func (cache *AbstractCache[T]) Release(key int64) {

	cache.lock.Lock()
	defer cache.lock.Unlock()

	ref := cache.references[key] - 1
	if ref <= 0 {
		// 释放资源
		obj, err := cache.Get(key)
		if err != nil {
			// 释放资源失败
			return
		}
		cache.releaseForCache(obj)
		delete(cache.references, key)
		delete(cache.cache, key)
		cache.count--
		return
	} else {
		cache.references[key] = ref
	}
}

// Close 关闭缓存
func (cache *AbstractCache[T]) Close() {
	cache.lock.Lock()
	defer cache.lock.Unlock()

	// 获取 map 中的所有键
	// 这里使用了一个技巧，先创建一个切片，然后遍历 map，将 map 中的键放入切片中
	// 这样可以避免在遍历 map 的时候删除 map 中的元素
	keys := make([]int64, 0, len(cache.cache))
	for key := range cache.cache {
		keys = append(keys, key)
	}

	// 遍历键并执行释放操作
	for _, key := range keys {
		obj := cache.cache[key]
		cache.releaseForCache(obj)
		delete(cache.references, key)
		delete(cache.cache, key)
		cache.count--
	}

}