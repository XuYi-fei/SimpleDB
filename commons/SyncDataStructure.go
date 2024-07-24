package commons

import "sync"

// SyncMap 是一个泛型同步map
type SyncMap[K comparable, V any] struct {
	internalMap sync.Map
}

// Store 将键值对存储在map中
func (m *SyncMap[K, V]) Store(key K, value V) {
	m.internalMap.Store(key, value)
}

// Load 根据键获取值
func (m *SyncMap[K, V]) Load(key K) (V, bool) {
	value, ok := m.internalMap.Load(key)
	if !ok {
		var zero V
		return zero, ok
	}
	return value.(V), ok
}

// Delete 根据键删除值
func (m *SyncMap[K, V]) Delete(key K) {
	m.internalMap.Delete(key)
}

// Range 遍历地图中的所有键值对
func (m *SyncMap[K, V]) Range(f func(key K, value V) bool) {
	m.internalMap.Range(func(k, v interface{}) bool {
		return f(k.(K), v.(V))
	})
}
