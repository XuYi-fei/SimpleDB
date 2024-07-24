package common

type MockCache struct {
}

// 实现接口的方法，注意签名要匹配
func (cache *MockCache) getForCache(key int64) (int64, error) {
	return key, nil
}

func (cache *MockCache) releaseForCache(key int64) {
	// 实现释放资源的逻辑
}
