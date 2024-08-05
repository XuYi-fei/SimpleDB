package tests

import (
	"SimpleDB/backend/common"
	"SimpleDB/commons"
	"math/rand"
	"sync"
	"testing"
)

func TestAbstractCache(t *testing.T) {
	t.Log("TestAbstractCache")
	mockCache := &common.MockCache{}
	cache := common.NewAbstractCache[int64](50, mockCache)

	wg := sync.WaitGroup{}
	wg.Add(50)

	for i := 0; i < 50; i++ {
		go func() {
			for j := 0; j < 200; j++ {
				uid := rand.Int63()
				h, err := cache.Get(uid)
				if err != nil {
					t.Logf(err.Error())
					if err.Error() != commons.ErrorMessage.CacheIsFullError {
						t.Fatalf(err.Error())
					} else {
						continue
					}
				}

				if h != uid {
					t.Errorf("uid not equal")
				}
				cache.Release(h)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
