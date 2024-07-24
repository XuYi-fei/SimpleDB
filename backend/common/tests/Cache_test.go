package tests

import (
	"dbofmine/backend/common"
	"dbofmine/commons"
	"math/rand"
	"sync"
	"testing"
)

func getForCache(key int64) (int64, error) {
	return key, nil
}

func releaseForCache(obj int64) {

}

func TestAbstractCache(t *testing.T) {
	t.Log("TestAbstractCache")
	cache := common.NewAbstractCache(50, getForCache, releaseForCache)
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
