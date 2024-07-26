package tests

import (
	"dbofmine/backend/dm/dmPage"
	"dbofmine/backend/dm/dmPageCache"
	"dbofmine/backend/utils"
	"dbofmine/commons"
	"github.com/sirupsen/logrus"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	Logger = commons.NewLoggerByLevel(logrus.InfoLevel)
)

func TestPageCacheImpl(t *testing.T) {
	t.Log("TestPageCacheImpl")
	pc := dmPageCache.NewPageCacheImpl("/Users/xuyifei/repos/dbofmine/data/test/backend/dm/dmPageCache", int64(dmPageCache.PageSize*50))
	for i := 0; i < 100; i++ {
		tmp := make([]byte, dmPageCache.PageSize)
		tmp[0] = byte(i)

		pageNumber := pc.NewPage(tmp)
		newPage, err := pc.GetPage(pageNumber)
		if err != nil {
			panic(err)
		}
		newPage.SetDirty(true)
		newPage.Release()
	}
	pc.Close()

	pc = dmPageCache.OpenPageCacheImpl("/Users/xuyifei/repos/dbofmine/data/test/backend/dm/dmPageCache", int64(dmPageCache.PageSize*50))
	for i := 1; i <= 100; i++ {
		page, _ := pc.GetPage(i)
		if page.GetData()[0] != byte(i-1) {
			t.Fatalf("page data not equal")
		}
		page.Release()
	}
	pc.Close()

	os.RemoveAll("/Users/xuyifei/repos/dbofmine/data/test/backend/dm/dmPageCache" + dmPageCache.DB_SUFFIX)
}

func TestPageCacheMultiSimple(t *testing.T) {
	logger := commons.NewLoggerByLevel(logrus.InfoLevel)
	pc1 := dmPageCache.NewPageCacheImpl("/Users/xuyifei/repos/dbofmine/data/test/backend/dm/dmPageCacheSimpleTest", int64(dmPageCache.PageSize*50))

	wg := sync.WaitGroup{}
	wg.Add(200)
	//var zeroCnt int32 = 0
	//var oneCnt int32 = 0

	var numberPages1 int32 = 0
	for i := 0; i < 200; i++ {
		go func() {
			for i := 0; i < 80; i++ {
				op := utils.SafeRandomInt(20)
				if op == 0 {
					//atomic.AddInt32(&zeroCnt, 1)
					// 生成随机页
					data := utils.SafeRandomBytes(dmPageCache.PageSize)
					pageNumber := pc1.NewPage(data)
					logger.Debugf("Adding key: %d, op: %d", pageNumber, op)
					// 获取刚刚的页，现在应该是从缓存读取
					time.Sleep(2000 * time.Millisecond)
					page, err := pc1.GetPage(pageNumber)
					if err != nil {
						panic(err)
					}
					atomic.AddInt32(&numberPages1, 1)
					logger.Debugf("Ready to release key: %d", pageNumber)
					page.Release()
				} else if op < 20 {
					//atomic.AddInt32(&oneCnt, 1)
					mod := int(atomic.LoadInt32(&numberPages1))
					logger.Debugf("Ready to release, op: %d", op)

					//mod := int(numberPages1)
					if mod == 0 {
						continue
					}

					pageNumber := utils.SafeRandomInt(mod) + 1
					page, err := pc1.GetPage(pageNumber)
					if err != nil {
						panic(err)
					}
					page.Release()
				}
			}
			wg.Done()

		}()
	}

	wg.Wait()
	pc1.Close()
	//commons.Logger.Infof("zeroCnt: %d, oneCnt: %d", zeroCnt, oneCnt)
	os.RemoveAll("/Users/xuyifei/repos/dbofmine/data/test/backend/dm/dmPageCacheSimpleTest" + dmPageCache.DB_SUFFIX)

}

func TestPageCacheMulti(t *testing.T) {
	pc2 := dmPageCache.NewPageCacheImpl("/Users/xuyifei/repos/dbofmine/data/test/backend/dm/dmPageCacheMultiTest", int64(dmPageCache.PageSize*50))
	defer os.RemoveAll("/Users/xuyifei/repos/dbofmine/data/test/backend/dm/dmPageCacheMultiTest" + dmPageCache.DB_SUFFIX)
	mpc := &dmPageCache.MockPageCache{
		Cache: make(map[int]*dmPage.MockPage),
	}
	lock := commons.ReentrantLock{}

	wg := sync.WaitGroup{}
	wg.Add(30)
	var numberPages2 int32 = 0

	for i := 0; i < 30; i++ {
		go func() {
			for i := 0; i < 1000; i++ {
				op := utils.SafeRandomInt(20)
				if op == 0 {
					data := utils.SafeRandomBytes(dmPageCache.PageSize)
					lock.Lock()
					pageNumber := pc2.NewPage(data)
					mockPageNumber := mpc.NewPage(data)
					commons.Logger.Debugf("Adding pn: %d, mpc: %d, op: %d", pageNumber, mockPageNumber, op)
					if pageNumber != mockPageNumber {
						t.Fatalf("page number not equal")
					}
					lock.Unlock()
					atomic.AddInt32(&numberPages2, 1)
				} else if op < 10 {
					mod := int(atomic.LoadInt32(&numberPages2))
					if mod == 0 {
						continue
					}
					pageNumber := utils.SafeRandomInt(mod) + 1
					page, err := pc2.GetPage(pageNumber)
					if err != nil {
						panic(err)
					}
					mockPage := mpc.GetPage(pageNumber)
					page.Lock()
					if page.GetPageNumber() != mockPage.GetPageNumber() {
						t.Fatalf("page number not equal")
					}
					page.Unlock()
					page.Release()
				} else {
					mod := int(atomic.LoadInt32(&numberPages2))
					if mod == 0 {
						continue
					}
					pageNumber := utils.SafeRandomInt(mod) + 1

					page, err := pc2.GetPage(pageNumber)
					if err != nil {
						panic(err)
					}
					mockPage := mpc.GetPage(pageNumber)
					newData := utils.SafeRandomBytes(dmPageCache.PageSize)

					page.Lock()
					mockPage.SetDirty(true)
					for j := 0; j < dmPageCache.PageSize; j++ {
						mockPage.GetData()[j] = newData[j]
					}
					page.SetDirty(true)
					for j := 0; j < dmPageCache.PageSize; j++ {
						page.GetData()[j] = newData[j]
					}
					page.Unlock()
					page.Release()
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
}
