package tests

import (
	"SimpleDB/backend/dm"
	"SimpleDB/backend/dm/constants"
	"SimpleDB/backend/tm"
	"SimpleDB/backend/utils"
	"SimpleDB/commons"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
)

var (
	uids0    []int64
	uids1    []int64
	uidsLock commons.ReentrantLock
)

func initUids() {
	uids1 = make([]int64, 0)
	uids0 = make([]int64, 0)
}

func TestDataManagerSingle(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TESTSingle.db")
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TESTSingle.log")
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TESTSingle.xid")
	})
	tm0, _ := tm.CreateTransactionManagerImpl("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TESTSingle")
	dm0 := dm.CreateDataManager("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TESTSingle", int64(constants.PageSize*10), tm0)
	dm1 := NewMockDataManager()

	taskNum := 500
	wg := sync.WaitGroup{}
	wg.Add(1)

	initUids()
	insertRatio := 50

	go func() {
		dataLength := 60
		defer wg.Done()
		for i := 0; i < taskNum; i++ {
			op := int(math.Abs(float64(utils.SafeRandomInt(math.MaxInt32)))) % 100
			if op < insertRatio {
				data := utils.SafeRandomBytes(dataLength)
				var u0 int64 = 0
				var u1 int64 = 0
				u0, _ = dm0.Insert(0, data)
				u1 = dm1.Insert(0, data)

				uidsLock.Lock()
				uids0 = append(uids0, u0)
				uids1 = append(uids1, u1)
				uidsLock.Unlock()

			} else {

				uidsLock.Lock()

				if len(uids0) == 0 {
					uidsLock.Unlock()
					continue
				}

				tmp := utils.SafeRandomInt(len(uids0))
				u0 := uids0[tmp]
				u1 := uids1[tmp]

				data0 := dm0.Read(u0)
				if data0 == nil {
					continue
				}
				data1 := dm1.Read(u1)

				data0.RLock()
				data1.RLock()

				s0 := data0.Data()
				s1 := data1.Data()

				if !commons.BytesCompare(s0, s1) {
					t.Errorf("Data not equal")
				}
				data0.RUnLock()
				data1.RUnLock()

				newData := utils.SafeRandomBytes(dataLength)
				data0.Before()
				data1.Before()

				s0 = make([]byte, len(newData))
				copy(s0, newData)
				s1 = make([]byte, len(newData))
				copy(s1, newData)

				data0.After(0)
				data1.After(0)
				data0.Release()
				data1.Release()

			}
		}
	}()

	wg.Wait()
	dm0.Close()
	dm1.Close()
}

func TestDataManagerMulti(t *testing.T) {

	t.Cleanup(func() {
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TESTDMMulti.db")
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TESTDMMulti.log")
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TESTDMMulti.xid")
	})

	defer func() {
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TESTDMMulti.db")
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TESTDMMulti.log")
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TESTDMMulti.xid")
	}()

	tm0, _ := tm.CreateTransactionManagerImpl("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TESTDMMulti")
	dm0 := dm.CreateDataManager("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TESTDMMulti", int64(constants.PageSize*50), tm0)
	dm1 := NewMockDataManager()

	taskNum := 100
	wg := sync.WaitGroup{}
	wg.Add(10)

	initUids()
	insertRatio := 50

	for j := 0; j < 10; j++ {
		go func(k int) {
			fmt.Printf("Start, j = %d\n", k)
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Recovered in goroutine %d: %v\n", k, r)
				}
				fmt.Printf("Finish, j = %d\n", k)
				wg.Done()
			}()
			dataLength := 60
			for i := 0; i < taskNum; i++ {
				fmt.Printf("")
				op := int(math.Abs(float64(utils.SafeRandomInt(math.MaxInt32)))) % 100
				if op < insertRatio {
					data := utils.SafeRandomBytes(dataLength)
					var u0 int64 = 0
					var u1 int64 = 0
					u0, _ = dm0.Insert(0, data)
					u1 = dm1.Insert(0, data)

					uidsLock.Lock()
					uids0 = append(uids0, u0)
					uids1 = append(uids1, u1)
					uidsLock.Unlock()

				} else {

					uidsLock.Lock()

					if len(uids0) == 0 {
						uidsLock.Unlock()
						continue
					}

					tmp := utils.SafeRandomInt(len(uids0))
					u0 := uids0[tmp]
					u1 := uids1[tmp]

					data0 := dm0.Read(u0)
					if data0 == nil {
						uidsLock.Unlock()
						continue
					}
					data1 := dm1.Read(u1)

					data0.RLock()
					data1.RLock()

					s0 := data0.Data()
					s1 := data1.Data()

					if !commons.BytesCompare(s0, s1) {
						t.Errorf("Data not equal")
					}
					data0.RUnLock()
					data1.RUnLock()

					newData := utils.SafeRandomBytes(dataLength)
					data0.Before()
					data1.Before()

					s0 = make([]byte, len(newData))
					copy(s0, newData)
					s1 = make([]byte, len(newData))
					copy(s1, newData)

					data0.After(0)
					data1.After(0)
					data0.Release()
					data1.Release()
					uidsLock.Unlock()
				}

			}
		}(j)
	}

	wg.Wait()
	dm0.Close()
	dm1.Close()
}

func TestDataManagerRecoverSimple(t *testing.T) {

	t.Cleanup(func() {
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TestRecoverSimple.db")
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TestRecoverSimple.log")
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TestRecoverSimple.xid")
	})

	defer func() {
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TestRecoverSimple.db")
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TestRecoverSimple.log")
		os.RemoveAll("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TestRecoverSimple.xid")
	}()

	tm0, _ := tm.CreateTransactionManagerImpl("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TestRecoverSimple")
	dm0 := dm.CreateDataManager("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TestRecoverSimple", int64(constants.PageSize*50), tm0)
	dm1 := NewMockDataManager()

	dm0.Close()

	taskNum := 100

	initUids()
	insertRatio := 50

	for j := 0; j < 8; j++ {
		dm0 = dm.OpenDataManager("/Users/xuyifei/repos/SimpleDB/data/test/backend/dm/TestRecoverSimple", int64(constants.PageSize*30), tm0)
		wg := sync.WaitGroup{}
		wg.Add(10)
		for k := 0; k < 10; k++ {
			go func() {
				dataLength := 60
				defer wg.Done()
				for i := 0; i < taskNum; i++ {
					op := int(math.Abs(float64(utils.SafeRandomInt(math.MaxInt32)))) % 100
					if op < insertRatio {
						data := utils.SafeRandomBytes(dataLength)
						var u0 int64 = 0
						var u1 int64 = 0
						u0, _ = dm0.Insert(0, data)
						u1 = dm1.Insert(0, data)

						uidsLock.Lock()
						uids0 = append(uids0, u0)
						uids1 = append(uids1, u1)
						uidsLock.Unlock()

					} else {

						uidsLock.Lock()

						if len(uids0) == 0 {
							uidsLock.Unlock()
							continue
						}

						tmp := utils.SafeRandomInt(len(uids0))
						u0 := uids0[tmp]
						u1 := uids1[tmp]

						data0 := dm0.Read(u0)
						if data0 == nil {
							uidsLock.Unlock()
							continue
						}
						data1 := dm1.Read(u1)

						data0.RLock()
						data1.RLock()

						s0 := data0.Data()
						s1 := data1.Data()

						if !commons.BytesCompare(s0, s1) {
							t.Errorf("Data not equal")
						}
						data0.RUnLock()
						data1.RUnLock()

						newData := utils.SafeRandomBytes(dataLength)
						data0.Before()
						data1.Before()

						s0 = make([]byte, len(newData))
						copy(s0, newData)
						s1 = make([]byte, len(newData))
						copy(s1, newData)

						data0.After(0)
						data1.After(0)
						data0.Release()
						data1.Release()
						uidsLock.Unlock()

					}
				}
			}()
		}
		wg.Wait()

	}

	dm0.Close()
	dm1.Close()
}
