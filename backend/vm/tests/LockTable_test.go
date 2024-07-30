package tests

import (
	"dbofmine/backend/vm"
	"dbofmine/commons"
	"testing"
)

func TestLockTable(t *testing.T) {
	t.Log("TestLockTable")
	lt := vm.NewLockTable()
	_, err := lt.Add(1, 1)
	if err != nil {
		t.Errorf(err.Error())
	}

	_, err = lt.Add(2, 2)
	if err != nil {
		t.Errorf(err.Error())
	}

	_, err = lt.Add(2, 1)
	if err != nil {
		t.Errorf(err.Error())
	}

	_, err = lt.Add(1, 2)
	if err == nil {
		t.Errorf("Deadlock not detected")
	} else {
		commons.Logger.Debugf("Deadlock detected")
	}
}

func TestLockTable2(t *testing.T) {
	t.Log("TestLockTable2")

	lt := vm.NewLockTable()

	for i := 1; i <= 100; i++ {
		o, err := lt.Add(int64(i), int64(i))
		if err != nil {
			t.Errorf(err.Error())
		}
		if o != nil {
			go func() {
				o.Lock()
				o.Unlock()
			}()
		}
	}

	for i := 1; i <= 99; i++ {
		o, err := lt.Add(int64(i), int64(i+1))
		if err != nil {
			t.Errorf(err.Error())
		}
		if o != nil {
			go func() {
				o.Lock()
				o.Unlock()
			}()
		}
	}

	_, err := lt.Add(100, 1)
	if err == nil {
		t.Errorf("Deadlock not detected")
	} else {
		commons.Logger.Debugf("Deadlock detected")
	}

	lt.Remove(23)
	_, err = lt.Add(100, 1)
	if err != nil {
		t.Errorf("Deadlock detected")
	} else {
		commons.Logger.Debugf("Deadlock not detected")
	}
}
