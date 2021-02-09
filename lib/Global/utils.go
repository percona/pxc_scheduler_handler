package Global

import (
	log "github.com/sirupsen/logrus"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MyWaitGroup struct {
	sync.WaitGroup
	count int
}

func (wg *MyWaitGroup) WaitTimeout(timeout time.Duration) bool {
	done := make(chan struct{})

	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
		return false

	case <-time.After(timeout):
		return true
	}
}

func (wg *MyWaitGroup) IncreaseCounter() {
	wg.count++
}
func (wg *MyWaitGroup) DecreaseCounter() {
	if wg.count > 0 {
		wg.count--
	}
}

func (wg *MyWaitGroup) ReportCounter() int {
	return wg.count
}

const (
	Separator = string(os.PathSeparator)
)

//==================================

func FromStringToMAp(mystring string, separator string) map[string]string {
	myMap := make(map[string]string)
	if mystring != "" && separator != "" {
		keyValuePairArray := strings.Split(mystring, separator)
		for _, keyValuePair := range keyValuePairArray {
			//keyValuePair = strings.Trim(keyValuePair," ")
			keyValueSplit := strings.Split(keyValuePair, "=")
			if len(keyValueSplit) > 1 {
				var key = strings.TrimSpace(keyValueSplit[0])
				var value = strings.TrimSpace(keyValueSplit[1])
				if len(key) > 0 && key != "" && value != "" {
					myMap[key] = value
				}
			}
		}
	}
	return myMap
}

func ToInt(myString string) int {
	if len(myString) > 0 {
		i, err := strconv.Atoi(myString)
		if err != nil {
			pc, fn, line, _ := runtime.Caller(1)
			log.Error(pc, " ", fn, " ", line, ": ", err)
			return -1
		} else {
			return i
		}
	}
	return 0
}

func ToBool(myString string, boolTrueString string) bool {
	myString = strings.ToLower(myString)
	boolTrueString = strings.ToLower(boolTrueString)
	if myString != "" && myString == boolTrueString {
		return true
	} else {
		return false
	}
}

func Bool2int(b bool) int {
	if b {
		return 1
	}
	return 0
}


//----------------------
/* =====================
STATS
*/

type StatSyncMap struct {
	sync.RWMutex
	internal map[string][2]int64
}

func NewRegularIntMap() *StatSyncMap {
	return &StatSyncMap{
		internal: make(map[string][2]int64),
	}
}

func (rm *StatSyncMap) Load(key string) (value [2]int64, ok bool) {
	rm.RLock()
	defer rm.RUnlock()
	result, ok := rm.internal[key]

	return result, ok
}

func (rm *StatSyncMap) Delete(key string) {
	rm.Lock()
	defer rm.Unlock()
	delete(rm.internal, key)

}

func (rm *StatSyncMap) get(key string) [2]int64 {
	rm.Lock()
	defer rm.Unlock()
	return rm.internal[key]

}

func (rm *StatSyncMap) Store(key string, value [2]int64) {
	rm.Lock()
	defer rm.Unlock()
	rm.internal[key] = value

}

func (rm *StatSyncMap) ExposeMap() map[string][2]int64 {
	return rm.internal
}
