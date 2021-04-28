package DataObjects

import (
	"reflect"
	"testing"
)

func TestLockerImpl_findLock(t *testing.T) {
	//log.SetLevel(log.DebugLevel)

	var tests = []lockerRule{}
	nodes := make(map[string]ProxySQLNodeImpl)
	locker := testLockerFactory()

	tests = rulesTestFindLock(locker)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
				nodes[tt.proxysqlNode1.Dns] = tt.proxysqlNode1
				nodes[tt.proxysqlNode2.Dns] = tt.proxysqlNode2


			if _,got := locker.findLock(nodes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf(" %s findLock() = %v, want %v",tt.name, got, tt.want)
			}
		})
	}
}
