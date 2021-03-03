package dataobjects_test

import (
	"testing"

	DO "../DataObjects"
	global "../Global"
)

var gConfig = global.Configuration{
	ProxySQL: global.ProxySQLConfig{
		Port: 1000,
	},
	PxcCluster: global.PxcClusterConfig{
		SinglePrimary: true,
		Host:          "defaultHost",
		MaxNumWriters: 1,
		HgW:           1,
		BckHgW:        8001,
		HgR:           2,
		BckHgR:        8002,
	},
	Global: global.SchedulerConfig{
		Debug: true,
	},
}

func TestLockerInitNil(t *testing.T) {
	locker := DO.Locker{}
	if locker.Init(nil) {
		t.Errorf("Expected intitialization failure")
	}
}

func TestLockerInit(t *testing.T) {

	locker := DO.Locker{}
	if !locker.Init(&gConfig) {
		t.Errorf("Expected intitialization success")
	}
}
