package global_test

import (
	"testing"

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

func TestGetConfigProviderReturnsNull(t *testing.T) {
	config := global.GetConfig(func(string) *global.Configuration { return nil }, "test")

	if config != nil {
		t.Errorf("No config expected when error")
	}
}

func TestGetConfigReturnsWhatProviderProvides(t *testing.T) {
	config := global.GetConfig(func(string) *global.Configuration { return &gConfig }, "test")
	if config == nil {
		t.Errorf("Expected config returned from GetConfig()")
	}

	if config.ProxySQL.Port != 1000 ||
		config.PxcCluster.SinglePrimary != true ||
		config.PxcCluster.Host != "defaultHost" ||
		config.Global.Debug != true {
		t.Errorf("Configuration mismatch")
	}
}

func TestGetConfigSinglePrimaryVsMaxWritersSanityCheck(t *testing.T) {
	config := global.GetConfig(func(string) *global.Configuration {
		c := gConfig
		c.PxcCluster.MaxNumWriters = 2
		c.PxcCluster.SinglePrimary = true
		return &c
	}, "test")

	if config != nil {
		t.Errorf("No config expected when error")
	}
}

func TestGetConfigNegativeParamValue(t *testing.T) {
	config := global.GetConfig(func(string) *global.Configuration {
		c := gConfig
		c.PxcCluster.MaxNumWriters = -8
		c.PxcCluster.SinglePrimary = true

		return &c
	}, "test")

	if config != nil {
		t.Errorf("No config expected when error")
	}
}

func TestGetConfigWriteIsReaderInvalidValue(t *testing.T) {
	config := global.GetConfig(func(string) *global.Configuration {
		c := gConfig
		c.PxcCluster.WriterIsAlsoReader = 7

		return &c
	}, "test")

	if config != nil {
		t.Errorf("No config expected when error")
	}
}

func TestGetConfigActiveFailoverInvalidValue(t *testing.T) {
	config := global.GetConfig(func(string) *global.Configuration {
		c := gConfig
		c.PxcCluster.ActiveFailover = 4

		return &c
	}, "test")

	if config != nil {
		t.Errorf("No config expected when error")
	}
}
