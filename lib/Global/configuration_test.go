package Global_test

import (
	"testing"

	"../Global"
)

func defaultConfigProvider(map[string]string) *Global.Configuration {
	configuration := Global.Configuration{}

	configuration.Proxysql.Port = 1000

	configuration.Pxcluster.SinglePrimary = true
	configuration.Pxcluster.Host = "defaultHost"

	configuration.Global.Debug = true

	return &configuration
}

func TestGetConfigBaseReturnsNull(t *testing.T) {
	config, err := Global.GetConfig([]string{"p0", "--configfile=test"}, func(map[string]string) *Global.Configuration { return nil })
	if err == nil {
		t.Errorf("Expected error returned from GetConfig()")
	}
	if err != nil && config != nil {
		t.Errorf("No config expected when error")
	}
}

func TestGetConfigOverrideInt(t *testing.T) {
	config, _ := Global.GetConfig([]string{"p0", "--configfile=test --proxysql.port=555"}, defaultConfigProvider)
	if config.Proxysql.Port != 555 {
		t.Errorf("Overrided int value expected. Got: %v", config.Proxysql.Port)
	}
}

func TestGetConfigOverrideString(t *testing.T) {
	config, _ := Global.GetConfig([]string{"p0", "--configfile=test --pxccluster.host=modified"}, defaultConfigProvider)
	if config.Pxcluster.Host != "modified" {
		t.Errorf("Overrided string value expected. Got: %v", config.Pxcluster.Host)
	}
}

func TestGetConfigOverrideBool1(t *testing.T) {
	config, _ := Global.GetConfig([]string{"p0", "--configfile=test --global.debug=0"}, defaultConfigProvider)
	if config.Global.Debug != false {
		t.Errorf("Overrided bool value expected. Got: %v", config.Global.Debug)
	}
}

func TestGetConfigOverrideBool2(t *testing.T) {
	config, _ := Global.GetConfig([]string{"p0", "--configfile=test --global.debug=FALSE"}, defaultConfigProvider)
	if config.Global.Debug != false {
		t.Errorf("Overrided bool value expected. Got: %v", config.Global.Debug)
	}
}

func TestGetConfigNoConfigfile(t *testing.T) {
	config, err := Global.GetConfig([]string{"p0", "--global.debug=FALSE"}, defaultConfigProvider)
	if err == nil {
		t.Errorf("Expected error returned from GetConfig()")
	}
	if err != nil && config != nil {
		t.Errorf("No config expected when error")
	}
}
