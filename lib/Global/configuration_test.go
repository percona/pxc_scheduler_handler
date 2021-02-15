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
	configuration.Pxcluster.BckHgW = configuration.Pxcluster.HgW + 8000
	configuration.Pxcluster.BckHgR = configuration.Pxcluster.HgR + 8000

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

func TestGetConfigHgWDoNotMatch(t *testing.T) {
	config, err := Global.GetConfig([]string{"p0", "--configfile=test --pxccluster.BckHgW=2"}, defaultConfigProvider)
	if err == nil {
		t.Errorf("Expected error returned from GetConfig()")
	}
	if err != nil && config != nil {
		t.Errorf("No config expected when error")
	}
}

func TestGetConfigHgRDoNotMatch(t *testing.T) {
	config, err := Global.GetConfig([]string{"p0", "--configfile=test --pxccluster.BckHgR=2"}, defaultConfigProvider)
	if err == nil {
		t.Errorf("Expected error returned from GetConfig()")
	}
	if err != nil && config != nil {
		t.Errorf("No config expected when error")
	}
}

func TestGetConfigHgRDoMatch(t *testing.T) {
	config, err := Global.GetConfig([]string{"p0", "--configfile=test --pxccluster.BckHgR=8020 --pxccluster.HgR=20"}, defaultConfigProvider)
	if err != nil {
		t.Errorf("Got error from GetConfig()")
	}
	if config == nil {
		t.Errorf("No config obtained")
	}

	if config.Pxcluster.BckHgR != 8020 {
		t.Errorf("Wrong BckHgR. Expected 8020, got %v", config.Pxcluster.BckHgR)
	}

	if config.Pxcluster.HgR != 20 {
		t.Errorf("Wrong HgR. Expected 20, got %v", config.Pxcluster.HgR)
	}
}

func TestGetConfigHgWDoMatch(t *testing.T) {
	config, err := Global.GetConfig([]string{"p0", "--configfile=test --pxccluster.BckHgW=8021 --pxccluster.HgW=21"}, defaultConfigProvider)
	if err != nil {
		t.Errorf("Got error from GetConfig()")
	}
	if config == nil {
		t.Errorf("No config obtained")
	}

	if config.Pxcluster.BckHgW != 8021 {
		t.Errorf("Wrong BckHgR. Expected 8021, got %v", config.Pxcluster.BckHgW)
	}

	if config.Pxcluster.HgW != 21 {
		t.Errorf("Wrong HgR. Expected 21, got %v", config.Pxcluster.HgW)
	}
}
