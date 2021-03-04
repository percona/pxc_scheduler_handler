package global

import (
	"fmt"
	"reflect"

	"github.com/Tusamarco/toml"
	log "github.com/sirupsen/logrus"
)

/*
Here we have the references objects and methods to deal with the configuration file
Configuration is written in toml using the libraries found in: github.com/Tusamarco/toml
Configuration file has 3 main section:
	[pxccluster]
		...
	[proxysql]
		...
    [globalScheduler]
*/

// Configuration is the main structure working as a container for
// the configuration sections.
// So far only 3 but this may increase like logs for instance
type Configuration struct {
	ProxySQL   ProxySQLConfig   `toml:"proxysql"`
	PxcCluster PxcClusterConfig `toml:"pxccluster"`
	Global     SchedulerConfig  `toml:"Global"`
}

// PxcClusterConfig is Pxc cluster configuration class
type PxcClusterConfig struct {
	ActiveFailover     int
	FailBack           bool
	CheckTimeOut       int
	ClusterID          int
	Development        bool
	DevelopmentTime    int32
	Host               string
	LogDir             string
	LogLevel           string
	MainSegment        int
	MaxNumWriters      int
	OS                 string
	Password           string
	Port               int
	RetryDown          int
	RetryUp            int
	SinglePrimary      bool
	SslClient          string
	SslKey             string
	SslCa              string
	SslcertificatePath string
	User               string
	WriterIsAlsoReader int // Shouldn't it be bool?
	HgW                int
	HgR                int
	BckHgW             int
	BckHgR             int
	SingleWriter       int
	MaxWriters         int
}

// ProxySQLConfig is ProxySQL configuration class
type ProxySQLConfig struct {
	Host         string
	Password     string
	Port         int
	User         string
	Clustered    bool
	LockFilePath string
}

// SchedulerConfig is the class for ProxySQL scheduler configuration
type SchedulerConfig struct {
	Debug              bool
	LogLevel           string
	LogTarget          string // #stdout | file
	LogFile            string //"/tmp/pscheduler"
	Daemonize          bool
	DaemonInterval     int
	Performance        bool
	LockFileTimeout    int64
	LockClusterTimeout int64
}

// GetConfigFromTomlFile parses toml config into struct
func GetConfigFromTomlFile(configPath string) *Configuration {
	var config Configuration
	if _, err := toml.DecodeFile(configPath, &config); err != nil {
		fmt.Println(err)
		return nil
	}
	return &config
}

// ConfigProvider is the delegate responsible for providing configuration
type ConfigProvider func(context string) *Configuration

// GetConfig provides configuration obtained from configProvider after validation
func GetConfig(configProvider ConfigProvider, context string) *Configuration {
	config := configProvider(context)
	if config != nil && config.sanityCheck() {
		return config
	}
	return nil
}

func sanitizeBoundaries(iface interface{}) bool {
	ifv := reflect.ValueOf(iface)
	ift := reflect.TypeOf(iface)

	// go deeper into the structure
	for i := 0; i < ift.NumField(); i++ {
		v := ifv.Field(i)
		switch v.Kind() {
		case reflect.Struct:
			if !sanitizeBoundaries(ifv.Field(i).Interface()) {
				return false
			}
		case reflect.Int:
			if ifv.Field(i).Int() < 0 {
				log.Error(fmt.Sprintf("Wrong value of %v parameter: %v", ift.Field(i).Name, ifv.Field(i).Int()))

				return false
			}
		}
	}
	return true
}

func (conf *Configuration) sanitizeValues() bool {
	if conf.PxcCluster.WriterIsAlsoReader > 1 {
		log.Error(fmt.Sprintf("Wrong value of WriterIsAlsoReader parameter: %v", conf.PxcCluster.WriterIsAlsoReader))
		return false
	}

	if conf.PxcCluster.ActiveFailover > 1 {
		log.Error(fmt.Sprintf("Wrong value of ActiveFailover parameter: %v", conf.PxcCluster.ActiveFailover))
		return false
	}

	// Check for correct LockFilePath
	if conf.ProxySQL.LockFilePath == "" {
		log.Warn(fmt.Sprintf("LockFilePath is invalid. Currently set to: |%s|  I will set to /tmp/ ", conf.ProxySQL.LockFilePath))
		conf.ProxySQL.LockFilePath = "/tmp"
		if !CheckIfPathExists(conf.ProxySQL.LockFilePath) {
			log.Error(fmt.Sprintf("LockFilePath is not accessible currently set to: |%s|", conf.ProxySQL.LockFilePath))
			return false
		}
	}

	return true
}
func (conf *Configuration) sanitizeParamsValues() bool {
	// for int's we need values >= 0. Probably we should change (almost) all
	// int -> uint
	if !sanitizeBoundaries(*conf) || !conf.sanitizeValues() {
		return false
	}

	return true
}

func (conf *Configuration) sanityCheck() bool {
	if !conf.sanitizeParamsValues() {
		return false
	}

	// check for single primary and writer is also reader
	if conf.PxcCluster.MaxNumWriters > 1 &&
		conf.PxcCluster.SinglePrimary {
		log.Error("Configuration error cannot have SinglePrimary true and MaxNumWriter >1")
		return false
	}

	if conf.PxcCluster.WriterIsAlsoReader != 1 && (conf.PxcCluster.MaxWriters > 1 || !conf.PxcCluster.SinglePrimary) {
		log.Error("Configuration error cannot have WriterIsAlsoReader NOT = 1 and use more than one Writer")
		return false
	}

	// check for HG consistency
	// here HG and backup HG must match
	if conf.PxcCluster.HgW+8000 != conf.PxcCluster.BckHgW || conf.PxcCluster.HgR+8000 != conf.PxcCluster.BckHgR {
		log.Error(fmt.Sprintf("Hostgroups and Backup HG do not match. HGw %d - BKHGw %d; HGr %d - BKHGr %d",
			conf.PxcCluster.HgW,
			conf.PxcCluster.BckHgW,
			conf.PxcCluster.HgR,
			conf.PxcCluster.BckHgR))
		return false
	}

	return true
}
