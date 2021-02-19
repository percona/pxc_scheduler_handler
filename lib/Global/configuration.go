package Global

import (
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/Tusamarco/toml"
	log "github.com/sirupsen/logrus"
	//"github.com/alexflint/go-arg"
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

//Main structure working as container for the configuration sections
// So far only 2 but this may increase like logs for instance
type Configuration struct {
	Proxysql  proxySql        `toml:"proxysql"`
	Pxcluster pxcCluster      `toml:"pxccluster"`
	Global    globalScheduler `toml:"Global"`
}

//Pxc configuration class
type pxcCluster struct {
	ActiveFailover     int
	FailBack           bool
	CheckTimeOut       int
	ClusterId          int
	Debug              int
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
	WriterIsAlsoReader int
	HgW                int
	HgR                int
	BckHgW             int
	BckHgR             int
	SingleWriter       int
	MaxWriters         int
}

//ProxySQL configuration class
type proxySql struct {
	Host       string
	Password   string
	Port       int
	User       string
	Clustered  bool
	LockFilePath string
}

//Global scheduler conf
type globalScheduler struct {
	Debug       bool
	LogLevel    string
	LogTarget   string // #stdout | file
	LogFile     string //"/tmp/pscheduler"
	Development bool
	DevInterval int
	Performance bool
	LockFileTimeout int64
	LockClusterTimeout int64

}

//Methods to return the config as map
func GetConfig(path string) Configuration {
	var config Configuration
	if _, err := toml.DecodeFile(path, &config); err != nil {
		fmt.Println(err)
		syscall.Exit(2)
	}
	return config
}

func (conf *Configuration) SanityCheck() {
	//check for single primary and writer is also reader
	if conf.Pxcluster.MaxNumWriters > 1 &&
		conf.Pxcluster.SinglePrimary {
		log.Error("Configuration error cannot have SinglePrimary true and MaxNumWriter >1")

		os.Exit(1)
	}

	if conf.Pxcluster.WriterIsAlsoReader != 1 && (conf.Pxcluster.MaxWriters > 1 || !conf.Pxcluster.SinglePrimary) {
		log.Error("Configuration error cannot have WriterIsAlsoReader NOT = 1 and use more than one Writer")

		os.Exit(1)

	}

	//check for HG consistency
	// here HG and backup HG must match
	if conf.Pxcluster.HgW+8000 != conf.Pxcluster.BckHgW || conf.Pxcluster.HgR+8000 != conf.Pxcluster.BckHgR {
		log.Error(fmt.Sprintf("Hostgroups and Backup HG do not match. HGw %d - BKHGw %d; HGr %d - BKHGr %d",
			conf.Pxcluster.HgW,
			conf.Pxcluster.BckHgW,
			conf.Pxcluster.HgR,
			conf.Pxcluster.BckHgR))
		os.Exit(1)
	}

	//Check for correct LockFilePath
	if conf.Proxysql.LockFilePath =="" || !CheckIfPathExists(conf.Proxysql.LockFilePath){
		log.Error(fmt.Sprintf("LockFilePath is either invalid or not accessible currently set to: |%s|",conf.Proxysql.LockFilePath))
		os.Exit(1)
	}

}

//initialize the log
func InitLog(config Configuration) {
	//var ilog = logrus.New()
	if strings.ToLower(config.Global.LogTarget) == "stdout" {
		log.SetOutput(os.Stdout)
	} else if strings.ToLower(config.Global.LogTarget) == "file" &&
		config.Global.LogFile != "" {
		//try to initialize the log on file if it fails it will redirect to stdout
		file, err := os.OpenFile(config.Global.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err == nil {
			log.SetOutput(file)
		} else {
			log.Error("Failed to log to file, using default stderr")
		}
	}

	//set log severity level
	switch level := strings.ToLower(config.Global.LogLevel); level {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warning":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.ErrorLevel)
	}

	//log.SetFormatter(&log.JSONFormatter{}) <-- future for Kafaka
	if log.GetLevel() == log.DebugLevel {
		log.Debug("Testing the log")
		log.Info("Testing the log")
		log.Warning("Testing the log")
		log.Error("testing log errors")
		log.SetLevel(log.DebugLevel)
		log.Debug("Log initialized")
	}

}
