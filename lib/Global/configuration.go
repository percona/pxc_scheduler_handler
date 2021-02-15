package Global

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"syscall"

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
	Initialize bool
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
}

func splitArguments(args string) (map[string]string, error) {
	a := regexp.MustCompile("\\s*--")
	tokens := a.Split(args, -1)

	result := make(map[string]string)
	for i := 0; i < len(tokens); i++ {
		if len(tokens[i]) == 0 {
			continue
		}
		kv := strings.Split(tokens[i], "=")
		if len(kv) != 2 {
			return nil, errors.New("Invalid token: " + tokens[i])
		}
		result[strings.ToLower(strings.TrimSpace(kv[0]))] = strings.TrimSpace(kv[1])
	}
	return result, nil
}

type BaseConfigProvider func(map[string]string) *Configuration

func GetConfig(args []string, baseConfigProvider BaseConfigProvider) (*Configuration, error) {
	// we expect exactly one argument which is the string containing
	// all possible arguments (this is because ProxySQL allows scheduler script
	// to be called with at most 5 command line arguments)
	if len(args) != 2 {
		return nil, errors.New("Invalid configuration")
	}

	argsMap, err := splitArguments(args[1])
	if err != nil {
		return nil, errors.New("Error while parsing arguments")
	}

	// configfile is mandatory argument
	if _, ok := argsMap["configfile"]; !ok {
		return nil, errors.New("Missing 'configfile' argument")
	}

	config := baseConfigProvider(argsMap)
	if config == nil {
		return nil, errors.New("Error while loading base config")
	}

	// Now override config got from file with command line parameters
	config.align(argsMap)

	//Let us do a sanity check on the configuration to prevent most obvious issues
	config.sanityCheck()

	return config, nil
}

func alignCommon(ps reflect.Value, k string, v string) bool {
	s := ps.Elem()
	if s.Kind() == reflect.Struct {
		k = strings.ToLower(k)
		f := s.FieldByNameFunc(func(s string) bool { return strings.ToLower(s) == k })
		if f.IsValid() && f.CanSet() {
			switch f.Kind() {
			case reflect.Int:
				if x, err := strconv.Atoi(v); err == nil {
					if !f.OverflowInt(int64(x)) {
						f.SetInt(int64(x))
						return true
					}
				}
			case reflect.String:
				f.SetString(v)
				return true
			case reflect.Bool:
				if b, err := strconv.ParseBool(v); err == nil {
					f.SetBool(b)
					return true
				}
			}
		}
	}
	return false
}

func (conf *pxcCluster) align(k string, v string) bool {
	return alignCommon(reflect.ValueOf(conf), k, v)
}

func (conf *proxySql) align(k string, v string) bool {
	return alignCommon(reflect.ValueOf(conf), k, v)
}

func (conf *globalScheduler) align(k string, v string) bool {
	return alignCommon(reflect.ValueOf(conf), k, v)
}

func (conf *Configuration) align(params map[string]string) {

	for k, v := range params {
		modParam := strings.SplitN(k, ".", 2)
		if len(modParam) != 2 {
			continue
		}
		switch strings.ToLower(modParam[0]) {
		case "proxysql":
			conf.Proxysql.align(modParam[1], v)
		case "pxccluster":
			conf.Pxcluster.align(modParam[1], v)
		case "global":
			conf.Global.align(modParam[1], v)
		}
	}
}

//Methods to return the config as map
func GetConfigFromFile(args map[string]string) *Configuration {
	// parse config file to get the base configuration
	//read config and return a config object
	configFile := args["configfile"]

	var configPath string

	if _, ok := args["configpath"]; ok {
		configPath = args["configpath"] + Separator
	} else {
		currPath, err := os.Getwd()
		if err != nil {
			return nil
		}
		configPath = currPath + Separator + "config" + Separator
	}

	var config Configuration
	if _, err := toml.DecodeFile(configPath+configFile, &config); err != nil {
		fmt.Println(err)
		syscall.Exit(2)
	}
	return &config
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
	if conf.Pxcluster.HgW + 8000 != conf.Pxcluster.BckHgW || conf.Pxcluster.HgR + 8000 != conf.Pxcluster.BckHgR{
		log.Error(fmt.Sprintf("Hostgroups and Backup HG do not match. HGw %d - BKHGw %d; HGr %d - BKHGr %d",
			conf.Pxcluster.HgW,
			conf.Pxcluster.BckHgW,
			conf.Pxcluster.HgR,
			conf.Pxcluster.BckHgR ))
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
