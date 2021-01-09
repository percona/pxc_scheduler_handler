package Global

import (
	"fmt"
	toml "github.com/Tusamarco/toml"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)


//perfomance settings and structure
var Performance bool
var PerformanceMap map[string][2]int64

func SetPerformanceValue(key string, start bool){
	valStat := [2]int64{0,0}
	if start {
		valStat[0] = time.Now().UnixNano()
	}else{
		valStat = PerformanceMap[key]
		valStat[1]= time.Now().UnixNano()
	}
	PerformanceMap[key] = valStat
}

func ReportPerformance(){
	formatter := message.NewPrinter(language.English)

	log.Info("======== Reporting execution times (nanosec/ms)by phase ============")
	for key, wm := range PerformanceMap {
		value := formatter.Sprintf("%d",  wm[1] - wm[0])
		log.Info("Phase: ", key, " = ", value," us ",strconv.FormatInt( (wm[1] - wm[0])/1000000, 10)," ms" )
	}

}
//
//type PerformanceObject struct {
//	ExecutionPhase map[string][]int64
//}

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
	Proxysql proxySql `toml:"proxysql"`
	Pxcluster pxcCluster `toml:"pxccluster"`
	Global globalScheduler `toml:Global`
}

//Pxc configuration class
type pxcCluster struct {
	ActiveFailover int
	CheckTimeOut int
	ClusterId int
	Debug int
	Development bool
	DevelopmentTime int32
	Host string
	LogDir string
	LogLevel string
	MainSegment int
	MaxNumWriters int
	OS string
	Password string
	Port int
	RetryDown int
	RetryUp int
	SinglePrimary bool
	SslClient string
	SslKey string
	SslCa string
	SslCertificate_path string
	User string
	WriterIsReader int


}

//ProxySQL configuration class
type proxySql struct{
	Host string
	Password string
	Port int
	User string
	Clustered bool
	Initialize bool
}

//Global scheduler conf
type globalScheduler struct{
Debug bool
LogLevel string
LogTarget string // #stdout | file
LogFile string //"/tmp/pscheduler"
Development bool
DevInterval int
Performance bool

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

//initialize the log
func InitLog(config Configuration) {
    //var ilog = logrus.New()
	if( strings.ToLower(config.Global.LogTarget) == "stdout") {
		log.SetOutput(os.Stdout)
	}else if (strings.ToLower(config.Global.LogTarget) == "file" &&
	 		  config.Global.LogFile != ""){
		//try to initialize the log on file if it fails it will redirect to stdout
		file, err := os.OpenFile(config.Global.LogFile, os.O_APPEND|os.O_WRONLY, 0666)
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