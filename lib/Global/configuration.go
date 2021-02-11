package Global

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Tusamarco/toml"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	//"github.com/alexflint/go-arg"
)

//perfomance settings and structure
var Performance bool
var PerformanceMapOrdered *OrderedPerfMap

var PerformanceMap *StatSyncInfo //map[string][2]int64

func SetPerformanceValue(key string, start bool) {
	valStat := [2]int64{0, 0}
	if start {
		valStat[0] = time.Now().UnixNano()
	} else {
		valStat = PerformanceMap.get(key)
		valStat[1] = time.Now().UnixNano()
	}
	PerformanceMap.Store(key, valStat) //  ExposeMap()[key] = valStat
}

func SetPerformanceObj(key string, start bool,logLevel log.Level) {
	var perfObj PerfObject
	valStat := [2]int64{}

	if val, exists := PerformanceMapOrdered.Get(key); !exists{
		perfObj  = val
		perfObj.LogLevel = logLevel
		perfObj.Name = key
		valStat = [2]int64{0, 0}
	}else{
		perfObj = val
		valStat = perfObj.Time
	}

	if start {
		valStat[0] = time.Now().UnixNano()
	} else {
		valStat[1] = time.Now().UnixNano()
	}
	perfObj.Time = valStat

	PerformanceMapOrdered.Set(key, perfObj) //  ExposeMap()[key] = valStat
}

func ReportPerformance() {
	formatter := message.NewPrinter(language.English)

	if log.InfoLevel <= log.GetLevel() {
		fmt.Println("======== Reporting execution times (nanosec/ms)by phase ============")
	}
	it := PerformanceMapOrdered.Iterator()
	for {
		i, _, perfObj := it()
		if perfObj.Name != "" {
			time := perfObj.Time
			value := formatter.Sprintf("%d", time[1]-time[0])
			if perfObj.LogLevel <= log.GetLevel() {
				fmt.Println("Phase: ", perfObj.Name, " = ", value, " us ", strconv.FormatInt((time[1]-time[0])/1000000, 10), " ms")
			}
		}

		if i == nil {
			break
		}
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

var Args struct {
	ConfigFile string `arg:"required" arg:"--configFile" help:"Config file name"`
	Configpath string `arg:" --configpath" help:"Config path name. if omitted execution directory is used"`

	//Global scheduler conf
	Debug       bool
	LogLevel    string
	LogTarget   string // #stdout | file
	LogFile     string //"/tmp/pscheduler"
	Development bool
	DevInterval int
	Performance bool

	//type pxcCluster struct {
	ActiveFailover     int
	FailBack           bool
	CheckTimeOut       int
	ClusterId          int
	DevelopmentTime    int32
	LogDir             string
	MainSegment        int
	MaxNumWriters      int
	RetryDown          int
	RetryUp            int
	SinglePrimary      bool
	SslClient          string
	SslKey             string
	SslCa              string
	SslcertificatePath string
	WriterIsAlsoReader int
	HgW                int
	HgR                int
	BckHgW             int
	BckHgR             int
	SingleWriter       int
	MaxWriters         int

	//ProxySQL configuration class
	//type proxySql struct {
	Host      string
	Password  string
	Port      int
	User      string
	Clustered bool
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
	if conf.Pxcluster.MaxNumWriters > 1 &&
		conf.Pxcluster.SinglePrimary {
		log.Error("Configuration error cannot have SinglePrimary true and MaxNumWriter >1")

		os.Exit(1)
	}

	if conf.Pxcluster.WriterIsAlsoReader != 1 && (conf.Pxcluster.MaxWriters > 1 || !conf.Pxcluster.SinglePrimary) {
		log.Error("Configuration error cannot have WriterIsAlsoReader NOT = 1 and use more than one Writer")

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

//WIP not use
func (config *Configuration) AlignWithArgs(osArgs []string) {
	iargs := len(osArgs) - 1
	localArgs := make([]string, iargs, 100)
	iargs = 0
	for i := 1; i < len(osArgs); i++ {
		temp := strings.ReplaceAll(osArgs[i], "--", "")
		localArgs[iargs] = temp
		iargs++
	}

	refArgs := reflect.ValueOf(Args)
	//refGlobal := reflect.ValueOf(config.Global)
	//refProxy := reflect.ValueOf(&config.Proxysql)
	//refPxc := reflect.ValueOf(&config.Pxcluster)


	//argsArray := make([]string,refArgs.NumField())


	fmt.Print("\n")
	var err error
	//Now wr set the values from command line to Conf structure
	for iargs = 0; iargs < len(localArgs) ; iargs++ {
		value := refArgs.FieldByName(localArgs[iargs])
		err = ReflectStructField(&config.Pxcluster,localArgs[iargs])

		if err == nil {
			f :=  reflect.ValueOf(&config.Pxcluster)
			fType := f.Elem().Type()
			typeOf := reflect.ValueOf(config.Pxcluster).Type()

			for i := 0 ; i < f.Elem().NumField() ; i++{
				if f.Elem().Field(i).CanInterface(){
					name := typeOf.Field(i).Name
					fmt.Print(name,"  ", localArgs[iargs],"\n")
					if name == localArgs[iargs]{
						fieldType := typeOf.Field(i).Type.Name()
						kValue := reflect.ValueOf(value)
						fmt.Printf("field type: ", fieldType,"\n")
						fmt.Printf(" value = %d \n",kValue)
						if kValue.IsValid() {
							f.Elem().Field(i).Set(kValue.Convert(fType.Field(i).Type))
						}
					}
				}
				//fmt.Print(localArgs[iargs], " = ", value, "\n")
				break
			}
		}else{
			fmt.Print(localArgs[iargs], " Not present in refPxc ",err, "\n")
		}

	}


	// we need to loop for each element
	//typeOfG := refGlobal.Type()

	//fmt.Print(typeOfG.Field(1).Name)

	//iArgs := refArgs.NumField()
	//iG := refGlobal.NumField()
	fmt.Print("\n")

}


func caseInsenstiveFieldByName(v reflect.Value, name string) reflect.Value {
	name = strings.ToLower(name)
	return v.FieldByNameFunc(func(n string) bool { return strings.ToLower(n) == name })
}
