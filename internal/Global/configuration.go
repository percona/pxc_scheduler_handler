/*
 * Copyright (c) Marco Tusa 2021 - present
 *                     GNU GENERAL PUBLIC LICENSE
 *                        Version 3, 29 June 2007
 *
 *  Copyright (C) 2007 Free Software Foundation, Inc. <https://fsf.org/>
 *  Everyone is permitted to copy and distribute verbatim copies
 *  of this license document, but changing it is not allowed.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package Global

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
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
	Proxysql  ProxySql        `toml:"proxysql"`
	Pxcluster PxcCluster      `toml:"pxccluster"`
	Global    GlobalScheduler `toml:"Global"`
}

//Pxc configuration class
type PxcCluster struct {
	ActiveFailover         int
	FailBack               bool
	CheckTimeOut           int
	ClusterId              int
	Debug                  int //Deprecated: this is redundant and not in use
	Development            bool
	DevelopmentTime        int32
	Host                   string
	LogDir                 string
	LogLevel               string
	MainSegment            int
	MaxNumWriters          int
	OS                     string
	Password               string
	Port                   int
	RetryDown              int
	RetryUp                int
	SinglePrimary          bool
	SslClient              string
	SslKey                 string
	SslCa                  string
	SslcertificatePath     string
	User                   string
	WriterIsAlsoReader     int
	HgW                    int
	HgR                    int
	ConfigHgRange		   int `default:"8000"`
	MaintenanceHgRange     int `default:"9000"`
	BckHgW                 int `default:"0"`
	BckHgR                 int `default:"0"`
	SingleWriter           int
	MaxWriters             int
	PersistPrimarySettings int `default:"0"`
}
func (conf *Configuration) fillDefaults(){
	conf.Pxcluster.MaintenanceHgRange = 9000
	conf.Pxcluster.ConfigHgRange = 8000
	conf.Pxcluster.PersistPrimarySettings = 0
}

//ProxySQL configuration class
type ProxySql struct {
	Host                     string
	Password                 string
	Port                     int
	User                     string
	Clustered                bool
	RespectManualOfflineSoft bool `default:false`
	LockFilePath             string
}

//Global scheduler conf
type GlobalScheduler struct {
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

//Methods to return the config as map
func GetConfig(path string) Configuration {
	var config Configuration
	config.fillDefaults()
	if _, err := toml.DecodeFile(path, &config); err != nil {
		log.Error(err)
		syscall.Exit(2)
	}
	return config
}

func (conf *Configuration) SanityCheck() bool {
	//check for single primary and writer is also reader
	if conf.Pxcluster.MaxNumWriters > 1 &&
		conf.Pxcluster.SinglePrimary {
		log.SetReportCaller(true)
		log.Fatal("Configuration error cannot have SinglePrimary true and MaxNumWriter >1")
		return false
		//os.Exit(1)
	}

	if conf.Pxcluster.WriterIsAlsoReader != 1 && (conf.Pxcluster.MaxWriters > 1 || !conf.Pxcluster.SinglePrimary) {
		log.SetReportCaller(true)
		log.Fatal("Configuration error cannot have WriterIsAlsoReader NOT = 1 and use more than one Writer")
		return false
		//os.Exit(1)
	}

	//check for HG consistency
	// here HG and backup HG must match

	if conf.Pxcluster.BckHgW > 0 || conf.Pxcluster.BckHgR > 0 {
		log.SetReportCaller(false)
		log.Warning(fmt.Sprintf("Configuration hostgroups (BckHgW & BckHgR) options are DEPRECATED Please configure only ConfigHgRange instead. IE HGw=%d, HGr=%d, ConfigHgRange=%d",
			conf.Pxcluster.HgW,
			conf.Pxcluster.HgR,
			conf.Pxcluster.ConfigHgRange))

		conf.Pxcluster.BckHgW = conf.Pxcluster.ConfigHgRange + conf.Pxcluster.HgW
		conf.Pxcluster.BckHgR = conf.Pxcluster.ConfigHgRange + conf.Pxcluster.HgR

//		return false
		//os.Exit(1)
	} else{
		conf.Pxcluster.BckHgW = conf.Pxcluster.ConfigHgRange + conf.Pxcluster.HgW
		conf.Pxcluster.BckHgR = conf.Pxcluster.ConfigHgRange + conf.Pxcluster.HgR
	}

	//Check for correct LockFilePath
	if conf.Proxysql.LockFilePath == "" {
		log.Warn(fmt.Sprintf("LockFilePath is invalid. Currently set to: |%s|  I will set to /tmp/ ", conf.Proxysql.LockFilePath))
		conf.Proxysql.LockFilePath = "/tmp"
		if !CheckIfPathExists(conf.Proxysql.LockFilePath) {
			log.SetReportCaller(true)
			log.Fatal(fmt.Sprintf("LockFilePath is not accessible currently set to: |%s|", conf.Proxysql.LockFilePath))
			return false
			//os.Exit(1)
		}

	}

	//check SSL path and certificates
	if conf.Pxcluster.SslcertificatePath !="" {
		Separator := string(os.PathSeparator)
		//var failing = false
		log.SetReportCaller(false)
		if !CheckIfPathExists(conf.Pxcluster.SslcertificatePath) {
			log.Warning(fmt.Sprintf("SSL Path is not accessible currently set to: |%s|", conf.Pxcluster.SslcertificatePath))
			//failing = true
		}else {
			if !CheckIfPathExists(conf.Pxcluster.SslcertificatePath + Separator + conf.Pxcluster.SslCa) {
				log.Warning(fmt.Sprintf("SSLCA Path is not accessible currently set to: |%s|", conf.Pxcluster.SslcertificatePath+Separator+conf.Pxcluster.SslCa))
				//failing = true
			}
			if !CheckIfPathExists(conf.Pxcluster.SslcertificatePath + Separator + conf.Pxcluster.SslKey) {
				log.Warning(fmt.Sprintf("SSL Key Path is not accessible currently set to: |%s|", conf.Pxcluster.SslcertificatePath+Separator+conf.Pxcluster.SslKey))
				//failing = true
			}

			if !CheckIfPathExists(conf.Pxcluster.SslcertificatePath + Separator + conf.Pxcluster.SslClient) {
				log.Warning(fmt.Sprintf("SSL Client Path is not accessible currently set to: |%s|", conf.Pxcluster.SslcertificatePath+Separator+conf.Pxcluster.SslClient))
				//failing = true
			}
		}
		//if failing{
		//	return false
		//}
		return true
	}

	return true

}

//initialize the log
func InitLog(config Configuration) bool {

	//set a consistent output for the log no matter if file or stdout
	formatter := LogFormat{}
	formatter.TimestampFormat = "2006-01-02 15:04:05"
	log.SetFormatter(&formatter)

	if strings.ToLower(config.Global.LogTarget) == "stdout" {
		log.SetOutput(os.Stdout)

		//log.SetFormatter(&log.TextFormatter{
		//	DisableColors: true,
		//	FullTimestamp: true,
		//})

	} else if strings.ToLower(config.Global.LogTarget) == "file" &&
		config.Global.LogFile != "" {
		//try to initialize the log on file if it fails it will redirect to stdout
		file, err := os.OpenFile(config.Global.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err == nil {
			log.SetOutput(file)
		} else {
			log.Error("Error logging to file ", err.Error())
			//log.Error("Failed to log to file, using default stderr")
			return false
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

	if log.GetLevel() == log.DebugLevel {
		log.Info("Go version: ", runtime.Version())
		log.Debug("Testing the log")
		log.Info("Testing the log")
		log.Warning("Testing the log")
		log.Error("testing log errors")
		log.SetLevel(log.DebugLevel)
		log.Debug("Log initialized")
	}
	return true
}

type LogFormat struct {
	TimestampFormat string
}

func (f *LogFormat) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer

	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	b.WriteString("\x1b[" + strconv.Itoa(getColorByLevel(entry.Level)) + "m")
	b.WriteByte('[')
	b.WriteString(strings.ToUpper(entry.Level.String()))
	b.WriteString("]")
	b.WriteString("\x1b[0m")
	b.WriteByte(':')
	b.WriteString(entry.Time.Format(f.TimestampFormat))

	if entry.Message != "" {
		b.WriteString(" - ")
		b.WriteString(entry.Message)
	}

	if len(entry.Data) > 0 {
		b.WriteString(" || ")
	}
	for key, value := range entry.Data {
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteByte('{')
		fmt.Fprint(b, value)
		b.WriteString("}, ")
	}

	b.WriteByte('\n')
	return b.Bytes(), nil
}

const (
	colorRed    = 31
	colorYellow = 33
	colorBlue   = 36
	colorGray   = 34 //37
	paniclevel  = 35
)

func getColorByLevel(level log.Level) int {
	switch level {
	case log.DebugLevel:
		return colorGray
	case log.WarnLevel:
		return colorYellow
	case log.ErrorLevel:
		return colorRed
	case log.PanicLevel, log.FatalLevel:
		return paniclevel
	default:
		return colorBlue
	}
}
