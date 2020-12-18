package lib

import (
	"fmt"
	toml "github.com/Tusamarco/toml"
	"syscall"
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
	Proxysql proxySql `toml:"proxysql"`
	Pxcluster pxcCluster `toml:"pxccluster"`
	Global globalScheduler `toml:global`
}

//Pxc configuration class
type pxcCluster struct {
	ActiveFailover int
	CheckTimeOut int32
	Debug int
	Development bool
	DevelopmentTime int32
	Host string `toml:"proxysql"`
	LogDir string
	LogLevel string
	MainSegment int
	MaxNumWriters int
	OS string
	Password string `toml:"proxysql"`
	Port int  `toml:"proxysql"`
	RetryDown int
	RetryUp int
	SinglePrimary bool
	User string `toml:"proxysql"`
	WriterIsReader int


}

//ProxySQL configuration class
type proxySql struct{
	Host string
	Password string
	Port int
	User string
	Clustered bool
}

//Global scheduler conf

type globalScheduler struct{
Debug bool
LogLevel string
LogTarget string // #stdout | file
LogFile string //"/tmp/pscheduler"
Development bool
DevInterval int

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

