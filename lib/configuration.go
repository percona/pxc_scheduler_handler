package lib

import (
	"fmt"
	toml "github.com/Tusamarco/toml"
	"syscall"
)

type Configuration struct {
	Proxysql proxySql `toml:"proxysql"`
	Pxcluster pxcCluster `toml:"pxccluster"`
}


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

type proxySql struct{
	Host string
	Password string
	Port int
	User string
}

func GetConfig(path string) Configuration {
	var config Configuration
	if _, err := toml.DecodeFile(path, &config); err != nil {
		fmt.Println(err)
		syscall.Exit(2)
	}
	return config
}

