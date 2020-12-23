package main

import (
	DO "./lib/DataObjects"
	Global "./lib/Global"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
)


/*
Main function must contains only initial parameter, log system init and main object init
 */
func main() {
//global setup of basic parameters
	const (
		Separator = string(os.PathSeparator)
	)

	var configFile string
    Global.Performance = false

	if len(os.Args) < 2 || len(os.Args) > 2 {
		fmt.Println("You must pass the config-file=xxx parameter ONLY")
		os.Exit(1)
	}
	//read config and return a config object
	configFile = strings.ReplaceAll(string(os.Args[1]), "config-file=", "")
	var currPath, err = os.Getwd()
	var config = Global.GetConfig(currPath + Separator + "config" + Separator + configFile)

	//initialize the log system
	Global.InitLog(config)
	//should we track performance or not
	Global.Performance = config.Global.Performance

	/*
	main game start here defining the Proxy Objects
	*/

	//initialize performance collection if requested
	if Global.Performance {
		Global.PerformanceMap = make(map[string][2]int64)
		Global.SetPerformanceValue("main",true)
	}
	proxysqlCluster := new(DO.ProxySQLCluster)
	proxysqlNode := new(DO.ProxySQLNode)

	if err != nil {
		panic(err.Error())
		os.Exit(1)
	}

	if config.Proxysql.Clustered {
		proxysqlCluster.Active = true
		proxysqlCluster.User = config.Proxysql.User
		proxysqlCluster.Password = config.Proxysql.Password

		nodes:= proxysqlCluster.GetProxySQLnodes()

		log.Info(" Number of ProxySQL cluster nodes: " , len(nodes))
	} else {
		if proxysqlNode.Init(config) {
			if log.GetLevel() == log.DebugLevel {
				log.Debug("ProxySQL node initialized ",proxysqlNode)
			}
		}
	}


	/*
	Final cleanup
	 */
	if proxysqlNode != nil {
		if proxysqlNode.CloseConnection(){
			if log.GetLevel() == log.DebugLevel {
				log.Info("Connection close")
			}
		}
	}

	if Global.Performance{
		Global.SetPerformanceValue("main",false)
		Global.ReportPerformance()
	}


    var datanode DO.DataNodePxc
	datanode.DataNodeBase.Comment="aa"

	config.Pxcluster.ActiveFailover = 2

	//my map with records
	//allWm := make(map[int32]Windmill)

	//// Open up our database connection.
	//// I've set up a database on my local machine using phpmyadmin.
	//// The database is called testDb
	//db, err := sql.Open("mysql", "app_test:test@tcp(192.168.4.22:3306)/windmills_s")
	//
	//// if there is an error opening the connection, handle it
	//if err != nil {
	//	panic(err.Error())
	//}

	// defer the close till after the main function has finished
	// executing
	//defer db.Close()


	//recordset, err  := db.Query("SELECT * from windmills_s.windmills1 limit 10")
	//for recordset.Next(){
	//	var wm Windmill
	//	err = recordset.Scan(
	//		&wm.id,
	//		&wm.uuid,
	//		&wm.millid,
	//		&wm.kwatts_s,
	//		&wm.date,
	//		&wm.location,
	//		&wm.active,
	//		&wm.time,
	//		&wm.strrecordtype)
	//
	//	allWm[wm.id] = wm
	//
	//}
	//if err != nil {
	//	panic(err.Error()) // proper error handling instead of panic in your app
	//}
	//for key, wm := range allWm {
	//	log.WithFields(log.Fields{"key": key,
	//		"windmill id": wm.id,
	//		"KW": wm.kwatts_s,
	//		"uuid": wm.uuid,
	//		"time": wm.time}).Info()
	//	//fmt.Printf(  "%d Windmills id  %d KW = %d  uuid = %s  time = %s \n",
	//	//	key,wm.id,wm.kwatts_s,wm.uuid, wm.time )
	//}
}
