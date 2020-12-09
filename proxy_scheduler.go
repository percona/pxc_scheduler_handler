package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"

	Lib "./lib"
)

type Windmill struct {
	id int32
	uuid string
	millid int
	kwatts_s int
	date string
	location string
	active int
	time string
	strrecordtype string
}


func main() {
	//fmt.Println("Go MySQL Tutorial")
	//var log = logrus.New()
	const(
		Separator = string(os.PathSeparator)
	)
	var configFile string

	if(len(os.Args) < 1 || len(os.Args) > 2){
		fmt.Println("You must pass the config-file=xxx parameter ONLY")
		os.Exit(1)
	}
	configFile = strings.ReplaceAll(string(os.Args[1]),"config-file=","")

	var currPath, err = os.Getwd()
	var config = Lib.GetConfig(currPath + Separator + "config"+ Separator +configFile)

	config.Pxcluster.ActiveFailover = 2
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
	//log.SetFormatter(&log.JSONFormatter{})

	log.Info("Testing the log")
	log.Error("testing log errors")

	//my map with records
	allWm := make(map[int32]Windmill)

	// Open up our database connection.
	// I've set up a database on my local machine using phpmyadmin.
	// The database is called testDb
	db, err := sql.Open("mysql", "app_test:test@tcp(192.168.4.22:3306)/windmills_s")

	// if there is an error opening the connection, handle it
	if err != nil {
		panic(err.Error())
	}

	// defer the close till after the main function has finished
	// executing
	defer db.Close()


	recordset, err  := db.Query("SELECT * from windmills_s.windmills1 limit 10")
	for recordset.Next(){
		var wm Windmill
		err = recordset.Scan(
			&wm.id,
			&wm.uuid,
			&wm.millid,
			&wm.kwatts_s,
			&wm.date,
			&wm.location,
			&wm.active,
			&wm.time,
			&wm.strrecordtype)

		allWm[wm.id] = wm

	}
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}
	for key, wm := range allWm {
		log.WithFields(log.Fields{"key": key,
			"windmill id": wm.id,
			"KW": wm.kwatts_s,
			"uuid": wm.uuid,
			"time": wm.time}).Info()
		//fmt.Printf(  "%d Windmills id  %d KW = %d  uuid = %s  time = %s \n",
		//	key,wm.id,wm.kwatts_s,wm.uuid, wm.time )
	}
}
