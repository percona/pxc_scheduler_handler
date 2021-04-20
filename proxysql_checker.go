package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	DO "pxc_scheduler_handler/internal/DataObjects"
	global "pxc_scheduler_handler/internal/Global"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

/*
#######################################
#
# ProxySQL galera check v2
#
# Author Marco Tusa
# Copyright (C) (2016 - 2021)
#
#
# THIS PROGRAM IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, version 3;
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 59 Temple
# Place, Suite 330, Boston, MA  02111-1307  USA.
#######################################
*/
/*
Main function must contains only initial parameter, log system init and main object init
*/
func main() {
	//global setup of basic parameters
	const (
		Separator = string(os.PathSeparator)
	)
	var daemonLoopWait = 0
	var daemonLoop = 0
	//var lockId string //LockId is compose by clusterID_HG_W_HG_R

	var configFile string
	var configPath string
	locker := new(DO.Locker)

	//initialize help
	help := new(global.HelpText)
	help.Init()

	// By default performance collection is disabled
	global.Performance = false

	//Manage config and parameters from conf file [start]
	flag.StringVar(&configFile, "configfile", "", "Config file name for the script")
	flag.StringVar(&configPath, "configpath", "", "Config file path")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\n%s\n", help.GetHelpText())
	}
	flag.Parse()

	//check for current params
	if len(os.Args) < 2 || configFile == "" {
		fmt.Println("You must at least pass the --configfile=xxx parameter ")
		os.Exit(1)
	}

	var currPath, err = os.Getwd()

	if configPath != "" {
		if configPath[len(configPath)-1:] == Separator {
			currPath = configPath
		} else {
			currPath = configPath + Separator
		}
	} else {
		currPath = currPath + Separator + "config" + Separator
	}

	if err != nil {
		fmt.Print("Problem loading the config")
		os.Exit(1)
	}

	for i := 0; i <= daemonLoop; {
		//Return our full configuration from file
		var config = global.GetConfig(currPath + configFile)

		//Let us do a sanity check on the configuration to prevent most obvious issues
		if !config.SanityCheck() {
			os.Exit(1)
		}

		//initialize the log system
		if !global.InitLog(config) {
			fmt.Println("Not able to initialize log system exiting")
			os.Exit(1)
		}
		//Initialize the locker
		if !locker.Init(&config) {
			log.Error("Cannot initialize Locker")
			os.Exit(1)
		}

		//In case we have development mode active then loop here
		if config.Global.Daemonize {
			daemonLoop = 2
			daemonLoopWait = config.Global.DaemonInterval
		}
		//set the locker on file
		if !locker.SetLockFile() {
			fmt.Println("Cannot create a lock, exit")
			os.Exit(1)
		}

		//should we track performance or not
		global.Performance = config.Global.Performance

		/*
			main game start here defining the Proxy Objects
		*/
		//initialize performance collection if requested
		if global.Performance {
			global.PerformanceMapOrdered = global.NewOrderedMap()
			global.PerformanceMap = global.NewRegularIntMap()
			global.SetPerformanceObj("main", true, log.ErrorLevel)
		}
		// create the two main containers the ProxySQL cluster and at least ONE ProxySQL node
		proxysqlNode := locker.MyServer

		/*
			TODO the check against a cluster require a PRE-phase to align the nodes and an AFTER to be sure nodes settings are distributed.
			Not yet implemented
		*/
		if config.Proxysql.Clustered {

			if locker.CheckClusterLock() != nil {
				//our node has the lock
				if !initProxySQLNode(proxysqlNode, config) {
					locker.RemoveLockFile()
					os.Exit(1)
				}
			} else {
				//	Another node has the lock we must exit
				locker.RemoveLockFile()
				os.Exit(1)
			}
		} else {
			if !initProxySQLNode(proxysqlNode, config) {
				locker.RemoveLockFile()
				os.Exit(1)
			}
		}

		if proxysqlNode.GetDataCluster(config) {
			log.Debug("PXC cluster data nodes initialized ")
		} else {
			log.Error("Initialization failed")
			locker.RemoveLockFile()
			os.Exit(1)
		}

		/*
			If we are here all the init phase is over and nodes should be containing the current status.
			Is it now time to evaluate what needs to be done.
			Priority is given to ANY service interruption as if Writer does not exists
			We will have 3 moments:
				- identify any anomalies (evaluateAllNodes)
				- check for the writers and failover / failback (evaluateWriters)
					- Fail-over will take action immediately, evaluating which is the best candidate.
				- check for readers and WriterIsAlso reader option
		*/

		/*
			Analyse the nodes and identify the list of nodes that we must take action on
			The action Map contains all the nodes that require modification with the proper Action ID set
		*/
		proxysqlNode.ActionNodeList = proxysqlNode.MySQLCluster.GetActionList()

		// Once we have the Map we translate it into SQL commands to process
		if !proxysqlNode.ProcessChanges() {
			locker.RemoveLockFile()
			os.Exit(1)
		}

		/*
			Final cleanup
		*/
		if proxysqlNode != nil {
			if proxysqlNode.CloseConnection() {
				if log.GetLevel() == log.DebugLevel {
					log.Info("Connection close")
				}
			}
		}

		if global.Performance {
			global.SetPerformanceObj("main", false, log.ErrorLevel)
			global.ReportPerformance()
		}

		//remove lock and wait
		locker.RemoveLockFile()

		if config.Global.Daemonize {
			time.Sleep(time.Duration(daemonLoopWait) * time.Millisecond)
		} else {
			i++
		}
		log.Info("")

	}
	//if !config.global.Development {
	//	locker.RemoveLockFile()
	//}

}

func initProxySQLNode(proxysqlNode *DO.ProxySQLNode, config global.Configuration) bool {
	//ProxySQL Node work start here
	if proxysqlNode.Init(&config) {
		if log.GetLevel() == log.DebugLevel {
			log.Debug("ProxySQL node initialized ")
		}
		return true
	} else {
		log.Error("Initialization failed")
		return false
	}
}
