package DataObjects

import (
	Global "../Global"
	SQL "../Sql/Proxy"
	"bytes"
	"database/sql"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
)

/*
Init the proxySQL node
 */
func (node *ProxySQLNode) Init(config Global.Configuration) bool{
	if Global.Performance {
		Global.SetPerformanceValue("proxysql_init",true)
	}
	node.User= config.Proxysql.User
	node.Password = config.Proxysql.Password
	node.Dns = config.Proxysql.Host
	node.Port = config.Proxysql.Port
	if node.GetConnection() {
		// I have moved out this to config file no need to add a DB connection also if local
		//node.CheckTables(config.Proxysql.Initialize)
	}else{
		log.Error("Cannot connect to indicated Proxy.\n")
		log.Info( "Host: " + config.Proxysql.Host," Port: ", config.Proxysql.Port," User: " +config.Proxysql.User )
		os.Exit(1)
	}
	if ! node.getVariables(){
		log.Error("Cannot load variables from Proxy.\n")
		return false
	}

	if ! node.GetDataCluster(config){
		log.Error("Cannot load Data cluster from Proxy.\n")
		return false
	}


	if Global.Performance {
		Global.SetPerformanceValue("proxysql_init",false)
	}

	if node.Connection != nil {
		return true
	} else{
		return false
	}

}

func (node *ProxySQLNode) getVariables() bool{
	variables := make(map[string]string)

	recordset, err  := node.Connection.Query(SQL.Dml_show_variables)
	if err != nil{
		log.Error(err.Error())
		os.Exit(1)
	}

	for recordset.Next() {
		var name string
		var value string
		recordset.Scan(&name,&value)
		variables[name] = value
	}
	node.Variables = variables
	if node.Variables["mysql-monitor_username"] != "" && node.Variables["mysql-monitor_password"] != ""{
		node.MonitorUser = node.Variables["mysql-monitor_username"]
		node.MonitorPassword =  node.Variables["mysql-monitor_password"]
	}else{
		log.Error("ProxySQL Monitor user not declared correctly please check variables mysql-monitor_username|mysql-monitor_password")
		os.Exit(1)
	}
	return true
}

/*this method is used to assign a connection to a proxySQL node
return true if successful in any other case false

Note ?timeout=1s is HARDCODED on purpose. This is a check that MUST execute in less than a second.
Having a connection taking longer than that is outrageous. Period!
*/
func (node *ProxySQLNode) GetConnection() bool{
	if Global.Performance {
		Global.SetPerformanceValue("main_connection",true)
	}
	//dns := node.User + ":" + node.Password + "@tcp(" + node.Dns + ":"+ strconv.Itoa(node.Port) +")/admin" //
	//if log.GetLevel() == log.DebugLevel {log.Debug(dns)}

	db, err := sql.Open("mysql", node.User + ":" + node.Password + "@tcp(" + node.Dns + ":"+ strconv.Itoa(node.Port) +")/main?timeout=1s")

	//defer db.Close()
	node.Connection = db
	// if there is an error opening the connection, handle it
	if err != nil {
		err.Error()
		return false
	}

	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		err.Error()
		return false
	}

	if Global.Performance {
		Global.SetPerformanceValue("main_connection",false)
	}
	return true
}
/*this method is call to close the connection to a proxysql node
return true if successful in any other case false
*/

func (node *ProxySQLNode) CloseConnection() bool{
	if node.Connection != nil {
		err := node.Connection.Close()
		if err != nil {
			panic(err.Error())
			return false
		}
		return true
	}
	return false
}

/*
Populate proxy node
 */


/*
check if tables for running exists
*/
func (node *ProxySQLNode) CheckTables(initTables bool) bool {
	var tableNameDb string
	var executionCheck bool
	tableNames := strings.Split(PxcTables,",")
	executionCheck = false
	recordset, err  := node.Connection.Query(SQL.Info_Check_table)

	if err != nil{
		log.Error(err.Error())
		os.Exit(1)
	}

	if recordset != nil {
		var sb bytes.Buffer

		for recordset.Next() {
			recordset.Scan(&tableNameDb)
			if sb.Len() > 0 {
				sb.WriteString("," + tableNameDb)
			} else {
				sb.WriteString(tableNameDb)
			}
		}
		tables := sb.String()
		for _, tableName := range tableNames {
			if strings.Index(tables, tableName) > -1 {
				log.Info("Table ", tableName, " is already there ")
				executionCheck = true
			} else if initTables {
				node.executeProxyCommand(node.Connection, strings.ReplaceAll(SQL.Ddl_drop_table_generic,"#table-name#",tableName))
				executionCheck = chooseCreateTable(node,tableName)
			}
		}
		//if initTables {break}

		if initTables && !executionCheck{
			for _, tableName := range tableNames {
				executionCheck = chooseCreateTable(node,tableName)
			}
		}
    } else {return false}


	return executionCheck
}
/*
In case of first run OR cleanup tables must be created
 */
func chooseCreateTable(node *ProxySQLNode, tableName string) bool{
	switch table := strings.ToLower(tableName); table {
	case "pxc_servers_original":
		return node.executeProxyCommand(node.Connection, SQL.Ddl_Create_mysal_server_original)
	case "pxc_servers_scheduler":
		return node.executeProxyCommand(node.Connection, SQL.Ddl_create_mysql_servers_scheduler)
	case "pxc_clusters":
		return node.executeProxyCommand(node.Connection, SQL.Ddl_create_pxc_clusters)
	default:
		break
	}
	return false
}

/*
Retrieve active cluster
check for pxc_cluster and cluster_id add to the object a DataCluster object.
DataCluster returns already Initialized, which means it returns with all node populated with status
ProxySQLNode
	|
	|-> DataCluster
			|
			|-> DataObject
					|
				Pxc | GR
 */
func (node *ProxySQLNode) GetDataCluster(config Global.Configuration) bool{
	//Init the data cluster
	dataClusterPxc := new(DataCluster)
	dataClusterPxc.MonitorPassword = node.MonitorPassword
	dataClusterPxc.MonitorUser = node.MonitorUser

	if ! dataClusterPxc.init(config, node.Connection){
		log.Error("Cannot initialize the data cluster id ", config.Pxcluster.ClusterId)
		return false
	}

	node.MySQLCluster = dataClusterPxc
	return true
}


//============================================
// HostGroup

func (hgw *Hostgroup) init(id int, hgType string, size int) *Hostgroup{
	hg := new(Hostgroup)
	hg.Id = id
	hg.Type = hgType
	hg.Size = size

	return hg
}