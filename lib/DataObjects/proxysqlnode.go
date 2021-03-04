package dataobjects

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	global "../Global"
	SQL "../Sql/Proxy"
	log "github.com/sirupsen/logrus"
)

/*
ProxySQL Node
*/

func NewProxySQLNode(ip string) ProxySQLNode {
	o := proxySQLNodeImpl{
		ip: ip,
	}
	return &o
}

type ProxySQLNode interface {
	SetActionNodeList(map[string]DataNodePxc) // OK
	Dns() string                              // OK
	Ip() string                               // OK
	Port() int                                // OK
	Connection() *sql.DB                      // OK
	MySQLCluster() *DataCluster               // KH: to be removed
	IsInitialized() bool                      // OK
	SetHoldLock(bool)                         // OK
	SetLockExpired(bool)                      // OK
	LockExpired() bool                        // OK
	SetLastLockTime(int64)                    // OK
	LastLockTime() int64                      // OK
	SetComment(string)                        // OK
	Comment() string                          // OK

	Init(*global.Configuration) bool // OK
	CloseConnection() bool           // KH: why it is public?
	FetchDataCluster(*global.Configuration) bool
	ProcessChanges() bool
}

type proxySQLNodeImpl struct {
	actionNodeList  map[string]DataNodePxc
	dns             string
	hostgoups       map[int]Hostgroup
	ip              string
	monitorPassword string
	monitorUser     string
	password        string
	port            int
	user            string
	connection      *sql.DB
	mySQLCluster    *DataCluster
	variables       map[string]string
	isInitialized   bool
	weight          int
	holdLock        bool
	isLockExpired   bool
	lastLockTime    int64
	comment         string
}

/*===============================================================
Methods
*/
func (node *proxySQLNodeImpl) SetActionNodeList(v map[string]DataNodePxc) {
	node.actionNodeList = v
}

func (node *proxySQLNodeImpl) Dns() string {
	return node.dns
}

func (node *proxySQLNodeImpl) Ip() string {
	return node.ip
}

func (node *proxySQLNodeImpl) Port() int {
	return node.port
}

func (node *proxySQLNodeImpl) Connection() *sql.DB {
	return node.connection
}
func (node *proxySQLNodeImpl) MySQLCluster() *DataCluster {
	return node.mySQLCluster
}

func (node *proxySQLNodeImpl) IsInitialized() bool {
	return node.isInitialized
}

func (node *proxySQLNodeImpl) SetHoldLock(v bool) {
	node.holdLock = v
}

func (node *proxySQLNodeImpl) SetLockExpired(v bool) {
	node.isLockExpired = v
}

func (node *proxySQLNodeImpl) LockExpired() bool {
	return node.isLockExpired
}

func (node *proxySQLNodeImpl) SetLastLockTime(v int64) {
	node.lastLockTime = v
}
func (node *proxySQLNodeImpl) LastLockTime() int64 {
	return node.lastLockTime
}
func (node *proxySQLNodeImpl) SetComment(v string) {
	node.comment = v
}
func (node *proxySQLNodeImpl) Comment() string {
	return node.comment
}

/*
Init the proxySQL node
*/
func (node *proxySQLNodeImpl) Init(config *global.Configuration) bool {
	if global.Performance {
		global.SetPerformanceObj("proxysql_init", true, log.InfoLevel)
	}
	node.user = config.ProxySQL.User
	node.password = config.ProxySQL.Password
	node.dns = config.ProxySQL.Host + ":" + strconv.Itoa(config.ProxySQL.Port)
	node.port = config.ProxySQL.Port

	//Establish connection to the destination ProxySQL node
	if node.getConnection() {
	} else {
		log.Error("Cannot connect to indicated Proxy.\n")
		log.Info("Host: "+config.ProxySQL.Host, " Port: ", config.ProxySQL.Port, " User: "+config.ProxySQL.User)
		return false
	}
	//Retrieve all variables from Proxy
	if !node.getVariables() {
		log.Error("Cannot load variables from Proxy.\n")
		return false
	}

	//initiate the cluster and all the related nodes
	//if !node.FetchDataCluster(config) {
	//	log.Error("Cannot load Data cluster from Proxy.\n")
	//	return false
	//}

	//calculate the performance
	if global.Performance {
		global.SetPerformanceObj("proxysql_init", false, log.InfoLevel)
	}

	if node.connection != nil {
		node.isInitialized = true
		return true
	} else {
		node.isInitialized = false
		return false
	}

}

/*
Retrieve ProxySQL variables and store them internally in a map
*/
func (node *proxySQLNodeImpl) getVariables() bool {
	variables := make(map[string]string)

	recordset, err := node.connection.Query(SQL.Dml_show_variables)
	if err != nil {
		log.Error(err.Error())
		return false
	}

	for recordset.Next() {
		var name string
		var value string
		recordset.Scan(&name, &value)
		variables[name] = value
	}
	node.variables = variables
	if node.variables["mysql-monitor_username"] != "" && node.variables["mysql-monitor_password"] != "" {
		node.monitorUser = node.variables["mysql-monitor_username"]
		node.monitorPassword = node.variables["mysql-monitor_password"]
	} else {
		log.Error("ProxySQL Monitor user not declared correctly please check variables mysql-monitor_username|mysql-monitor_password")
		return false
	}
	return true
}

/*this method is used to assign a connection to a proxySQL node
return true if successful in any other case false

Note ?timeout=1s is HARDCODED on purpose. This is a check that MUST execute in less than a second.
Having a connection taking longer than that is outrageous. Period!
*/
func (node *proxySQLNodeImpl) getConnection() bool {
	if global.Performance {
		global.SetPerformanceObj("main_connection", true, log.DebugLevel)
	}
	//dns := node.User + ":" + node.Password + "@tcp(" + node.Dns + ":"+ strconv.Itoa(node.Port) +")/admin" //
	//if log.GetLevel() == log.DebugLevel {log.Debug(dns)}

	db, err := sql.Open("mysql", node.user+":"+node.password+"@tcp("+node.dns+")/main?timeout=1s")

	//defer db.Close()
	node.connection = db
	// if there is an error opening the connection, handle it
	if err != nil {
		return false
	}

	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		log.Error(err)
		return false
	}

	if global.Performance {
		global.SetPerformanceObj("main_connection", false, log.DebugLevel)
	}
	return true
}

/*this method is call to close the connection to a proxysql node
return true if successful in any other case false
*/

func (node *proxySQLNodeImpl) CloseConnection() bool {
	if node.connection != nil {
		err := node.connection.Close()
		if err != nil {
			panic(err.Error())
		}
		return true
	}
	return false
}

/*
Populate proxy node
*/

/*
FetchDataCluster retrieves active cluster.
If the method finishes with success, data cluster view is available via
MySQLCluster member of the object.
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
func (node *proxySQLNodeImpl) FetchDataCluster(config *global.Configuration) bool {
	// Init the data cluster
	dataClusterPxc := new(DataCluster)
	dataClusterPxc.MonitorPassword = node.monitorPassword
	dataClusterPxc.MonitorUser = node.monitorUser

	if !dataClusterPxc.init(config, node.connection) {
		log.Error("Cannot initialize the data cluster id ", config.PxcCluster.ClusterID)
		return false
	}

	node.mySQLCluster = dataClusterPxc
	return true
}

/*
This method is the one applying the changes to the proxy database
*/
func (node *proxySQLNodeImpl) ProcessChanges() bool {
	/*
		Actions for each node in the loop (node.ActionNodeList)
		identify the kind of action
		check if is RETRY dependent
		check for retries
		Add SQL statement SQL array.
	*/
	if global.Performance {
		global.SetPerformanceObj("Process changes - ActionMap - (ProxysqlNode)", true, log.DebugLevel)
	}

	var SQLActionString []string

	log.Info("Processing action node list and build SQL commands")
	for _, dataNodePxc := range node.actionNodeList {
		dataNode := dataNodePxc.DataNodeBase
		actionCode := dataNode.ActionType
		hg := dataNode.HostgroupId
		ip := dataNode.Dns[0:strings.Index(dataNode.Dns, ":")]
		port := dataNode.Dns[strings.Index(dataNode.Dns, ":")+1:]
		portI := global.ToInt(port)
		switch actionCode {
		case 0:
			log.Info(fmt.Sprintf("Node %d %s nothing to do", dataNode.HostgroupId, dataNode.Dns)) //"NOTHING_TO_DO"
		case 1000:
			if dataNode.RetryUp >= node.mySQLCluster.RetryUp {
				SQLActionString = append(SQLActionString, node.moveNodeUpFromOfflineSoft(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.saveRetry(dataNode, hg, ip, portI))
			} // "MOVE_UP_OFFLINE"
		case 1010:
			if dataNode.RetryUp >= node.mySQLCluster.RetryUp {
				SQLActionString = append(SQLActionString, node.moveNodeUpFromHGCange(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.saveRetry(dataNode, hg, ip, portI))
			} // "MOVE_UP_HG_CHANGE"
		case 3001:
			if dataNode.RetryDown >= node.mySQLCluster.RetryDown {
				SQLActionString = append(SQLActionString, node.moveNodeDownToHGCange(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.saveRetry(dataNode, hg, ip, portI))
			} // "MOVE_DOWN_HG_CHANGE"
		case 3010:
			if dataNode.RetryDown >= node.mySQLCluster.RetryDown {
				SQLActionString = append(SQLActionString, node.moveNodeDownToOfflineSoft(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.saveRetry(dataNode, hg, ip, portI))
			} // "MOVE_DOWN_OFFLINE"
		case 3020:
			if dataNode.RetryDown >= node.mySQLCluster.RetryDown {
				SQLActionString = append(SQLActionString, node.moveNodeDownToOfflineSoft(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.saveRetry(dataNode, hg, ip, portI))
			} // "MOVE_TO_MAINTENANCE"
		case 3030:
			if dataNode.RetryUp >= node.mySQLCluster.RetryUp {
				SQLActionString = append(SQLActionString, node.moveNodeUpFromOfflineSoft(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.saveRetry(dataNode, hg, ip, portI))
			} // "MOVE_OUT_MAINTENANCE"
		case 4010:
			SQLActionString = append(SQLActionString, node.insertRead(dataNode, hg, ip, portI)) // "INSERT_READ"
		case 4020:
			SQLActionString = append(SQLActionString, node.insertWrite(dataNode, hg, ip, portI)) // "INSERT_WRITE"
		case 5000:
			SQLActionString = append(SQLActionString, node.deleteDataNode(dataNode, hg, ip, portI)) // "DELETE_NODE"
		case 5001:
			if dataNode.RetryDown >= node.mySQLCluster.RetryUp {
				SQLActionString = append(SQLActionString, node.deleteDataNode(dataNode, hg, ip, portI))
				//we need to cleanup also the reader in any case
				SQLActionString = append(SQLActionString, node.deleteDataNode(dataNode, node.MySQLCluster().HgWriterId, ip, portI))
				SQLActionString = append(SQLActionString, node.insertWrite(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.saveRetry(dataNode, hg, ip, portI))
			} // "MOVE_SWAP_READER_TO_WRITER"
		case 5101:
			if dataNode.RetryDown >= node.mySQLCluster.RetryDown {
				SQLActionString = append(SQLActionString, node.deleteDataNode(dataNode, hg, ip, portI))
				//we need to cleanup also the writer in any case
				SQLActionString = append(SQLActionString, node.deleteDataNode(dataNode, node.MySQLCluster().HgReaderId, ip, portI))
				SQLActionString = append(SQLActionString, node.insertRead(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.saveRetry(dataNode, hg, ip, portI))
			} // "MOVE_SWAP_WRITER_TO_READER"
		case 9999:
			SQLActionString = append(SQLActionString, node.saveRetry(dataNode, hg, ip, portI)) // "SAVE_RETRY"

		}

	}
	if global.Performance {
		global.SetPerformanceObj("Process changes - ActionMap - (ProxysqlNode)", false, log.DebugLevel)
	}

	if !node.executeSQLChanges(SQLActionString) {
		log.Fatal("Cannot apply changes error in SQL execution in ProxySQL, Exit with error")
		return false
	}
	return true
}
func (node *proxySQLNodeImpl) moveNodeUpFromOfflineSoft(dataNode DataNode, hg int, ip string, port int) string {

	myString := fmt.Sprintf(" UPDATE mysql_servers SET status='ONLINE' WHERE hostgroup_id=%d AND hostname='%s' AND port=%d", hg, ip, port)
	log.Debug(fmt.Sprintf("Preparing for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}
func (node *proxySQLNodeImpl) moveNodeDownToOfflineSoft(dataNode DataNode, hg int, ip string, port int) string {
	myString := fmt.Sprintf(" UPDATE mysql_servers SET status='OFFLINE_SOFT' WHERE hostgroup_id=%d AND hostname='%s' AND port=%d", hg, ip, port)
	log.Debug(fmt.Sprintf("Preparing for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}
func (node *proxySQLNodeImpl) moveNodeUpFromHGCange(dataNode DataNode, hg int, ip string, port int) string {
	myString := fmt.Sprintf(" UPDATE mysql_servers SET hostgroup_id=%d WHERE hostgroup_id=%d AND hostname='%s' AND port=%d", hg-9000, hg, ip, port)
	log.Debug(fmt.Sprintf("Preparing for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}
func (node *proxySQLNodeImpl) moveNodeDownToHGCange(dataNode DataNode, hg int, ip string, port int) string {
	myString := fmt.Sprintf(" UPDATE mysql_servers SET hostgroup_id=%d WHERE hostgroup_id=%d AND hostname='%s' AND port=%d", hg+9000, hg, ip, port)
	log.Debug(fmt.Sprintf("Preparing for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}

/*
When inserting a node we need to differentiate when is a NEW node coming from the Bakcup HG because in that case we will NOT push it directly to prod
*/
func (node *proxySQLNodeImpl) insertRead(dataNode DataNode, hg int, ip string, port int) string {
	if dataNode.NodeIsNew {
		hg = node.mySQLCluster.HgReaderId + 9000
	} else {
		hg = node.mySQLCluster.HgReaderId
	}
	myString := fmt.Sprintf("INSERT INTO mysql_servers (hostgroup_id, hostname,port,gtid_port,status,weight,compression,max_connections,max_replication_lag,use_ssl,max_latency_ms,comment) "+
		" VALUES(%d,'%s',%d,%d,'%s',%d,%d,%d,%d,%d,%d,'%s')",
		hg,
		ip,
		port,
		dataNode.GtidPort,
		dataNode.ProxyStatus,
		dataNode.Weight,
		dataNode.Compression,
		dataNode.MaxConnection,
		dataNode.MaxReplicationLag,
		global.Bool2int(dataNode.UseSsl),
		dataNode.MaxLatency,
		dataNode.Comment)
	log.Debug(fmt.Sprintf("Preparing for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}

/*
When inserting a node we need to differentiate when is a NEW node coming from the Bakcup HG because in that case we will NOT push it directly to prod
*/
func (node *proxySQLNodeImpl) insertWrite(dataNode DataNode, hg int, ip string, port int) string {
	if dataNode.NodeIsNew {
		hg = node.mySQLCluster.HgWriterId + 9000
	} else {
		hg = node.mySQLCluster.HgWriterId
	}

	myString := fmt.Sprintf("INSERT INTO mysql_servers (hostgroup_id, hostname,port,gtid_port,status,weight,compression,max_connections,max_replication_lag,use_ssl,max_latency_ms,comment) "+
		" VALUES(%d,'%s',%d,%d,'%s',%d,%d,%d,%d,%d,%d,'%s')",
		hg,
		ip,
		port,
		dataNode.GtidPort,
		dataNode.ProxyStatus,
		dataNode.Weight,
		dataNode.Compression,
		dataNode.MaxConnection,
		dataNode.MaxReplicationLag,
		global.Bool2int(dataNode.UseSsl),
		dataNode.MaxLatency,
		dataNode.Comment)
	log.Debug(fmt.Sprintf("Preparing for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}

/*
Delete the given node
*/
func (node *proxySQLNodeImpl) deleteDataNode(dataNode DataNode, hg int, ip string, port int) string {

	myString := fmt.Sprintf(" Delete from mysql_servers WHERE hostgroup_id=%d AND hostname='%s' AND port=%d", hg, ip, port)
	log.Debug(fmt.Sprintf("Preparing for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}

/*
This action is used to modify the RETRY options stored in the comment field
It is important to know that after a final action (like move to OFFLINE_SOFT or move to another HG)
the application will try to reset the RETRIES to 0
*/
func (node *proxySQLNodeImpl) saveRetry(dataNode DataNode, hg int, ip string, port int) string {
	retry := fmt.Sprintf("%d_W_%d_R_retry_up=%d;%d_W_%d_R_retry_down=%d;",
		node.mySQLCluster.HgWriterId,
		node.mySQLCluster.HgReaderId,
		dataNode.RetryUp,
		node.mySQLCluster.HgWriterId,
		node.mySQLCluster.HgReaderId,
		dataNode.RetryDown)
	myString := fmt.Sprintf(" UPDATE mysql_servers SET comment='%s%s' WHERE hostgroup_id=%d AND hostname='%s' AND port=%d", dataNode.Comment, retry, hg, ip, port)
	log.Debug(fmt.Sprintf("Adding for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}

/*
We are going to apply all the SQL inside a transaction, so either all or nothing
*/
func (node *proxySQLNodeImpl) executeSQLChanges(SQLActionString []string) bool {
	//if nothing to execute just return true
	if len(SQLActionString) <= 0 {
		return true
	}

	if global.Performance {
		global.SetPerformanceObj("Execute SQL changes - ActionMap - (ProxysqlNode)", true, log.DebugLevel)
	}
	//We will execute all the commands inside a transaction if any error we will roll back all
	ctx := context.Background()
	tx, err := node.connection.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal("Error in creating transaction to push changes ", err)
	}
	for i := 0; i < len(SQLActionString); i++ {
		if SQLActionString[i] != "" {
			_, err = tx.ExecContext(ctx, SQLActionString[i])
			if err != nil {
				tx.Rollback()
				log.Fatal("Error executing SQL: ", SQLActionString[i], " Rollback and exit")
				log.Error(err)
				return false
			}
		}
	}
	err = tx.Commit()
	if err != nil {
		log.Fatal("Error IN COMMIT exit")
		return false

	} else {
		_, err = node.connection.Exec("LOAD mysql servers to RUN ")
		if err != nil {
			log.Fatal("Cannot load new mysql configuration to RUN ")
			return false
		} else {
			_, err = node.connection.Exec("SAVE mysql servers to DISK ")
			if err != nil {
				log.Fatal("Cannot save new mysql configuration to DISK ")
				return false
			}
		}

	}
	if global.Performance {
		global.SetPerformanceObj("Execute SQL changes - ActionMap - (ProxysqlNode)", false, log.DebugLevel)
	}

	return true
}
