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

package DataObjects

import (
	"context"
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	global "pxc_scheduler_handler/internal/Global"
	SQL "pxc_scheduler_handler/internal/Sql/Proxy"
	"strconv"
)

/*
Interfaces
*/

type ProxySQLCluster interface {
	GetProxySQLnodes(myNode *ProxySQLNodeImpl) bool
}
type ProxySQLNode interface {
	getVariables() bool
	GetConnection() bool
	CloseConnection() bool
	GetDataCluster(config global.Configuration) bool
	ProcessChanges() bool

	MoveNodeUpFromOfflineSoft(dataNode DataNodeImpl, hg int, ip string, port int) string
	MoveNodeDownToOfflineSoft(dataNode DataNodeImpl, hg int, ip string, port int) string
	MoveNodeUpFromHGCange(dataNode DataNodeImpl, hg int, ip string, port int) string
	MoveNodeDownToHGCange(dataNode DataNodeImpl, hg int, ip string, port int) string
	InsertRead(dataNode DataNodeImpl, hg int, ip string, port int) string
	InsertWrite(dataNode DataNodeImpl, hg int, ip string, port int) string
	DeleteDataNode(dataNode DataNodeImpl, hg int, ip string, port int) string
	SaveRetry(dataNode DataNodeImpl, hg int, ip string, port int) string
	executeSQLChanges(SQLActionString []string) bool
}

/*
Cluster object and methods
*/
type ProxySQLClusterImpl struct {
	Name     string
	Nodes    map[string]ProxySQLNodeImpl
	Active   bool
	User     string
	Password string
}

// This method in ProxySQL Cluster Object is responsible for getting the list of ACTIVE ProxySQL servers.
// Interestingly ProxySQL has not clue if a ProxySQL nodes ur down. Or at least is not reported in the proxysql_server tables or any stats table
// Given that we check if nodes are reachable opening a connection and closing it
func (cluster ProxySQLClusterImpl) GetProxySQLnodes(myNode *ProxySQLNodeImpl) bool {
	//nodes := make(map[int]*ProxySQLNodeImpl)

	recordset, err := myNode.Connection.Query(SQL.Dml_select_proxy_servers)
	if err != nil {
		log.Error(err.Error())
		//os.Exit(1)
	}

	for recordset.Next() {
		var weight int
		var hostname string
		var port int
		var comment string
		recordset.Scan(&weight, &hostname, &port, &comment)
		var newNode = new(ProxySQLNodeImpl)
		newNode.Ip = hostname
		newNode.Weight = weight
		newNode.Port = port
		newNode.Comment = comment
		newNode.Dns = net.JoinHostPort(newNode.Ip , strconv.Itoa(newNode.Port))
		newNode.User = cluster.User
		newNode.Password = cluster.Password

		// Given Proxysql is NOT removing a non healthy node from proxySQL_cluster I must add a step here to check and eventually remove failing ProxySQL nodes
		if newNode.GetConnection() {
			if newNode.Dns != myNode.Dns {
				newNode.CloseConnection()
			}
			cluster.Nodes[newNode.Dns] = *newNode
		} else {
			log.Error(fmt.Sprintf("ProxySQL Node %s is down or not reachable PLEASE REMOVE IT from the proxysql_servers table OR fix the issue", newNode.Dns))
		}

	}

	return true
}

/*
ProxySQL Node
*/

type ProxySQLNodeImpl struct {
	ActionNodeList  map[string]DataNodeImpl
	Dns             string
	Hostgoups       map[int]Hostgroup
	Ip              string
	MonitorPassword string
	MonitorUser     string
	Password        string
	Port            int
	User            string
	Connection      *sql.DB
	MySQLCluster    *DataClusterImpl
	Variables       map[string]string
	IsInitialized   bool
	Weight          int
	HoldLock        bool
	IsLockExpired   bool
	LastLockTime    int64
	Comment         string
	Config 			*global.Configuration
}

type Hostgroup struct {
	Id    int
	Size  int
	Type  string
	Nodes []DataNodeImpl
}

/*===============================================================
Methods
*/

/*
Init the proxySQL node
*/
func (node *ProxySQLNodeImpl) Init(config *global.Configuration) bool {
	if global.Performance {
		global.SetPerformanceObj("proxysql_init", true, log.InfoLevel)
	}
	node.User = config.Proxysql.User
	node.Password = config.Proxysql.Password
	node.Dns = net.JoinHostPort(config.Proxysql.Host ,strconv.Itoa(config.Proxysql.Port))
	node.Port = config.Proxysql.Port

	//Establish connection to the destination Proxysql node
	if node.GetConnection() {
	} else {
		log.Error("Cannot connect to indicated Proxy.\n")
		log.Info("Host: "+config.Proxysql.Host, " Port: ", config.Proxysql.Port, " User: "+config.Proxysql.User)
		return false
		//os.Exit(1)
	}
	//Retrieve all variables from Proxy
	if !node.getVariables() {
		log.Error("Cannot load variables from Proxy.\n")
		return false
	}

	//initiate the cluster and all the related nodes
	//if !node.GetDataCluster(config) {
	//	log.Error("Cannot load Data cluster from Proxy.\n")
	//	return false
	//}

	//calculate the performance
	if global.Performance {
		global.SetPerformanceObj("proxysql_init", false, log.InfoLevel)
	}

	if node.Connection != nil {
		node.IsInitialized = true
		return true
	} else {
		node.IsInitialized = false
		return false
	}

}

/*
Retrieve ProxySQL variables and store them internally in a map
*/
func (node *ProxySQLNodeImpl) getVariables() bool {
	variables := make(map[string]string)

	recordset, err := node.Connection.Query(SQL.Dml_show_variables)
	if err != nil {
		log.Error(err.Error())
		return false
		//os.Exit(1)
	}

	for recordset.Next() {
		var name string
		var value string
		recordset.Scan(&name, &value)
		variables[name] = value
	}
	node.Variables = variables
	if node.Variables["mysql-monitor_username"] != "" && node.Variables["mysql-monitor_password"] != "" {
		node.MonitorUser = node.Variables["mysql-monitor_username"]
		node.MonitorPassword = node.Variables["mysql-monitor_password"]
	} else {
		log.Error("ProxySQL Monitor user not declared correctly please check variables mysql-monitor_username|mysql-monitor_password")
		return false
		//os.Exit(1)
	}
	return true
}

/*this method is used to assign a connection to a proxySQL node
return true if successful in any other case false

Note ?timeout=1s is HARDCODED on purpose. This is a check that MUST execute in less than a second.
Having a connection taking longer than that is outrageous. Period!
*/
func (node *ProxySQLNodeImpl) GetConnection() bool {
	if global.Performance {
		global.SetPerformanceObj("main_connection", true, log.DebugLevel)
	}
	//dns := node.User + ":" + node.Password + "@tcp(" + node.Dns + ":"+ strconv.Itoa(node.Port) +")/admin" //
	//if log.GetLevel() == log.DebugLevel {log.Debug(dns)}

	db, err := sql.Open("mysql", node.User+":"+node.Password+"@tcp("+net.JoinHostPort(node.Ip, strconv.Itoa(node.Port))+")/main?timeout=1s")

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

func (node *ProxySQLNodeImpl) CloseConnection() bool {
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
Retrieve active cluster
check for pxc_cluster and cluster_id add to the object a DataClusterImpl object.
DataClusterImpl returns already Initialized, which means it returns with all node populated with status
ProxySQLNodeImpl
	|
	|-> DataClusterImpl
			|
			|-> DataObject
					|
				Pxc | GR
*/
func (node *ProxySQLNodeImpl) GetDataCluster(config global.Configuration) bool {
	//Init the data cluster
	dataClusterPxc := new(DataClusterImpl)
	dataClusterPxc.MonitorPassword = node.MonitorPassword
	dataClusterPxc.MonitorUser = node.MonitorUser

	if !dataClusterPxc.init(config, node.Connection) {
		log.Error("Cannot initialize the data cluster id ", config.Pxcluster.ClusterId)
		return false
	}

	node.MySQLCluster = dataClusterPxc
	return true
}

/*
This method is the one applying the changes to the proxy database
*/
func (node *ProxySQLNodeImpl) ProcessChanges() bool {
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
	for _, dataNodePxc := range node.ActionNodeList {
		dataNode := dataNodePxc
		actionCode := dataNode.ActionType
		hg := dataNode.HostgroupId
		ip, port, _ := net.SplitHostPort(dataNode.Dns)
		//ip := dataNode.Dns[0:strings.Index(dataNode.Dns, ":")]
		//port := dataNode.Dns[strings.Index(dataNode.Dns, ":")+1:]
		portI := global.ToInt(port)
		switch actionCode {
		case 0:
			log.Info(fmt.Sprintf("Node %d %s nothing to do", dataNode.HostgroupId, dataNode.Dns)) //"NOTHING_TO_DO"
		case 1000:
			if dataNode.RetryUp >= node.MySQLCluster.RetryUp {
				SQLActionString = append(SQLActionString, node.MoveNodeUpFromOfflineSoft(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.SaveRetry(dataNode, hg, ip, portI))
			} // "MOVE_UP_OFFLINE"
		case 1010:
			if dataNode.RetryUp >= node.MySQLCluster.RetryUp {
				SQLActionString = append(SQLActionString, node.MoveNodeUpFromHGCange(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.SaveRetry(dataNode, hg, ip, portI))
			} // "MOVE_UP_HG_CHANGE"
		case 2010:
			SQLActionString = append(SQLActionString, node.ResetNodeDefaults(dataNode, hg, ip, portI))
			// "RESET_DEFAULTS"
		case 3001:
			if dataNode.RetryDown >= node.MySQLCluster.RetryDown {
				SQLActionString = append(SQLActionString, node.MoveNodeDownToHGCange(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.SaveRetry(dataNode, hg, ip, portI))
			} // "MOVE_DOWN_HG_CHANGE"
		case 3010:
			if dataNode.RetryDown >= node.MySQLCluster.RetryDown {
				SQLActionString = append(SQLActionString, node.MoveNodeDownToOfflineSoft(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.SaveRetry(dataNode, hg, ip, portI))
			} // "MOVE_DOWN_OFFLINE"
		case 3020:
			if dataNode.RetryDown >= node.MySQLCluster.RetryDown {
				SQLActionString = append(SQLActionString, node.MoveNodeDownToOfflineSoft(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.SaveRetry(dataNode, hg, ip, portI))
			} // "MOVE_TO_MAINTENANCE"
		case 3030:
			if dataNode.RetryUp >= node.MySQLCluster.RetryUp {
				SQLActionString = append(SQLActionString, node.MoveNodeUpFromOfflineSoft(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.SaveRetry(dataNode, hg, ip, portI))
			} // "MOVE_OUT_MAINTENANCE"
		case 4010:
			SQLActionString = append(SQLActionString, node.DeleteDataNode(dataNode, node.MySQLCluster.HgReaderId, ip, portI)) // "DELETE before insert to be sure no conflict arise"
			SQLActionString = append(SQLActionString, node.InsertRead(dataNode, hg, ip, portI))                               // "INSERT_READ"
		case 4020:
			SQLActionString = append(SQLActionString, node.DeleteDataNode(dataNode, node.MySQLCluster.HgWriterId, ip, portI)) // "DELETE before insert to be sure no conflict arise"
			SQLActionString = append(SQLActionString, node.InsertWrite(dataNode, hg, ip, portI))                              // "INSERT_WRITE"
		case 5000:
			SQLActionString = append(SQLActionString, node.DeleteDataNode(dataNode, hg, ip, portI)) // "DELETE_NODE"
		case 5001:
			if dataNode.RetryDown >= node.MySQLCluster.RetryUp {
				SQLActionString = append(SQLActionString, node.DeleteDataNode(dataNode, hg, ip, portI))
				//we need to cleanup also the reader in any case
				SQLActionString = append(SQLActionString, node.DeleteDataNode(dataNode, node.MySQLCluster.HgWriterId, ip, portI))
				SQLActionString = append(SQLActionString, node.InsertWrite(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.SaveRetry(dataNode, hg, ip, portI))
			} // "MOVE_SWAP_READER_TO_WRITER"
		case 5101:
			if dataNode.RetryDown >= node.MySQLCluster.RetryDown {
				SQLActionString = append(SQLActionString, node.DeleteDataNode(dataNode, hg, ip, portI))
				//we need to cleanup also the writer in any case
				SQLActionString = append(SQLActionString, node.DeleteDataNode(dataNode, node.MySQLCluster.HgReaderId, ip, portI))
				SQLActionString = append(SQLActionString, node.InsertRead(dataNode, hg, ip, portI))
			} else {
				SQLActionString = append(SQLActionString, node.SaveRetry(dataNode, hg, ip, portI))
			} // "MOVE_SWAP_WRITER_TO_READER"
		case 9999:
			SQLActionString = append(SQLActionString, node.SaveRetry(dataNode, hg, ip, portI)) // "SAVE_RETRY"

		}

	}
	if global.Performance {
		global.SetPerformanceObj("Process changes - ActionMap - (ProxysqlNode)", false, log.DebugLevel)
	}

	if !node.executeSQLChanges(SQLActionString) {
		log.Fatal("Cannot apply changes error in SQL execution in ProxySQL, Exit with error")
		return false
		//os.Exit(1)
	}
	return true
}

//This function update the server with the defaults as for 8000 group
func (node *ProxySQLNodeImpl) ResetNodeDefaults(dataNode DataNodeImpl, hg int, ip string, port int) string {
	myString := fmt.Sprintf(",weight=%d,max_connections=%d,max_replication_lag=%d,max_latency_ms=%d",
		dataNode.Weight, dataNode.MaxConnection, dataNode.MaxReplicationLag, dataNode.MaxReplicationLag)
	myString = fmt.Sprintf(" UPDATE mysql_servers SET status='ONLINE' %s WHERE hostgroup_id=%d AND hostname='%s' AND port=%d", myString, hg, ip, port)
	log.Debug(fmt.Sprintf("Preparing for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString

}

func (node *ProxySQLNodeImpl) MoveNodeUpFromOfflineSoft(dataNode DataNodeImpl, hg int, ip string, port int) string {

	myString := fmt.Sprintf(" UPDATE mysql_servers SET status='ONLINE' WHERE hostgroup_id=%d AND hostname='%s' AND port=%d", hg, ip, port)
	log.Debug(fmt.Sprintf("Preparing for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}
func (node *ProxySQLNodeImpl) MoveNodeDownToOfflineSoft(dataNode DataNodeImpl, hg int, ip string, port int) string {
	myString := fmt.Sprintf(" UPDATE mysql_servers SET status='OFFLINE_SOFT' WHERE hostgroup_id=%d AND hostname='%s' AND port=%d", hg, ip, port)
	log.Debug(fmt.Sprintf("Preparing for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}
func (node *ProxySQLNodeImpl) MoveNodeUpFromHGCange(dataNode DataNodeImpl, hg int, ip string, port int) string {
	myString := fmt.Sprintf(" UPDATE mysql_servers SET hostgroup_id=%d WHERE hostgroup_id=%d AND hostname='%s' AND port=%d", hg-node.Config.Pxcluster.MaintenanceHgRange, hg, ip, port)
	log.Debug(fmt.Sprintf("Preparing for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}
func (node *ProxySQLNodeImpl) MoveNodeDownToHGCange(dataNode DataNodeImpl, hg int, ip string, port int) string {
	myString := fmt.Sprintf(" UPDATE mysql_servers SET hostgroup_id=%d WHERE hostgroup_id=%d AND hostname='%s' AND port=%d", hg+node.Config.Pxcluster.MaintenanceHgRange, hg, ip, port)
	log.Debug(fmt.Sprintf("Preparing for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}

/*
When inserting a node we need to differentiate when is a NEW node coming from the Bakcup HG because in that case we will NOT push it directly to prod
*/
func (node *ProxySQLNodeImpl) InsertRead(dataNode DataNodeImpl, hg int, ip string, port int) string {
	if dataNode.NodeIsNew {
		hg = node.MySQLCluster.HgReaderId + node.Config.Pxcluster.MaintenanceHgRange
	} else {
		hg = node.MySQLCluster.HgReaderId
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
func (node *ProxySQLNodeImpl) InsertWrite(dataNode DataNodeImpl, hg int, ip string, port int) string {
	if dataNode.NodeIsNew {
		hg = node.MySQLCluster.HgWriterId + node.Config.Pxcluster.MaintenanceHgRange
	} else {
		hg = node.MySQLCluster.HgWriterId
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
func (node *ProxySQLNodeImpl) DeleteDataNode(dataNode DataNodeImpl, hg int, ip string, port int) string {

	myString := fmt.Sprintf(" Delete from mysql_servers WHERE hostgroup_id=%d AND hostname='%s' AND port=%d", hg, ip, port)
	log.Debug(fmt.Sprintf("Preparing for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}

/*
This action is used to modify the RETRY options stored in the comment field
It is important to know that after a final action (like move to OFFLINE_SOFT or move to another HG)
the application will try to reset the RETRIES to 0
*/
func (node *ProxySQLNodeImpl) SaveRetry(dataNode DataNodeImpl, hg int, ip string, port int) string {
	retry := fmt.Sprintf("%d_W_%d_R_retry_up=%d;%d_W_%d_R_retry_down=%d;",
		node.MySQLCluster.HgWriterId,
		node.MySQLCluster.HgReaderId,
		dataNode.RetryUp,
		node.MySQLCluster.HgWriterId,
		node.MySQLCluster.HgReaderId,
		dataNode.RetryDown)
	myString := fmt.Sprintf(" UPDATE mysql_servers SET comment='%s %s' WHERE hostgroup_id=%d AND hostname='%s' AND port=%d", dataNode.Comment, retry, hg, ip, port)
	log.Debug(fmt.Sprintf("Adding for node  %s:%d HG:%d SQL: %s", ip, port, hg, myString))
	return myString
}

/*
We are going to apply all the SQL inside a transaction, so either all or nothing
*/
func (node *ProxySQLNodeImpl) executeSQLChanges(SQLActionString []string) bool {
	//if nothing to execute just return true
	if len(SQLActionString) <= 0 {
		return true
	}

	if global.Performance {
		global.SetPerformanceObj("Execute SQL changes - ActionMap - (ProxysqlNode)", true, log.DebugLevel)
	}
	//We will execute all the commands inside a transaction if any error we will roll back all
	ctx := context.Background()
	tx, err := node.Connection.BeginTx(ctx, nil)
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
		_, err = node.Connection.Exec("LOAD mysql servers to RUN ")
		if err != nil {
			log.Fatal("Cannot load new mysql configuration to RUN ")
			return false
		} else {
			_, err = node.Connection.Exec("SAVE mysql servers to DISK ")
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

//============================================
// HostGroup

func (hgw *Hostgroup) init(id int, hgType string, size int) *Hostgroup {
	hg := new(Hostgroup)
	hg.Id = id
	hg.Type = hgType
	hg.Size = size

	return hg
}
