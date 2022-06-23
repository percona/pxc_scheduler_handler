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
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	global "pxc_scheduler_handler/internal/Global"
	SQLPxc "pxc_scheduler_handler/internal/Sql/Pcx"
	SQLProxy "pxc_scheduler_handler/internal/Sql/Proxy"
)

type DataCluster interface {
	init(config global.Configuration, connectionProxy *sql.DB) bool
	getNodesInfo() bool
	loadNodes(connectionProxy *sql.DB) bool
	getParametersFromProxySQL(config global.Configuration) bool
	consolidateNodes() bool
	consolidateHGs() bool
	calculateHgOnlineSize(myMap map[string]DataNodeImpl) int
	alignNodeValues(destination DataNodeImpl, source DataNodeImpl) DataNodeImpl
	GetActionList() map[string]DataNodeImpl
	cleanUpForLeftOver() bool
	checkFailoverIfFound() bool
	alignBackupNode(node DataNodeImpl)
	checkMissingForNodes(evalMap map[string]DataNodeImpl) map[string]DataNodeImpl
	evaluateAllProcessedNodes() bool
	evaluateNode(node DataNodeImpl) DataNodeImpl
	checkBackOffline(node DataNodeImpl, currentHg Hostgroup) (DataNodeImpl, bool)
	checkUpSaveRetry(node DataNodeImpl, currentHg Hostgroup) bool
	checkBackNew(node DataNodeImpl) bool
	checkBackPrimary(node DataNodeImpl, currentHg Hostgroup) bool
	checkBackDesyncButUnderReplicaLag(node DataNodeImpl, currentHg Hostgroup) bool
	checkReadOnly(node DataNodeImpl, currentHg Hostgroup) bool
	checkPxcMaint(node DataNodeImpl, currentHg Hostgroup) bool
	checkDonorReject(node DataNodeImpl, currentHg Hostgroup) bool
	checkRejectQueries(node DataNodeImpl, currentHg Hostgroup) bool
	checkNotPrimary(node DataNodeImpl, currentHg Hostgroup) bool
	checkAnyNotReadyStatus(node DataNodeImpl, currentHg Hostgroup) bool
	checkWsrepDesync(node DataNodeImpl, currentHg Hostgroup) bool
	forceRespectManualOfflineSoft(key string, node DataNodeImpl)
	cleanWriters() bool
	evaluateWriters() bool
	processFailoverFailBack(backupWriters map[string]DataNodeImpl)
	processUpActionMap()
	processDownActionMap()
	evaluateReaders() bool
	processWriterIsAlsoReader(readerNodes map[string]DataNodeImpl)
	processUpAndDownReaders(actionNodes map[string]DataNodeImpl, readerNodes map[string]DataNodeImpl)
	pushNewNode(node DataNodeImpl) bool
	identifyPrimaryBackupNode(dns string) bool
	identifyLowerNodeToRemove(node DataNodeImpl) bool
	identifyLowerNodeToRemoveBecauseFailback(node DataNodeImpl) bool
	copyPrimarySettingsToFailover()
	resetNodeDefault(nodeIn DataNodeImpl, nodeDefault DataNodeImpl)
}

type DataNode interface {
	GetConnection() bool
	CloseConnection() bool
	getNodeInternalInformation(dml string) map[string]string
	getNodeInformations(what string) map[string]string
	getRetry(writeHG int, readHG int)
	ReturnTextFromCode(code int) string
	ReturnActionCategory(code int) string

	NOTHING_TO_DO() int
	MOVE_UP_OFFLINE() int
	MOVE_UP_HG_CHANGE() int
	MOVE_DOWN_HG_CHANGE() int
	MOVE_DOWN_OFFLINE() int
	MOVE_TO_MAINTENANCE() int
	MOVE_OUT_MAINTENANCE() int
	INSERT_READ() int
	INSERT_WRITE() int
	DELETE_NODE() int
	MOVE_SWAP_READER_TO_WRITER() int
	MOVE_SWAP_WRITER_TO_READER() int
	SAVE_RETRY() int
	RESET_DEFAULTS() int

	getPxcView(dml string) PxcClusterView
	getInfo(wg *global.MyWaitGroup, cluster *DataClusterImpl) int
	setParameters()
}

type DataNodeImpl struct {
	ActionType        int
	NodeIsNew         bool
	RetryUp           int
	RetryDown         int
	Comment           string
	Compression       int
	Connection        *sql.DB
	ConnUsed          int
	Debug             bool
	Dns               string
	GtidPort          int
	HostgroupId       int
	Hostgroups        []Hostgroup
	Ip                string
	MaxConnection     int
	MaxLatency        int
	MaxReplicationLag int
	Name              string
	NodeTCPDown       bool
	Password          string
	Port              int
	Processed         bool
	ProcessStatus     int
	ProxyStatus       string
	ReadOnly          bool
	Ssl               *SslCertificates
	Status            map[string]string
	UseSsl            bool
	User              string
	Variables         map[string]string
	Weight            int

	//pxc
	PxcMaintMode            string
	WsrepConnected          bool
	WsrepDesinccount        int
	WsrepDonorrejectqueries bool
	WsrepGcommUuid          string
	WsrepLocalIndex         int
	WsrepPcWeight           int
	WsrepProvider           map[string]string
	WsrepReady              bool
	WsrepRejectqueries      bool
	WsrepLocalRecvQueue		int
	WsrepSegment            int
	WsrepStatus             int
	WsrepClusterSize        int
	WsrepClusterName        string
	WsrepClusterStatus      string
	WsrepNodeName           string
	HasPrimaryState         bool
	PxcView                 PxcClusterView
}

type DataClusterImpl struct {
	ActiveFailover    int
	FailBack          bool
	ActionNodes       map[string]DataNodeImpl
	BackupReaders     map[string]DataNodeImpl
	BackupWriters     map[string]DataNodeImpl
	BackupHgReaderId  int
	BakcupHgWriterId  int
	ConfigHgRange     int
	CheckTimeout      int
	ClusterIdentifier int //cluster_id
	ClusterSize       int
	HasPrimary        bool
	ClusterName       string
	Comment           string
	config            global.Configuration
	Debug             bool
	FailOverNode      DataNodeImpl
	HasFailoverNode   bool
	Haswriter         bool
	HgReaderId        int
	HgWriterId        int
	Hostgroups        map[int]Hostgroup
	//	Hosts map[string] DataNodeImpl
	MainSegment            int
	MonitorPassword        string
	MonitorUser            string
	Name                   string
	NodesPxc               *SyncMap //[string] DataNodeImpl // <ip:port,datanode>
	NodesPxcMaint          []DataNodeImpl
	MaxNumWriters          int
	MaintenanceHgRange     int
	OffLineReaders         map[string]DataNodeImpl
	OffLineWriters         map[string]DataNodeImpl
	OffLineHgReaderID      int
	OffLineHgWriterId      int
	ReaderNodes            map[string]DataNodeImpl
	RequireFailover        bool
	RetryDown              int
	RetryUp                int
	Singlenode             bool
	SinglePrimary          bool
	Size                   int
	Ssl                    *SslCertificates
	Status                 int
	WriterIsReader         int
	WriterNodes            map[string]DataNodeImpl
	PersistPrimarySettings int
	PersistPrimary         [2]DataNodeImpl
}

type SyncMap struct {
	sync.RWMutex
	internal map[string]DataNodeImpl
}

type SslCertificates struct {
	sslClient          string
	sslKey             string
	sslCa              string
	sslCertificatePath string
}

type PxcClusterView struct {
	//'HOST_NAME', 'UUID','STATUS','LOCAL_INDEX','SEGMENT'
	HostName   string
	Uuid       string
	Status     string
	LocalIndex int
	Segment    int
}

type VariableStatus struct {
	VarName  string `db:"VARIABLE_NAME"`
	VarValue string `db:"VARIABLE_VALUE"`
}

/*===============================================================
Methods
*/

/*
Data cluster initialization method
*/
func (cluster *DataClusterImpl) init(config global.Configuration, connectionProxy *sql.DB) bool {

	if global.Performance {
		global.SetPerformanceObj("data_cluster_init", true, log.InfoLevel)
	}
	//set parameters from the config file
	if log.GetLevel() == log.DebugLevel {
		cluster.Debug = true
	}

	cluster.ClusterIdentifier = config.Pxcluster.ClusterId
	cluster.CheckTimeout = config.Pxcluster.CheckTimeOut
	cluster.MainSegment = config.Pxcluster.MainSegment
	cluster.ActiveFailover = config.Pxcluster.ActiveFailover
	cluster.FailBack = config.Pxcluster.FailBack
	cluster.PersistPrimarySettings = config.Pxcluster.PersistPrimarySettings
	//cluster.PersistPrimary = []DataNodeImpl{(DataNodeImpl),new(DataNodeImpl)}
	cluster.config = config

	//Enable SSL support
	if config.Pxcluster.SslClient != "" && config.Pxcluster.SslKey != "" && config.Pxcluster.SslCa != "" {
		if global.Performance {
			global.SetPerformanceObj("ssl_certificates_read", true, log.InfoLevel)
		}
		ssl := new(SslCertificates)
		ssl.sslClient = config.Pxcluster.SslClient
		ssl.sslKey = config.Pxcluster.SslKey
		ssl.sslCa = config.Pxcluster.SslCa
		if config.Pxcluster.SslcertificatePath != "" {
			ssl.sslCertificatePath = config.Pxcluster.SslcertificatePath
		} else {
			path, err := os.Getwd()
			if err != nil {
				log.Error(err.Error())
				return false
				//os.Exit(1)
			}
			ssl.sslCertificatePath = path
		}
		cluster.Ssl = ssl
		if global.Performance {
			global.SetPerformanceObj("ssl_certificates_read", false, log.InfoLevel)
		}
	}

	//Get parameters from the config file
	if !cluster.getParametersFromProxySQL(config) {
		log.Error("Cannot retrieve information from Config about the required cluster ", cluster.ClusterIdentifier)
		return false
	}

	// Load nodes from proxysql mysql_server table and populate the different HGs
	if !cluster.loadNodes(connectionProxy) {
		log.Error("Cannot retrieve information from mysql_servers for cluster ", cluster.ClusterIdentifier)

		return false
	}
	// Retrieve the latest status from the node in multi-threading
	if !cluster.getNodesInfo() {
		log.Error("Cannot retrieve information from MySQL nodes ", cluster.ClusterIdentifier)
		return false
	} else {
		// We retrieve the information of the nodes by DNS, but each DNS is shared cross several HGs depending the role and the status
		// The consolidation is needed to align the all HGs with the retrieved status from the nodes
		if !cluster.consolidateNodes() {
			log.Error("Node Consolidation failed ", cluster.ClusterIdentifier)
			return false
		}

		// Consolidate HGs is required to calculate the real status BY HG of the size of the group and other info
		if !cluster.consolidateHGs() {
			log.Fatal("Cannot load Hostgroups in cluster object. Exiting")
			return false
			//os.Exit(1)
		}
	}

	if global.Performance {
		global.SetPerformanceObj("data_cluster_init", false, log.InfoLevel)
	}
	return true
}

//this method is used to parallelize the information retrieval from the datanodes.
// We will use the Nodes list with all the IP:Port pair no matter what HG to check the nodes and then will assign the information to the relevant node collection
// like Bkup(r/w) or Readers/Writers
func (cluster *DataClusterImpl) getNodesInfo() bool {
	if global.Performance {
		global.SetPerformanceObj("Get_Nodes_Info", true, log.InfoLevel)
	}
	var waitingGroup global.MyWaitGroup

	//Before getting the information, we check if any node in the ConfigHgRange is gone lost and if so we try to add it back
	cluster.NodesPxc.internal = cluster.checkMissingForNodes(cluster.NodesPxc.internal)

	// Process the check by node
	for key, node := range cluster.NodesPxc.ExposeMap() {
		waitingGroup.IncreaseCounter()
		// Here we go for parallelization but with a timeout as for configuration *** CheckTimeout ***
		go node.getInfo(&waitingGroup, cluster)
		//node.getInformation(&waitingGroup, cluster)

		if log.GetLevel() == log.DebugLevel {
			log.Debug("Retrieving information from node: ", key)
		}
	}

	log.Debug(fmt.Sprintf("waitingGroup composed by : #%d nodes", waitingGroup.ReportCounter()))

	start := time.Now().UnixNano()
	for i := 0; i < cluster.CheckTimeout; i++ {
		time.Sleep(1 * time.Millisecond)

		if waitingGroup.ReportCounter() == 0 {
			break
		}
		//		log.Debug("wait ", i)
	}
	end := time.Now().UnixNano()
	timems := (end - start) / 1000000
	log.Debug("time taken : ", timems, " ms; checkTimeOut : ", cluster.CheckTimeout, " ms")
	if int(timems) > cluster.CheckTimeout {
		log.Warning("CheckTimeout exceeded try to increase it above the execution time : ", timems)
		if waitingGroup.ReportCounter() > 0 {
			log.Debug("waitingGroup composed by [after loop]: #", waitingGroup.ReportCounter())
		}
		//os.Exit(1)

	}
	if global.Performance {
		global.SetPerformanceObj("Get_Nodes_Info", false, log.InfoLevel)
	}
	return true
}

/*
This functions get the nodes list from the proxysql table mysql_servers for the given HGs and check their conditions
Ony one test for IP:port is executed and status shared across HGs
In debug-dev mode information is retrieved sequentially.
In prod is parallelized

*/
func (cluster *DataClusterImpl) loadNodes(connectionProxy *sql.DB) bool {
	// get list of nodes from ProxySQL
	if global.Performance {
		global.SetPerformanceObj("loadNodes", true, log.InfoLevel)
	}
	backupPrimaryDns := ""
	backupWeightMax := 0
	var sb strings.Builder

	sb.WriteString(strconv.Itoa(cluster.HgWriterId))
	sb.WriteString("," + strconv.Itoa(cluster.HgReaderId))
	sb.WriteString("," + strconv.Itoa(cluster.BakcupHgWriterId))
	sb.WriteString("," + strconv.Itoa(cluster.BackupHgReaderId))
	sb.WriteString("," + strconv.Itoa(cluster.OffLineHgWriterId))
	sb.WriteString("," + strconv.Itoa(cluster.OffLineHgReaderID))

	cluster.ActionNodes = make(map[string]DataNodeImpl)
	cluster.NodesPxc = NewRegularIntMap() //make(map[string]DataNodeImpl)
	cluster.BackupWriters = make(map[string]DataNodeImpl)
	cluster.BackupReaders = make(map[string]DataNodeImpl)
	cluster.WriterNodes = make(map[string]DataNodeImpl)
	cluster.ReaderNodes = make(map[string]DataNodeImpl)
	cluster.OffLineWriters = make(map[string]DataNodeImpl)
	cluster.OffLineReaders = make(map[string]DataNodeImpl)

	sqlCommand := strings.ReplaceAll(SQLProxy.Dml_Select_mysql_nodes, "?", sb.String())
	recordset, err := connectionProxy.Query(sqlCommand)
	sb.Reset()

	if err != nil {
		log.Error(err.Error())
		return false
		//os.Exit(1)
	}

	//select hostgroup_id, hostname,port,gtid_port, status,weight, compression,max_connections, max_replication_lag,use_ssl,max_latency_ms,comment
	for recordset.Next() {
		var myNode DataNodeImpl
		recordset.Scan(&myNode.HostgroupId,
			&myNode.Ip,
			&myNode.Port,
			&myNode.GtidPort,
			&myNode.ProxyStatus,
			&myNode.Weight,
			&myNode.Compression,
			&myNode.MaxConnection,
			&myNode.MaxReplicationLag,
			&myNode.UseSsl,
			&myNode.MaxLatency,
			&myNode.Comment,
			&myNode.ConnUsed)
		myNode.User = cluster.MonitorUser
		myNode.Password = cluster.MonitorPassword
		myNode.Dns =  net.JoinHostPort(myNode.Ip ,strconv.Itoa(myNode.Port))
		if len(myNode.Comment) > 0 {
			myNode.getRetry(cluster.HgWriterId, cluster.HgReaderId)
		}

		//Load ssl object to node if present in cluster/config
		if cluster.Ssl != nil {
			myNode.Ssl = cluster.Ssl
		}

		switch myNode.HostgroupId {
		case cluster.HgWriterId:
			cluster.WriterNodes[myNode.Dns] = myNode
		case cluster.HgReaderId:
			cluster.ReaderNodes[myNode.Dns] = myNode
		case cluster.BakcupHgWriterId:
			cluster.BackupWriters[myNode.Dns] = myNode
			//We calculate in any case which node in the backup writer is the Primary, which for us is the one with higher weight, period!
			if myNode.Weight > backupWeightMax {
				backupWeightMax = myNode.Weight
				backupPrimaryDns = myNode.Dns
			}
		case cluster.BackupHgReaderId:
			cluster.BackupReaders[myNode.Dns] = myNode
		case cluster.OffLineHgWriterId:
			cluster.OffLineWriters[myNode.Dns] = myNode
		case cluster.OffLineHgReaderID:
			cluster.OffLineReaders[myNode.Dns] = myNode
		}

		/*
			We add only the real servers in the list to check with DB access
			we include all the HG operating like Write/Read and relevant OFFLINE special HG
		*/
		if _, ok := cluster.NodesPxc.ExposeMap()[myNode.Dns]; !ok {
			if myNode.HostgroupId == cluster.HgWriterId ||
				myNode.HostgroupId == cluster.HgReaderId ||
				myNode.HostgroupId == cluster.OffLineHgWriterId ||
				myNode.HostgroupId == cluster.OffLineHgReaderID {
				cluster.NodesPxc.Store(myNode.Dns, myNode)
			}
		}

	}

	//if we have cluster.PersistPrimary we identify which is the primary node and add to the array
	if cluster.PersistPrimarySettings > 0 && cluster.SinglePrimary {
		cluster.identifyPrimaryBackupNode(backupPrimaryDns)
	}

	//collect performance
	if global.Performance {
		global.SetPerformanceObj("loadNodes", false, log.InfoLevel)
	}
	return true
}

//We identify and set the Primary node reference for Writer and reader in case of failover
func (cluster *DataClusterImpl) identifyPrimaryBackupNode(dns string) int {

	if cluster.PersistPrimarySettings > 0 {
		cluster.PersistPrimary[0] = cluster.BackupWriters[dns]
		if cluster.PersistPrimarySettings > 1 {
			cluster.PersistPrimary[1] = cluster.BackupReaders[dns]
			return 2
		}
		return 1
	}
	return 0
}

//load values from db disk in ProxySQL
func (cluster *DataClusterImpl) getParametersFromProxySQL(config global.Configuration) bool {
	if global.Performance {
		global.SetPerformanceObj("Get_Parametes_from_ProxySQL", true, log.InfoLevel)
	}
	cluster.ClusterIdentifier = config.Pxcluster.ClusterId
	cluster.HgWriterId = config.Pxcluster.HgW
	cluster.HgReaderId = config.Pxcluster.HgR
	cluster.BakcupHgWriterId = config.Pxcluster.BckHgW
	cluster.BackupHgReaderId = config.Pxcluster.BckHgR
	cluster.SinglePrimary = config.Pxcluster.SinglePrimary
	cluster.MaxNumWriters = config.Pxcluster.MaxNumWriters
	cluster.WriterIsReader = config.Pxcluster.WriterIsAlsoReader
	cluster.RetryUp = config.Pxcluster.RetryUp
	cluster.RetryDown = config.Pxcluster.RetryDown
	cluster.ConfigHgRange = config.Pxcluster.ConfigHgRange
	cluster.MaintenanceHgRange = config.Pxcluster.MaintenanceHgRange
	//)
	if log.GetLevel() == log.DebugLevel {
		log.Debug("Cluster arguments ", " clusterid=", cluster.ClusterIdentifier,
			" hg_w:", cluster.HgWriterId,
			" hg_r:", cluster.HgReaderId,
			" bckhg_w:", cluster.BakcupHgWriterId,
			" bckhg_w:", cluster.BackupHgReaderId,
			" singlePrimary:", cluster.SinglePrimary,
			" num_writers:", cluster.MaxNumWriters,
			" writer_is_also_r:", cluster.WriterIsReader,
			" retry_up:", cluster.RetryUp,
			" retry_down:", cluster.RetryDown,
			" check_timeout:", cluster.CheckTimeout,
			" main_segment:", cluster.MainSegment)
	}
	cluster.OffLineHgReaderID = cluster.MaintenanceHgRange + cluster.HgReaderId
	cluster.OffLineHgWriterId = cluster.MaintenanceHgRange + cluster.HgWriterId
	if global.Performance {
		global.SetPerformanceObj("Get_Parametes_from_ProxySQL", false, log.InfoLevel)
	}
	return true
	//}

}

/*
This method is responsible to be sure that each node list Writer/read/backups/offline are aligned with the status just Identified from the live nodes
Here we align the physical server status with the role of each server.
Given a physical node is present in more then a list we need to evaluate all of them for each physical server
this is not huge as work because the loop will normally have a max of 5 physical nodes for PXC and 9 for GR
*/
func (cluster *DataClusterImpl) consolidateNodes() bool {

	/*
		simple loop on the nodes and overwrite only some values of the ones in the lists
	*/
	for key, node := range cluster.NodesPxc.internal {
		nodeIsIn := false
		if _, ok := cluster.WriterNodes[key]; ok {
			cluster.WriterNodes[key] = cluster.alignNodeValues(cluster.WriterNodes[key], node)
			nodeIsIn = true
		}
		if _, ok := cluster.ReaderNodes[key]; ok {
			cluster.ReaderNodes[key] = cluster.alignNodeValues(cluster.ReaderNodes[key], node)
			nodeIsIn = true
		}
		if _, ok := cluster.BackupWriters[key]; ok {
			cluster.BackupWriters[key] = cluster.alignNodeValues(cluster.BackupWriters[key], node)

		}
		if _, ok := cluster.BackupReaders[key]; ok {
			cluster.BackupReaders[key] = cluster.alignNodeValues(cluster.BackupReaders[key], node)
		}
		if _, ok := cluster.OffLineWriters[key]; ok {
			cluster.OffLineWriters[key] = cluster.alignNodeValues(cluster.OffLineWriters[key], node)
			nodeIsIn = true
		}
		if _, ok := cluster.OffLineReaders[key]; ok {
			cluster.OffLineReaders[key] = cluster.alignNodeValues(cluster.OffLineReaders[key], node)
			nodeIsIn = true
		}

		//align cluster Parameters
		if node.HasPrimaryState {
			cluster.HasPrimary = true
			cluster.Size = node.WsrepClusterSize
			cluster.Name = node.WsrepClusterName

		}
		if !nodeIsIn {
			cluster.ReaderNodes[key] = node
		}
	}

	return true
}

/*
We have only active HostGroups here like the R/W ones and the special Maintenance
*/
func (cluster *DataClusterImpl) consolidateHGs() bool {
	specialWrite := cluster.HgWriterId + cluster.MaintenanceHgRange
	specialRead := cluster.HgReaderId + cluster.MaintenanceHgRange
	hgM := map[int]Hostgroup{
		cluster.HgWriterId: {
			Id:    cluster.HgWriterId,
			Type:  "W",
			Nodes: nil,
			Size:  cluster.calculateHgOnlineSize(cluster.WriterNodes),
		},
		cluster.HgReaderId: {
			Id:    cluster.HgReaderId,
			Type:  "R",
			Nodes: nil,
			Size:  cluster.calculateHgOnlineSize(cluster.ReaderNodes),
		},
		specialWrite: {
			Id:    specialWrite,
			Type:  "WREC",
			Nodes: nil,
			Size:  cluster.calculateHgOnlineSize(cluster.OffLineWriters),
		},
		specialRead: {
			Id:    specialRead,
			Type:  "RREC",
			Nodes: nil,
			Size:  cluster.calculateHgOnlineSize(cluster.OffLineReaders),
		},
	}
	cluster.Hostgroups = hgM

	return true
}
func (cluster *DataClusterImpl) calculateHgOnlineSize(myMap map[string]DataNodeImpl) int {
	var i int
	for _, node := range myMap {
		if node.ProxyStatus == "ONLINE" {
			i++
		}
	}
	return i
}

// We align only the relevant information, not all the node
func (cluster *DataClusterImpl) alignNodeValues(destination DataNodeImpl, source DataNodeImpl) DataNodeImpl {
	destination.ActionType = source.ActionType
	destination.Variables = source.Variables
	destination.Status = source.Status
	destination.PxcMaintMode = source.PxcMaintMode
	destination.PxcView = source.PxcView
	//destination.RetryUp = source.RetryUp
	//destination.RetryDown = source.RetryDown
	destination.Processed = source.Processed
	destination.setParameters()
	return destination
}

/*
This method is where we initiate the analysis of the nodes an the starting point of the population of the actionList
The actionList is the object returning the list of nodes that require modification
Any modification at their status in ProxySQL is done by the ProxySQLNodeImpl object
*/
func (cluster *DataClusterImpl) GetActionList() map[string]DataNodeImpl {
	if global.Performance {
		global.SetPerformanceObj("Get Action Map (DataClusterImpl)", true, log.DebugLevel)
	}
	/*
		NOTES:
			steps:
			1) Identify if Primary
		    2) process nodes and identify theirs status, if need modification I add them to the ActionList
		    3) Check writers and if we need Failover
		    4) Check if we have at least 1 reader, if not we add/force the Writer no matter what


				4 check for readers state
	*/
	//1-2
	// first of all clean any refuse and check for possible left over
	cluster.cleanUpForLeftOver()
	// Here we analyze the status of the nodes
	cluster.evaluateAllProcessedNodes()
	//3
	//we cannot just put up or down a writer. We must check if we need to fail over and if we can add another writer
	cluster.HasPrimary = cluster.evaluateWriters()
	//4
	//check if we have a reader active if no reader we will force the writer to become reader no matter what
	cluster.evaluateReaders()

	//Finally check if we have found of not a node to failover in case of needs
	if !cluster.checkFailoverIfFound() {
		log.Error("Error electing Node for fail-over")
	}
	if global.Performance {
		global.SetPerformanceObj("Get Action Map (DataClusterImpl)", false, log.DebugLevel)
	}

	//At this point we should be able to do actions in consistent way
	return cluster.ActionNodes
}

/*
We need to check if for any reasons we left some node suspended in the maintenance groups
this can happen if script is interrupted or manual action
*/
func (cluster *DataClusterImpl) cleanUpForLeftOver() bool {
	//arrayOfMaps := [2]map[string]DataNodeImpl{cluster.WriterNodes, cluster.ReaderNodes}
	for key, node := range cluster.WriterNodes {
		if _, ok := cluster.OffLineWriters[key]; ok {
			node.HostgroupId = cluster.HgWriterId + cluster.MaintenanceHgRange
			node.ActionType = node.DELETE_NODE()
			cluster.ActionNodes[strconv.Itoa(node.ActionType)+"_"+strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
			delete(cluster.OffLineWriters, key)
		}
	}
	for key, node := range cluster.ReaderNodes {
		if _, ok := cluster.OffLineReaders[key]; ok {
			node.HostgroupId = cluster.HgReaderId + cluster.MaintenanceHgRange
			node.ActionType = node.DELETE_NODE()
			cluster.ActionNodes[strconv.Itoa(node.ActionType)+"_"+strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
			delete(cluster.OffLineReaders, key)
		}
	}

	//if we are working with Preserve Primary value we may need to reset the values in the Maintenance HG
	if cluster.PersistPrimarySettings > 0 {
		for key, node := range cluster.OffLineWriters {
			backupNode := cluster.BackupWriters[key]
			if cluster.compareNodeDefault(node, backupNode) {
				node = cluster.resetNodeDefault(node, backupNode)
				node.ActionType = node.RESET_DEFAULTS()
				cluster.ActionNodes[strconv.Itoa(node.ActionType)+"_"+strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
			}
		}
		for key, node := range cluster.OffLineReaders {
			backupNode := cluster.BackupReaders[key]
			if cluster.compareNodeDefault(node, backupNode) {
				node = cluster.resetNodeDefault(node, backupNode)
				node.ActionType = node.RESET_DEFAULTS()
				cluster.ActionNodes[strconv.Itoa(node.ActionType)+"_"+strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
			}
		}
	}

	return true
}

//just check if we have identify failover node if not notify with HUGE alert
func (cluster *DataClusterImpl) checkFailoverIfFound() bool {
	if cluster.RequireFailover &&
		len(cluster.WriterNodes) < 1 &&
		cluster.FailOverNode.HostgroupId == 0 {
		//TODO try to help the user identify the most common issues like:
		// 1) no node in the main segment
		// 2) node is not processed correctly (security/network)

		//Huge alert
		log.Error(fmt.Sprintf("!!!!!!!!!!!!!!! NO node Found For fail-over in the main segment %d check if ;gmcast.segment; value in wsrep_provider_option matches the mainSegment in the configuration. Also check for possible connection security errors, or if you have READ-ONLY flag=ON!!!!!!!!!!!!!",
			cluster.MainSegment))
		return false
	}

	return true

}

//align backup HGs
func (cluster *DataClusterImpl) alignBackupNode(node DataNodeImpl) {
	if _, ok := cluster.BackupWriters[node.Dns]; ok {
		cluster.BackupWriters[node.Dns] = cluster.alignNodeValues(cluster.BackupWriters[node.Dns], node)
	}
	if _, ok := cluster.BackupReaders[node.Dns]; ok {
		cluster.BackupReaders[node.Dns] = cluster.alignNodeValues(cluster.BackupReaders[node.Dns], node)
	}

}

// We can try to add back missed nodes (from bakcupHG) and se if they are coming back
func (cluster *DataClusterImpl) checkMissingForNodes(evalMap map[string]DataNodeImpl) map[string]DataNodeImpl {

	//also if adding back nodes we will always try to add them back as readers and IF they pass then could become writers
	//we merge the two Maps in one to process all together
	arrayOfMaps := [2]map[string]DataNodeImpl{cluster.BackupReaders, cluster.BackupWriters}

	for i := 0; i < len(arrayOfMaps); i++ {
		for _, node := range arrayOfMaps[i] {
			key1 := node.Dns
			if _, ok := evalMap[key1]; !ok {
				node.HostgroupId = cluster.HgReaderId
				node.NodeIsNew = true
				evalMap[node.Dns] = node
			}
		}
	}
	return evalMap
}

// we will review all the nodes keeping into account the status and hostgroups
func (cluster *DataClusterImpl) evaluateAllProcessedNodes() bool {
	var arrayOfMaps = [4]map[string]DataNodeImpl{cluster.WriterNodes, cluster.ReaderNodes, cluster.OffLineWriters, cluster.OffLineReaders}
	evalMap := MergeMaps(arrayOfMaps)

	if len(evalMap) > 0 {
		for key, node := range evalMap {
			//for key, node := range cluster.NodesPxc.internal {
			log.Debug("Evaluating node ", key)
			//Only nodes that were successfully processed (got status from query) are evaluated
			if node.Processed {
				cluster.evaluateNode(node)

			} else if node.ProxyStatus == "SHUNNED" &&
				node.HostgroupId < cluster.ConfigHgRange {
				//Any Shunned Node is moved to Special HG Maintenance
				if cluster.RetryDown > 0 {
					node.RetryDown++
				}
				node.ActionType = node.MOVE_DOWN_HG_CHANGE()
				cluster.ActionNodes[strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
				//If is time for action and the node is part of the writers I will remove it from here so we can fail-over
				//if  _, ok := cluster.WriterNodes[node.Dns]; ok &&
				//	node.RetryDown >= cluster.RetryDown {
				//	delete(cluster.WriterNodes,node.Dns)
				//}
				log.Warning("Node: ", node.Dns, " ", node.WsrepNodeName, " HG: ", cluster.Hostgroups[node.HostgroupId].Id, " Type ", cluster.Hostgroups[node.HostgroupId].Type, " is im PROXYSQL state ", node.ProxyStatus,
					" moving it to HG ", node.HostgroupId+cluster.MaintenanceHgRange, " given SHUNNED")

			}
			// Align with config HGs
			cluster.alignBackupNode(node)
		}
	}

	return false
}

/* process by nde we will check for several conditions:
- pxc-maint
- wsrep-sync
- primary status
- wsrep reject queries
- donor reject queries
- read only

here we identify first what needs to go off-line after what can brings up.
any node subject to an action is going to be add to the actionMap
*/
func (cluster *DataClusterImpl) evaluateNode(node DataNodeImpl) DataNodeImpl {
	if node.Processed {
		if node.HostgroupId == cluster.HgWriterId ||
			node.HostgroupId == cluster.HgReaderId ||
			node.HostgroupId == cluster.OffLineHgWriterId ||
			node.HostgroupId == cluster.OffLineHgReaderID {

			node.ActionType = node.NOTHING_TO_DO()
			currentHg := cluster.Hostgroups[node.HostgroupId]

			// Check for Demoting actions first
			//---------------------------------------
			//#Check major exclusions
			//# 1) wsrep state
			//# 2) Node is not read only
			//# 3) at least another node in the HG

			//ony node not in config HG or special Maintenance will be processed
			if node.HostgroupId < cluster.MaintenanceHgRange {
				// desync
				if cluster.checkWsrepDesync(node, currentHg) {
					return node
				}

				// Node is in unsafe state we will move to maintenance group Maintenance
				if cluster.checkAnyNotReadyStatus(node, currentHg) {
					return node
				}

				//#3) Node/cluster in non primary
				if cluster.checkNotPrimary(node, currentHg) {
					return node
				}

				//# 4) wsrep_reject_queries=NONE
				if cluster.checkRejectQueries(node, currentHg) {
					return node
				}

				//#5) Donor, node donot reject queries =1 size of cluster > 2 of nodes in the same segments
				if cluster.checkDonorReject(node, currentHg) {
					return node
				}

				//#6) Node had pxc_maint_mode set to anything except DISABLED, not matter what it will go in OFFLINE_SOFT
				if cluster.checkPxcMaint(node, currentHg) {
					return node
				}

				//7 Writer is READ_ONLY
				if cluster.checkReadOnly(node, currentHg) {
					return node
				}
			}

			/*
				COME BACK online *********************[]
			*/
			//#Node comes back from offline_soft when (all of them):
			//# 1) Node state is 4
			//# 3) wsrep_reject_queries = none
			//# 4) Primary state
			//# 5) pxc_maint_mode is DISABLED or undef

			pxc, done := cluster.checkBackOffline(node, currentHg)
			if done {
				return pxc
			}

			//# Node comes back from maintenance HG when (all of them):
			//# 1) node state is 4
			//# 3) wsrep_reject_queries = none
			//# 4) Primary state
			if cluster.checkBackPrimary(node, currentHg) {
				return node
			}

			/*
				When a new node is coming in we do not put it online directly, but instead we will insert it in the special group Maintenance
				this will allow us to process it correctly and validate the state.
				Then we will put online
			*/
			if cluster.checkBackNew(node) {
				return node
			}

			if cluster.checkBackDesyncButUnderReplicaLag(node, currentHg){
				return node;
			}
			//# in the case node is not in one of the declared state
			//# BUT it has the counter retry set THEN I reset it to 0 whatever it was because
			//# I assume it is ok now
			//TODO this MUST be checked I suspect it will not act right
			cluster.checkUpSaveRetry(node, currentHg)

		}
	}
	return node
}

func (cluster *DataClusterImpl) checkBackOffline(node DataNodeImpl, currentHg Hostgroup) (DataNodeImpl, bool) {
	if node.HostgroupId < cluster.ConfigHgRange &&
		node.WsrepStatus == 4 &&
		node.ProxyStatus == "OFFLINE_SOFT" &&
		!node.WsrepRejectqueries &&
		node.WsrepClusterStatus == "Primary" &&
		node.PxcMaintMode == "DISABLED" {
		if node.HostgroupId == cluster.HgWriterId && node.ReadOnly {
			return node, true
		}
		if cluster.RetryUp > 0 {
			node.RetryUp++
		}
		node.ActionType = node.MOVE_UP_OFFLINE()
		cluster.ActionNodes[strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
		log.Info("Node: ", node.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " coming back ONLINE from previous OFFLINE_SOFT ")
		return node, true
	}
	return DataNodeImpl{}, false
}

func (cluster *DataClusterImpl) checkUpSaveRetry(node DataNodeImpl, currentHg Hostgroup) bool {
	if node.ActionType == node.NOTHING_TO_DO() &&
		(node.RetryUp > 0 || node.RetryDown > 0) {
		node.RetryDown = 0
		node.RetryUp = 0
		node.ActionType = node.SAVE_RETRY()
		cluster.ActionNodes[strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
		log.Info("Node: ", node.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " resetting the retry cunters to 0, all seems fine now")
		return true
	}
	return false
}

func (cluster *DataClusterImpl) checkBackNew(node DataNodeImpl) bool {
	if node.NodeIsNew &&
		node.HostgroupId < cluster.MaintenanceHgRange {
		node.HostgroupId = node.HostgroupId + cluster.MaintenanceHgRange
		node.ActionType = node.INSERT_READ()
		cluster.ActionNodes[strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
		log.Info(fmt.Sprintf("I am going to re-insert a declared node in Backup HGs that went missed in ProxySQL mysql_server table "))
		log.Info(fmt.Sprintf("Node %s will be first inserted in special HG %d Then if it status is fine will be promoted", node.Dns, node.HostgroupId))
		return true
	}
	return false
}

func (cluster *DataClusterImpl) checkBackPrimary(node DataNodeImpl, currentHg Hostgroup) bool {
	if node.HostgroupId >= cluster.MaintenanceHgRange &&
		node.WsrepStatus == 4 &&
		!node.WsrepRejectqueries &&
		node.PxcMaintMode == "DISABLED" &&
		node.WsrepClusterStatus == "Primary" {
		if cluster.RetryUp > 0 {
			node.RetryUp++
		}
		node.ActionType = node.MOVE_UP_HG_CHANGE()
		cluster.ActionNodes[strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node

		log.Info("Node: ", node.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " coming back ONLINE from previous Offline Special Host Group ",
			node.HostgroupId)
		return true
	}
	return false
}

func (cluster *DataClusterImpl) checkBackDesyncButUnderReplicaLag(node DataNodeImpl, currentHg Hostgroup) bool {
	if node.HostgroupId < cluster.MaintenanceHgRange &&
		node.WsrepStatus == 2 &&
		!node.WsrepRejectqueries &&
		node.PxcMaintMode == "DISABLED" &&
		node.ProxyStatus == "OFFLINE_SOFT" &&
		node.WsrepClusterStatus == "Primary" {
		if node.MaxReplicationLag > 0 &&
			node.WsrepLocalRecvQueue < (node.MaxReplicationLag/2){
			if cluster.RetryUp > 0 {
				node.RetryUp++
			}
			node.ActionType = node.MOVE_UP_OFFLINE()
			cluster.ActionNodes[strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
			log.Infof("Node: %s %s HG: %d Type: %s  coming back ONLINE from previous Offline Special Host %d given wsrep_local_recv_queue(%d) is less than HALF of the  MaxReplicationLag(%d) ",
				node.Dns,
				node.WsrepNodeName,
				currentHg.Id,
				currentHg.Type,
				node.HostgroupId,
				node.WsrepLocalRecvQueue,
				node.MaxReplicationLag)
			return true
		}
	}

	return false
}

func (cluster *DataClusterImpl) checkReadOnly(node DataNodeImpl, currentHg Hostgroup) bool {
	if node.HostgroupId == cluster.HgWriterId &&
		node.ReadOnly {
		if cluster.RetryDown > 0 {
			node.RetryDown++
		}
		node.ActionType = node.MOVE_SWAP_WRITER_TO_READER()
		cluster.ActionNodes[strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
		log.Warning("Node: ", node.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " has READ_ONLY ",
			"moving it to Reader HG ")
		return true
	}
	return false
}

func (cluster *DataClusterImpl) checkPxcMaint(node DataNodeImpl, currentHg Hostgroup) bool {
	if node.PxcMaintMode != "DISABLED" &&
		node.ProxyStatus != "OFFLINE_SOFT" &&
		node.HostgroupId < cluster.ConfigHgRange {
		node.ActionType = node.MOVE_DOWN_OFFLINE()
		//when we do not increment retry is because we want an immediate action like in this case. So let us set the retry to max.
		node.RetryDown = cluster.RetryDown + 1
		cluster.ActionNodes[strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
		log.Warning("Node: ", node.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " has PXC_maint_mode as ", node.PxcMaintMode,
			" moving it to OFFLINE_SOFT ")
		return true
	}
	return false
}

func (cluster *DataClusterImpl) checkDonorReject(node DataNodeImpl, currentHg Hostgroup) bool {
	if node.WsrepDonorrejectqueries &&
		node.WsrepStatus == 2 &&
		currentHg.Size > 1 &&
		node.HostgroupId < cluster.ConfigHgRange {
		if cluster.RetryDown > 0 {
			node.RetryDown++
		}
		node.ActionType = node.MOVE_DOWN_HG_CHANGE()
		cluster.ActionNodes[strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
		log.Warning("Node: ", node.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " has WSREP Reject queries active ",
			"moving it to HG ", node.HostgroupId+cluster.MaintenanceHgRange)
		return true

	}
	return false
}

func (cluster *DataClusterImpl) checkRejectQueries(node DataNodeImpl, currentHg Hostgroup) bool {
	if node.WsrepRejectqueries &&
		node.HostgroupId < cluster.ConfigHgRange {
		if cluster.RetryDown > 0 {
			node.RetryDown++
		}
		node.ActionType = node.MOVE_DOWN_HG_CHANGE()
		cluster.ActionNodes[strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
		log.Warning("Node: ", node.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " has WSREP Reject queries active ",
			"moving it to HG ", node.HostgroupId+cluster.MaintenanceHgRange)
		return true
	}
	return false
}

func (cluster *DataClusterImpl) checkNotPrimary(node DataNodeImpl, currentHg Hostgroup) bool {
	if node.WsrepClusterStatus != "Primary" {
		if cluster.RetryDown > 0 {
			node.RetryDown++
		}
		node.ActionType = node.MOVE_DOWN_HG_CHANGE()
		cluster.ActionNodes[strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
		log.Warning("Node: ", node.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " is NOT in Primary state ",
			" moving it to HG ", node.HostgroupId+cluster.MaintenanceHgRange, " given unsafe node state")
		return true
	}
	return false
}

func (cluster *DataClusterImpl) checkAnyNotReadyStatus(node DataNodeImpl, currentHg Hostgroup) bool {
	if node.WsrepStatus != 2 &&
		node.WsrepStatus != 4 {
		//if cluster retry > 0 then we manage the node as well
		if cluster.RetryDown > 0 {
			node.RetryDown++
		}
		node.ActionType = node.MOVE_DOWN_HG_CHANGE()
		cluster.ActionNodes[strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
		log.Warning("Node: ", node.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " is in state ", node.WsrepStatus,
			"moving it to HG ", node.HostgroupId+cluster.MaintenanceHgRange, " given unsafe node state")

		return true
	}
	return false
}

func (cluster *DataClusterImpl) checkWsrepDesync(node DataNodeImpl, currentHg Hostgroup) bool {
	var act bool = false
	if node.WsrepStatus == 2 &&
		!node.ReadOnly &&
		node.ProxyStatus != "OFFLINE_SOFT" {
		if currentHg.Size <= 1 {
			log.Warning("Node: ", node.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " is in state ", node.WsrepStatus,
				" But I will not move to OFFLINE_SOFT given last node left in the Host group")
			node.ActionType = node.NOTHING_TO_DO()
			return false
		} else if currentHg.Size > 1 {
			//If we have desync but the MaxReplicationLAag is set then we evaluate it
			if node.MaxReplicationLag != 0 &&
				node.WsrepLocalRecvQueue > node.MaxReplicationLag {
				//if cluster retry > 0 then we manage the node as well
				act = true
			}else if node.MaxReplicationLag != 0 &&
				node.WsrepLocalRecvQueue < node.MaxReplicationLag {
				//if cluster retry > 0 then we manage the node as well
				act = false
			}else  {
				act = true
			}

			if act{
				if cluster.RetryDown > 0 {
					node.RetryDown++
				}
				node.ActionType = node.MOVE_DOWN_OFFLINE()
				cluster.ActionNodes[strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
				log.Warning("Node: ", node.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " is in state ", node.WsrepStatus,
					" moving it to OFFLINE_SOFT given we have other nodes in the Host group")
				return true
			}
		}

	}
	return false
}
func (cluster *DataClusterImpl) forceRespectManualOfflineSoft(key string, node DataNodeImpl) {
	delete(cluster.ActionNodes, strconv.Itoa(node.HostgroupId)+"_"+key)
	log.Warn(fmt.Sprintf("Unsecure option RespectManualOfflineSoft is TRUE I will remove also the Backup Host group the Node %s for HG id %d. This is unsecure and should be not be used", key, node.HostgroupId))

}

func (cluster *DataClusterImpl) cleanWriters() bool {
	cleanWriter := false
	for key, node := range cluster.WriterNodes {
		if node.ProxyStatus != "ONLINE" {
			delete(cluster.WriterNodes, key)
			log.Debug(fmt.Sprintf("Node %s is not in ONLINE state in writer HG %d removing while evaluating", key, node.HostgroupId))
			if cluster.config.Proxysql.RespectManualOfflineSoft {
				delete(cluster.BackupWriters, key)
				cluster.forceRespectManualOfflineSoft(key, node)
			}
			cleanWriter = true
		}

	}
	if len(cluster.WriterNodes) < 1 {
		if cluster.checkActiveFailover() {
			cluster.RequireFailover = true
		}
		cluster.HasPrimary = false
	}
	return cleanWriter
}

//Once we have done the identification of the node status in EvalNodes we can now process the nodes by ROLE. The main point here is First identify the Good possible writer(s)
func (cluster *DataClusterImpl) evaluateWriters() bool {
	backupWriters := cluster.BackupWriters

	//first action is to check if writers are healthy (ONLINE) and if not remove them
	cluster.cleanWriters()

	/*
		Unfortunately we must execute the loop multiple times because we FIRST loop is to identify the nodes going down
		And only after we can process correctly the up.
		The loop should normally be very short so it should be fast also if redundant
	*/
	cluster.processDownActionMap()

	// Process only Writers but now we Check if node is coming up. Here we also have the first action in case of FAILOVER. bacause if a node is coming up
	// And is a possible replacement for the Writer going down, we need to flag it.
	// Writer are evaluated on the base of the WEIGHT in the config group 8000
	cluster.processUpActionMap()

	//check if in special HG Maintenance and if so remove it from backupWriters
	for key := range cluster.OffLineWriters {
		if _, ok := backupWriters[key]; ok {
			delete(backupWriters, key)
		}
	}

	// At this point our list should have a clear status of what is coming up and/or down.
	// It also has possible failovers, so for the ones left we need to consolidate and eventually add what is missed.
	// let us check if the left writers are to be add or not
	cluster.processFailoverFailBack(backupWriters)

	//only if the failover node is a real node and not the default one HostgroupId = 0 then we add it to action list
	if cluster.FailOverNode.HostgroupId != 0 &&
		//cluster.FailOverNode.HostgroupId != cluster.HgWriterId+cluster.MaintenanceHgRange &&
		cluster.RequireFailover {

		//if cluster has Persistent Primary we must reset the New primary values with the one from primary
		if cluster.PersistPrimarySettings > 0 && cluster.SinglePrimary {
			cluster.copyPrimarySettingsToFailover()
		}
		cluster.ActionNodes[strconv.Itoa(cluster.HgWriterId)+"_"+cluster.FailOverNode.Dns] = cluster.FailOverNode
		log.Warning(fmt.Sprintf("We can try to failover from Backup Writer HG : %s I will try to add it back", cluster.FailOverNode.Dns))
	}

	return true
}

func (cluster *DataClusterImpl) processFailoverFailBack(backupWriters map[string]DataNodeImpl) {
	// TODO can we include in the unit test? I think this is too complex and not deterministic to do so
	for key, node := range backupWriters {
		// First of all we need to be sure node was tested
		if _, ok := cluster.NodesPxc.internal[node.Dns]; ok {

			//the backup node is not present we will try to add it
			if cluster.NodesPxc.internal[node.Dns].ProxyStatus == "ONLINE" &&
				!cluster.NodesPxc.internal[node.Dns].ReadOnly &&
				cluster.NodesPxc.internal[node.Dns].Processed {

				// in this case we just have to add the node given lower number of allowed writers. But only in the same segment
				if _, ok := cluster.WriterNodes[node.Dns]; !ok &&
					len(cluster.WriterNodes) < cluster.MaxNumWriters && cluster.MaxNumWriters > 1 &&
					node.WsrepSegment == cluster.MainSegment {
					node.HostgroupId = cluster.HgWriterId
					cluster.WriterNodes[node.Dns] = node
					node.ActionType = node.DELETE_NODE()
					cluster.ActionNodes[strconv.Itoa(node.ActionType)+"_"+strconv.Itoa(cluster.HgWriterId)+"_"+node.Dns] = node
					node.ActionType = node.INSERT_WRITE()
					cluster.ActionNodes[strconv.Itoa(node.ActionType)+"_"+strconv.Itoa(cluster.HgWriterId)+"_"+node.Dns] = node

					//if is a failover we will evaluate the node with the one already stored in case there is a writer coming UP from offline with higher weight
					// in that case we do not want an existing node to take over but we prefer to have directly the right node up
				} else if cluster.RequireFailover &&
					len(cluster.WriterNodes) < cluster.MaxNumWriters &&
					(node.WsrepSegment == cluster.MainSegment || cluster.ActiveFailover > 1) {
					if node.Weight > cluster.FailOverNode.Weight {
						node.ActionType = node.INSERT_WRITE()
						cluster.FailOverNode = node
						log.Warning(fmt.Sprintf("Failover require node identified as candidate: %s .", key))
					}

				// If we have exceeded the number of writers, the one with lower Weight will be removed
				} else if _, ok := cluster.WriterNodes[node.Dns]; ok &&
					len(cluster.WriterNodes) > cluster.MaxNumWriters &&
					node.ProxyStatus == "ONLINE" {
					//for each node in the cluster writers we check if need to remove any
					cluster.identifyLowerNodeToRemove(node)

				// Now if we have failback and we have a writer with HIGHER weight coming back we need to identify the one with lower again and remove it
				} else if len(cluster.WriterNodes) == cluster.MaxNumWriters &&
					!cluster.RequireFailover &&
					cluster.FailBack {
					//we have a node coming back because failback so WHO is going tobe removed?
					cluster.identifyLowerNodeToRemoveBecauseFailback(node)
				}
			}
		}
	}
}

//We identify who of the node in the Writers pool need to go away because failback
func (cluster *DataClusterImpl) identifyLowerNodeToRemoveBecauseFailback(node DataNodeImpl) bool {
	//we need to loop the writers
	for _, nodeB := range cluster.WriterNodes {
		if node.Weight > nodeB.Weight &&
			(node.WsrepSegment == cluster.MainSegment || cluster.ActiveFailover > 1) {
			node.ActionType = node.INSERT_WRITE()
			node.HostgroupId = cluster.HgWriterId

			// the node with lower weight is removed
			nodeB.RetryDown = cluster.RetryDown + 1
			nodeB.ActionType = nodeB.MOVE_DOWN_OFFLINE()
			cluster.ActionNodes[strconv.Itoa(cluster.HgWriterId)+"_"+node.Dns] = node
			cluster.ActionNodes[strconv.Itoa(cluster.HgWriterId)+"_"+nodeB.Dns] = nodeB

			//let also add it to the Writers to prevent double insertion
			cluster.WriterNodes[node.Dns] = node
			//remove failover flag from cluster
			cluster.RequireFailover = false

			log.Warn(fmt.Sprintf("Failback! Node %s is going down while Node %s is coming up as Writer ",
				strconv.Itoa(nodeB.HostgroupId)+"_"+nodeB.Dns,
				strconv.Itoa(node.HostgroupId)+"_"+node.Dns))
			//if found we just exit no need to loop more
			return true
			//break
		}
	}
	return false
}

//We identify which Node is the one with the lowest weight that needs to be removed from active writers list
func (cluster *DataClusterImpl) identifyLowerNodeToRemove(node DataNodeImpl) bool{
	lowerNode := node
	for _, wNode := range cluster.WriterNodes {
		if wNode.Weight < lowerNode.Weight &&
			wNode.Weight < node.Weight {
			log.Debug("Lower node found ", wNode.Dns ," weight new ", wNode.Weight, " current lower ", lowerNode.Dns, " Lower node weight ", lowerNode.Weight)
			lowerNode = wNode
		}

	}
	lowerNode.HostgroupId = cluster.HgWriterId
	lowerNode.ActionType = node.DELETE_NODE()
	if _, ok := cluster.ActionNodes[strconv.Itoa(cluster.HgWriterId)+"_"+lowerNode.Dns]; !ok {
		log.Debug("We are removing node with lower weight ", lowerNode.Dns, " Weight " , lowerNode.Weight )
		cluster.ActionNodes[strconv.Itoa(cluster.HgWriterId)+"_"+node.Dns] = lowerNode
		// Given we are going to remove this node from writer we also remove it from the collection (see also FR-34)
		delete(cluster.WriterNodes, node.Dns)

		if lowerNode.Dns== node.Dns {
			return false
		}
		return true
	}
	return false
}

func (cluster *DataClusterImpl) processUpActionMap() {
	// TODO can we include in the unit test? I think this is too complex and not deterministic to do so
	for key, node := range cluster.ActionNodes {
		//While evaluating the nodes that are coming up we also check if it is a Failover Node
		var hgI int
		var portI = 0
		var ipaddress = ""
		currentHg := cluster.Hostgroups[node.HostgroupId]
		hg := key[0:strings.Index(key, "_")]
		ipaddress = string(key[len(hg)+1:len(key)])
		ip, port, _ := net.SplitHostPort(ipaddress)
		hgI = global.ToInt(hg)
		portI = global.ToInt(port)
		// We process only WRITERS
		if currentHg.Type == "W" || currentHg.Type == "WREC" {
			if node.ReturnActionCategory(node.ActionType) == "UP" ||
				node.ReturnActionCategory(node.ActionType) == "SWAP_W" {
				log.Debug(fmt.Sprintf("Evaluating for UP writer node key: %s %d %s %d Status: %s", key, hgI, ip, portI, node.ReturnTextFromCode(node.ActionType)))
				//if we have retry Down > 0 we must evaluate it
				if node.RetryUp >= cluster.RetryUp {
					// we remove from backup to prevent double insertion
					//delete(backupWriters,node.Dns)
					//check if we have already a primary or if we have already the max number of writers
					if len(cluster.WriterNodes) < cluster.MaxNumWriters ||
						(len(cluster.WriterNodes) < 1 || cluster.RequireFailover) {
						if cluster.RequireFailover &&
							!cluster.HasFailoverNode &&
							!cluster.HasPrimary {
							// we also check if the weight is higher to be sure we put the nodes in order
							if node.Weight > cluster.FailOverNode.Weight &&
								(node.WsrepSegment == cluster.MainSegment || cluster.ActiveFailover > 1) {
								if cluster.SinglePrimary {
									delete(cluster.ActionNodes, strconv.Itoa(cluster.FailOverNode.HostgroupId)+"_"+cluster.FailOverNode.Dns)
									delete(cluster.WriterNodes, cluster.FailOverNode.Dns)
								}
								cluster.FailOverNode = node
								cluster.WriterNodes[node.Dns] = node
								log.Warning(fmt.Sprintf("FAILOVER!!! Cluster may have identified a Node to failover: %s", key))
							}

						} else {
							cluster.WriterNodes[node.Dns] = node
							log.Warning(fmt.Sprintf("Node %s is coming UP in writer HG %d", key, cluster.HgWriterId))
						}
						//Failback is a pain in the xxxx because it can cause a lot of bad behaviour.
						//IF cluster failback is active we need to check for coming up nodes if our node has higher WEIGHT of current writer(s) and eventually act
					} else if _, ok := cluster.BackupWriters[node.Dns]; ok &&
						cluster.FailBack {

						//if node is already coming up, must be removed from current writers
						delete(cluster.BackupWriters, node.Dns)

						//tempWriters := make(map[string]DataNodeImpl)
						lowerNode := node
						for _, wNode := range cluster.WriterNodes {
							if wNode.Weight < lowerNode.Weight &&
								wNode.Weight < node.Weight {
								lowerNode = wNode
							}
						}
						//IF the lower node IS NOT our new node then we have a FAIL BACK. But only if inside same segment OR if Active failover method allow the use of other segment
						//If instead our new node is the lowest .. no action and ignore it (for writers)
						if node.Dns != lowerNode.Dns &&
							(node.WsrepSegment == cluster.MainSegment || cluster.ActiveFailover > 1) {

							//in this case we need to set the action and also the retry or it will NOT go down consistently
							lowerNode.RetryDown = cluster.RetryDown + 1
							lowerNode.ActionType = lowerNode.MOVE_DOWN_OFFLINE()
							cluster.ActionNodes[strconv.Itoa(lowerNode.HostgroupId)+"_"+lowerNode.Dns] = lowerNode

							//we remove from the writerHG the node that is going down
							delete(cluster.WriterNodes, lowerNode.Dns)

							log.Warn(fmt.Sprintf("Failback! Node %s is going down while Node %s is coming up as Writer ",
								strconv.Itoa(lowerNode.HostgroupId)+"_"+lowerNode.Dns,
								strconv.Itoa(node.HostgroupId)+"_"+node.Dns))

							//let also add it to the Writers to prevent double insertion
							cluster.WriterNodes[node.Dns] = node

							//Given this is a failover I will remove the failover flag if present
							cluster.RequireFailover = false

						} else if node.Dns == lowerNode.Dns &&
							(cluster.RequireFailover || cluster.FailBack) {
							log.Info(fmt.Sprintf("Node %s coming back online but will not be promoted to Writer because lower Weight %d",
								strconv.Itoa(lowerNode.HostgroupId)+"_"+lowerNode.Dns,
								node.Weight))
							delete(cluster.ActionNodes, key)
						}
						//else{
						////	Node should not promote back but we cannot remove it. So we will notify in the log as ERROR to be sure is reported
						//	log.Error(fmt.Sprintf("Node %s is trying to come back as writer but it has lower WEIGHT respect to existing writers. Please MANUALLY remove it from Writer group %d",
						//		strconv.Itoa(lowerNode.HostgroupId) +"_" + lowerNode.Dns , cluster.HgWriterId))
						//	delete(cluster.ActionNodes,strconv.Itoa(lowerNode.HostgroupId) +"_" + lowerNode.Dns)
						//}
					} else if len(cluster.WriterNodes) < cluster.MaxNumWriters && !cluster.RequireFailover {
						log.Info(fmt.Sprintf("Node %s is trying to come UP in writer HG: %d but cannot promote given we already have enough writers %d ", key, cluster.HgWriterId, len(cluster.WriterNodes)))
						delete(cluster.ActionNodes, key)

					} else {
						//In this case we cannot put back another writer and node must be removed from Actionlist
						log.Info(fmt.Sprintf("Node %s is trying to come UP in writer HG: %d but cannot promote given we already have enough writers %d ", key, cluster.HgWriterId, len(cluster.WriterNodes)))
						delete(cluster.ActionNodes, key)
					}
				} else {
					log.Debug(fmt.Sprintf("Retry still low for UP writer node key: %s %d %s %d Status: %s Retry: %d",
						key, hgI, ip, portI, node.ReturnTextFromCode(node.ActionType), node.RetryUp))
				}
			}
		}
	}
}

func (cluster *DataClusterImpl) processDownActionMap() {
	// TODO can we include in the unit test? I think this is too complex and not deterministic to do so
	for key, node := range cluster.ActionNodes {
		var hgI int
		var portI = 0
		var ipaddress = ""
		currentHg := cluster.Hostgroups[node.HostgroupId]
		hg := key[0:strings.Index(key, "_")]
		ipaddress = string(key[len(hg)+1:len(key)])
		ip, port, _ := net.SplitHostPort(ipaddress)
		hgI = global.ToInt(hg)
		portI = global.ToInt(port)

		// We process only WRITERS and check for nodes marked in EvalNodes as DOWN
		if currentHg.Type == "W" || currentHg.Type == "WREC" {
			//We must first check if the node is going down, because if it is single primary we probably need to failover
			if node.ReturnActionCategory(node.ActionType) == "DOWN" ||
				node.ReturnActionCategory(node.ActionType) == "SWAP_R" {
				log.Debug(fmt.Sprintf("Evaluating for DOWN writer node key: %s %d %s %d Status: %s", key, hgI, ip, portI, node.ReturnTextFromCode(node.ActionType)))
				//if we have retry Down > 0 we must evaluate it
				if node.RetryDown >= cluster.RetryDown {
					//check if we have one writer left and is this is the writer node
					if _, ok := cluster.WriterNodes[node.Dns]; ok {
						if len(cluster.WriterNodes) == 1 {
							//if we are here this means our Writer is going down and we need to failover
							if cluster.checkActiveFailover() {
								cluster.RequireFailover = true
							}
							cluster.HasFailoverNode = false
							cluster.HasPrimary = false
							cluster.Haswriter = false
							//I remove the node from writers and backup
							delete(cluster.BackupWriters, node.Dns)
							delete(cluster.WriterNodes, node.Dns)
							log.Warning(fmt.Sprintf("FAILOVER!!! Cluster Needs a new Writer to fail-over last writer is going down %s", key))
						} else if len(cluster.WriterNodes) > 1 {
							delete(cluster.WriterNodes, node.Dns)
							delete(cluster.BackupWriters, node.Dns)
						}
					}
				} else {
					log.Debug(fmt.Sprintf("Retry still low for Down writer node key: %s %d %s %d Status: %s Retry: %d",
						key, hgI, ip, portI, node.ReturnTextFromCode(node.ActionType), node.RetryDown))
				}
			}
		}
	}
}

/*
This function checks if the cluster has activeFailover active and return true in this case.
If not it also raise a Warning given that not activitate failover is a bad bad idea
 */
func (cluster *DataClusterImpl) checkActiveFailover() bool {
	if cluster.ActiveFailover < 1 {
		log.Warning(fmt.Sprintf("Cluster require to perform fail-over but the settings of ActiveFailover is preventing it with a value of %d. Best practice, and default is to have ActiveFailover = 1 ",cluster.ActiveFailover))
		return false
	}
	return true
}

/*
This method identify if we have an active reader and if not will force (no matter what) the writer to be a reader.
It will also remove the writer as reader is we have WriterIsAlsoReader <> 1 and reader group with at least 1 element

*/
func (cluster *DataClusterImpl) evaluateReaders() bool {
	// TODO can we include in the unit test? I think this is too complex and not deterministic to do so
	readerNodes := make(map[string]DataNodeImpl)
	CopyMap(readerNodes, cluster.ReaderNodes)
	actionNodes := cluster.ActionNodes

	//first I add any missing node in the reader list
	for key, node := range cluster.BackupReaders {
		if _, ok := readerNodes[key]; !ok {
			readerNodes[key] = node
		}
	}

	//We process the action nodes to identify what is coming up and what down
	//for _, node := range readerNodes {
	cluster.processUpAndDownReaders(actionNodes, readerNodes)
	//}

	//check for nodes in the reader group that are also in the OFFLINE_HG for reads
	//check for offline_soft nodes left in the readerNodes
	for key, node := range readerNodes {
		if okHgR := cluster.OffLineReaders[key]; okHgR.Dns != "" || (node.ReturnActionCategory(node.ActionType) == "NOTHING_TO_DO" &&
			cluster.ReaderNodes[node.Dns].ProxyStatus == "OFFLINE_SOFT") {
			delete(readerNodes, node.Dns)
			if cluster.config.Proxysql.RespectManualOfflineSoft {
				delete(cluster.BackupReaders, node.Dns)
				cluster.forceRespectManualOfflineSoft(node.Dns, node)
			}
		}
	}

	//Check for readers and see if we can add
	if len(readerNodes) <= 0 {
		if len(cluster.WriterNodes) > 0 {
			for _, node := range cluster.WriterNodes {
				if node.Processed && node.ProxyStatus == "ONLINE" {
					readerNodes[node.Dns] = node
				}
			}
		}
	} else {
		// in case we need to deal with multiple readers, we need to also deal with writer is also read flag
		cluster.processWriterIsAlsoReader(readerNodes)
	}
	//whatever is now in the readNodes map should be pushed in offline read HG to be evaluated and move back in prod if OK
	for _, node := range readerNodes {
		key := node.Dns
		if okR := cluster.ReaderNodes[key]; okR.Dns != "" || !node.Processed {
			delete(readerNodes, key)
		} else {
			cluster.pushNewNode(node)
		}
	}

	return true
}

func (cluster *DataClusterImpl) processWriterIsAlsoReader(readerNodes map[string]DataNodeImpl) bool {
	// WriterIsAlsoReader != 1 so we need to check if writer is in reader group and remove it in the case we have more readers
	if cluster.WriterIsReader != 1 {

		if len(readerNodes) > 1 {
			for key, node := range readerNodes {
				//if the reader node is in the writer group and we have more than 1 reader good left, then we can remove the reader node
				if _, ok := cluster.WriterNodes[key]; ok &&
					node.HostgroupId == cluster.HgReaderId {
					delete(readerNodes, key)
					node.HostgroupId = cluster.HgReaderId
					node.ActionType = node.DELETE_NODE()
					cluster.ActionNodes[strconv.Itoa(cluster.HgReaderId)+"_"+node.Dns] = node
					return true
					//But if the reader node is from backup group this means we are just checkin if we should re-insert it so we do not need to delete, but just remove it from the list
				} else if _, ok := cluster.WriterNodes[key]; ok &&
					node.HostgroupId == cluster.BackupHgReaderId {
					delete(readerNodes, key)
					return true
				}

			}
		} else if len(readerNodes) == 1 {
			//if we see that we have 1 item left in readers and this one is also the writer and is already present, we will NOT process it otherwise we will insert back
			for key, node := range readerNodes {
				if _, ok := cluster.WriterNodes[key]; ok && node.HostgroupId == cluster.HgReaderId {
					node.Processed = false
					readerNodes[key] = node
				}
			}
			return false
		}
	}

	return false
}

func (cluster *DataClusterImpl) processUpAndDownReaders(actionNodes map[string]DataNodeImpl, readerNodes map[string]DataNodeImpl) bool {

	for _, actionNode := range actionNodes {
		currentHg := cluster.Hostgroups[actionNode.HostgroupId]
		if currentHg.Type == "R" || currentHg.Type == "RREC" {
			if actionNode.ReturnActionCategory(actionNode.ActionType) == "DOWN" ||
				actionNode.ReturnActionCategory(actionNode.ActionType) == "SWAP_W" {
				delete(readerNodes, actionNode.Dns)
				return true
				// If node is coming up we add it to the list of readers
			} else if actionNode.ReturnActionCategory(actionNode.ActionType) == "UP" ||
				actionNode.ReturnActionCategory(actionNode.ActionType) == "SWAP_R" {
				if actionNode.Processed && actionNode.ProxyStatus == "ONLINE" {
					actionNode.HostgroupId = cluster.HgReaderId
					actionNode.Processed = false
					readerNodes[actionNode.Dns] = actionNode
					return true
				}
			}
		}
	}
	return false
}

//add a new non existing Reader but force a delete first to avoid dirty writes. This only IF a Offline node with that key is NOT already present
func (cluster *DataClusterImpl) pushNewNode(node DataNodeImpl) bool {
	if ok := cluster.OffLineReaders[node.Dns]; ok.Dns != "" {
		return false
	}

	node.HostgroupId = cluster.OffLineHgReaderID
	node.ActionType = node.DELETE_NODE()
	node.NodeIsNew = true
	cluster.ActionNodes[strconv.Itoa(node.ActionType)+"_"+strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node
	node.ActionType = node.INSERT_READ()
	cluster.ActionNodes[strconv.Itoa(node.ActionType)+"_"+strconv.Itoa(node.HostgroupId)+"_"+node.Dns] = node

	return true
}

//In this method we modify the value of the failover to match the Promary
func (cluster *DataClusterImpl) copyPrimarySettingsToFailover() {
	cluster.FailOverNode.Weight = cluster.PersistPrimary[0].Weight
	cluster.FailOverNode.Compression = cluster.PersistPrimary[0].Compression
	cluster.FailOverNode.MaxConnection = cluster.PersistPrimary[0].MaxConnection
	cluster.FailOverNode.MaxReplicationLag = cluster.PersistPrimary[0].MaxReplicationLag
	cluster.FailOverNode.MaxLatency = cluster.PersistPrimary[0].MaxLatency
	cluster.FailOverNode.ActionType = cluster.FailOverNode.INSERT_WRITE()
	delete(cluster.ActionNodes, strconv.Itoa(cluster.HgWriterId+cluster.MaintenanceHgRange)+"_"+cluster.FailOverNode.Dns)
	cluster.FailOverNode.ProxyStatus = "ONLINE"

	if cluster.PersistPrimarySettings > 1 {
		myDns := cluster.PersistPrimary[0].Dns
		myReader := cluster.ReaderNodes[cluster.FailOverNode.Dns]
		myBkupReader := cluster.BackupReaders[myDns]
		myReader.Weight = myBkupReader.Weight
		myReader.Compression = myBkupReader.Compression
		myReader.MaxConnection = myBkupReader.MaxConnection
		myReader.MaxReplicationLag = myBkupReader.MaxReplicationLag
		myReader.MaxLatency = myBkupReader.MaxLatency
		myReader.ActionType = myReader.INSERT_READ()
		delete(cluster.ActionNodes, strconv.Itoa(cluster.HgReaderId+cluster.MaintenanceHgRange)+"_"+cluster.FailOverNode.Dns)
		cluster.ActionNodes[strconv.Itoa(cluster.HgReaderId)+"_"+myReader.Dns] = myReader
	}

}

//This function reset the node values to their defaults defined in BackupHG
func (cluster *DataClusterImpl) resetNodeDefault(nodeIn DataNodeImpl, nodeDefault DataNodeImpl) DataNodeImpl {
	nodeIn.Weight = nodeDefault.Weight
	nodeIn.Compression = nodeDefault.Compression
	nodeIn.MaxConnection = nodeDefault.MaxConnection
	nodeIn.MaxReplicationLag = nodeDefault.MaxReplicationLag
	nodeIn.MaxLatency = nodeDefault.MaxLatency

	return nodeIn
}

func (cluster *DataClusterImpl) compareNodeDefault(nodeIn DataNodeImpl, nodeDefault DataNodeImpl) bool {
	if nodeIn.Weight == nodeDefault.Weight &&
		nodeIn.Compression == nodeDefault.Compression &&
		nodeIn.MaxConnection == nodeDefault.MaxConnection &&
		nodeIn.MaxReplicationLag == nodeDefault.MaxReplicationLag &&
		nodeIn.MaxLatency == nodeDefault.MaxLatency {
		return false
	} else {
		return true
	}

}

// *** DATA NODE SECTION =============================================

/*this method is used to assign a connection to a proxySQL node
return true if successful in any other case false
*/
func (node *DataNodeImpl) GetConnection() bool {
	if global.Performance {
		global.SetPerformanceObj("node_connection_"+node.Dns, true, log.DebugLevel)
	}
	//dns := node.User + ":" + node.Password + "@tcp(" + node.Dns + ":"+ strconv.Itoa(node.Port) +")/admin" //
	//if log.GetLevel() == log.DebugLevel {log.Debug(dns)}

	//user:password@tcp([de:ad:be:ef::ca:fe]:80)/dbname?timeout=90s&collation=utf8mb4_unicode_ci

	/*
		to use ONLY ssl without certificates
		tls=true
		?timeout=90s&tls=true

		rootCertPool := x509.NewCertPool()
		pem, err := ioutil.ReadFile("/path/ca-cert.pem")
		if err != nil {
		   log.Fatal(err)
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
		   log.Fatal("Failed to append PEM.")
		}
		clientCert := make([]tls.Certificate, 0, 1)
		certs, err := tls.LoadX509KeyPair("/path/client-cert.pem", "/path/client-    key.pem")
		if err != nil {
		   log.Fatal(err)
		}
		clientCert = append(clientCert, certs)
		mysql.RegisterTLSConfig("custom", &tls.Config{
		                         RootCAs: rootCertPool,
		                         Certificates: clientCert,
		                        })

		db, err := sql.Open("mysql", "user@tcp(localhost:3306)/test?tls=custom")
	*/
	attributes := "?timeout=1s"

	if node.UseSsl {
		if node.Ssl == nil {
			attributes = attributes + "&tls=skip-verify"
		} else if node.Ssl.sslCertificatePath != "" {
			ca := node.Ssl.sslCertificatePath + global.Separator + node.Ssl.sslCa
			client := node.Ssl.sslCertificatePath + global.Separator + node.Ssl.sslClient
			key := node.Ssl.sslCertificatePath + global.Separator + node.Ssl.sslKey

			rootCertPool := x509.NewCertPool()
			pem, err := ioutil.ReadFile(ca)
			if err != nil {
				log.Error(err, " While trying to connect to node (CA certificate) ", node.Dns)
				return false
			}
			if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
				log.Error(err, " While trying to connect to node (PEM certificate) ", node.Dns)
				return false;
			}
			clientCert := make([]tls.Certificate, 0, 1)
			certs, err := tls.LoadX509KeyPair(client, key)
			if err != nil {
				log.Error(err, " While trying to connect to node (Key certificate) ", node.Dns)
				return false
			}
			clientCert = append(clientCert, certs)
			mysql.RegisterTLSConfig("custom", &tls.Config{
				RootCAs:      rootCertPool,
				Certificates: clientCert,
			})
			//attributes = attributes + "&tls=custom"
			attributes = attributes + "&tls=skip-verify"
		}
	}

	db, err := sql.Open("mysql", node.User+":"+node.Password+"@tcp("+net.JoinHostPort(node.Ip, strconv.Itoa(node.Port))+")/performance_schema"+attributes)

	//defer db.Close()
	node.Connection = db
	// if there is an error opening the connection, handle it
	if err != nil {
		log.Error(err.Error())
		return false
	}

	// Open doesn't open a connection. Validate DSN data:
	err = db.Ping()
	if err != nil {
		log.Error(err.Error())
		node.NodeTCPDown = true
		return false
	}
	node.NodeTCPDown = false

	if global.Performance {
		global.SetPerformanceObj("node_connection_"+node.Dns, false, log.DebugLevel)
	}
	return true
}

/*this method is call to close the connection to a proxysql node
return true if successful in any other case false
*/

func (node *DataNodeImpl) CloseConnection() bool {
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

func (node *DataNodeImpl) getNodeInternalInformation(dml string) map[string]string {
	recordset, err := node.Connection.Query(dml)
	if err != nil {
		log.Error(err.Error())
		return nil
	}

	variables := make(map[string]string)
	var varName string
	var varValue string
	for recordset.Next() {
		recordset.Scan(&varName,
			&varValue)
		variables[varName] = varValue
	}

	return variables
}

func (node *DataNodeImpl) getNodeInformations(what string) map[string]string {

	switch dml := strings.ToLower(what); dml {
	case "variables":
		return node.getNodeInternalInformation(SQLPxc.Dml_get_variables)
	case "status":
		return node.getNodeInternalInformation(SQLPxc.Dml_get_status)
	//case "pxc_view":
	//	return node.getNodeInternalInformation( strings.ReplaceAll( SQLPxc.Dml_get_pxc_view,"?",node.Variables["server_uuid"]))
	default:
		return nil
	}
}

func (node *DataNodeImpl) getRetry(writeHG int, readHG int) {
	//var valueUp string
	comment := node.Comment
	hgRetry := strconv.Itoa(writeHG) + "_W_" + strconv.Itoa(readHG) + "_R_retry_"
	regEUp := regexp.MustCompile(hgRetry + "up=\\d;")
	regEDw := regexp.MustCompile(hgRetry + "down=\\d;")
	up := regEUp.FindAllString(comment, -1)
	down := regEDw.FindAllString(comment, -1)
	if len(up) > 0 {
		comment = strings.ReplaceAll(comment, up[0], "")
		value := up[0][strings.Index(up[0], "=")+1 : len(up[0])-1]
		if len(value) > 0 {
			iValue, err := strconv.Atoi(value)
			if err == nil {
				log.Debug(" Retry up = ", value)
				node.RetryUp = iValue
			} else {
				iValue = 0
			}
		}
	}
	if len(down) > 0 {
		comment = strings.ReplaceAll(comment, down[0], "")
		value := down[0][strings.Index(down[0], "=")+1 : len(down[0])-1]
		if len(value) > 0 {
			iValue, err := strconv.Atoi(value)
			if err == nil {
				log.Debug(" Retry down = ", value)
				node.RetryDown = iValue
			} else {
				iValue = 0
			}
		}
	}
	node.Comment = comment
}

/*
NODE CONSTANT declaration by methods
*/
//goland:noinspection ALL
func (node *DataNodeImpl) NOTHING_TO_DO() int {
	return 0 // move a node from OFFLINE_SOFT
}
func (node *DataNodeImpl) MOVE_UP_OFFLINE() int {
	return 1000 // move a node from OFFLINE_SOFT
}
func (node *DataNodeImpl) MOVE_UP_HG_CHANGE() int {
	return 1010 // move a node from HG Maintenance (plus hg id) to reader HG
}
func (node *DataNodeImpl) RESET_DEFAULTS() int {
	return 2010 //reset the mysql server node values to defaults has declared in group config (8000)
}
func (node *DataNodeImpl) MOVE_DOWN_HG_CHANGE() int {
	return 3001 // move a node from original HG to maintenance HG (HG Maintenance (plus hg id) ) kill all existing connections
}

func (node *DataNodeImpl) MOVE_DOWN_OFFLINE() int {
	return 3010 // move node to OFFLINE_soft keep existing connections, no new connections.
}
func (node *DataNodeImpl) MOVE_TO_MAINTENANCE() int {
	return 3020 // move node to OFFLINE_soft keep existing connections, no new connections because maintenance.
}
func (node *DataNodeImpl) MOVE_OUT_MAINTENANCE() int {
	return 3030 // move node to OFFLINE_soft keep existing connections, no new connections because maintenance.
}
func (node *DataNodeImpl) INSERT_READ() int {
	return 4010 // Insert a node in the reader host group
}
func (node *DataNodeImpl) INSERT_WRITE() int {
	return 4020 // Insert a node in the writer host group
}
func (node *DataNodeImpl) DELETE_NODE() int {
	return 5000 // this remove the node from the hostgroup
}
func (node *DataNodeImpl) MOVE_SWAP_READER_TO_WRITER() int {
	return 5001 // remove a node from HG reader group and add it to HG writer group
}
func (node *DataNodeImpl) MOVE_SWAP_WRITER_TO_READER() int {
	return 5101 // remove a node from HG writer group and add it to HG reader group
}
func (node *DataNodeImpl) SAVE_RETRY() int {
	return 9999 // this remove the node from the hostgroup
}

func (node *DataNodeImpl) ReturnTextFromCode(code int) string {
	switch code {
	case 0:
		return "NOTHING_TO_DO"
	case 1000:
		return "MOVE_UP_OFFLINE"
	case 1010:
		return "MOVE_UP_HG_CHANGE"
	case 2010:
		return "RESET_DEFAULTS"
	case 3001:
		return "MOVE_DOWN_HG_CHANGE"
	case 3010:
		return "MOVE_DOWN_OFFLINE"
	case 3020:
		return "MOVE_TO_MAINTENANCE"
	case 3030:
		return "MOVE_OUT_MAINTENANCE"
	case 4010:
		return "INSERT_READ"
	case 4020:
		return "INSERT_WRITE"
	case 5000:
		return "DELETE_NODE"
	case 5001:
		return "MOVE_SWAP_READER_TO_WRITER"
	case 5101:
		return "MOVE_SWAP_WRITER_TO_READER"
	case 9999:
		return "SAVE_RETRY"
	}

	return ""
}
func (node *DataNodeImpl) ReturnActionCategory(code int) string {
	switch code {
	case 0:
		return "NOTHING_TO_DO"
	case 1000:
		return "UP"
	case 1010:
		return "UP"
	case 3001:
		return "DOWN"
	case 3010:
		return "DOWN"
	case 3020:
		return "DOWN"
	case 3030:
		return "UP"
	case 4010:
		return "UP"
	case 4020:
		return "UP"
	case 5000:
		return "DOWN"
	case 5001:
		return "SWAP_W"
	case 5101:
		return "SWAP_R"
	case 9999:
		return "SAVE_RETRY"
	}

	return ""
}

//from pxc
func (node *DataNodeImpl) getPxcView(dml string) PxcClusterView {
	recordset, err := node.Connection.Query(dml)
	if err != nil {
		log.Error(err.Error())
	}
	var pxcView PxcClusterView
	for recordset.Next() {
		recordset.Scan(&pxcView.HostName,
			&pxcView.Uuid,
			&pxcView.Status,
			&pxcView.LocalIndex,
			&pxcView.Segment)
	}
	return pxcView

}

//We parallelize the information retrieval using goroutine
func (node DataNodeImpl) getInfo(wg *global.MyWaitGroup, cluster *DataClusterImpl) int {
	if global.Performance {
		global.SetPerformanceObj(fmt.Sprintf("Get info for node %s", node.Dns), true, log.DebugLevel)
	}
	// Get the connection
	if !node.GetConnection(){
		node.NodeTCPDown = true
	}
	/*
		if connection is functioning we try to get the info
		Otherwise we go on and set node as NOT processed
	*/
	// get variables and status first then pxc_view
	if !node.NodeTCPDown {
		node.Variables = node.getNodeInformations("variables")
		node.Status = node.getNodeInformations("status")
		if node.Variables["server_uuid"] != "" {
			node.PxcView = node.getPxcView(strings.ReplaceAll(SQLPxc.Dml_get_pxc_view, "?", node.Status["wsrep_gcomm_uuid"]))
		}

		node.Processed = true

		//set the specific monitoring parameters
		node.setParameters()
		if global.Performance {
			global.SetPerformanceObj(fmt.Sprintf("Get info for node %s", node.Dns), false, log.DebugLevel)
		}
	} else {
		node.Processed = false
		log.Warn("Cannot load information (variables/status/pxc_view) for node: ", node.Dns)
	}

	cluster.NodesPxc.Store(node.Dns, node)
	log.Debug("node ", node.Dns, " done")

	// we close the connection as soon as done
	if global.Performance {
		global.SetPerformanceObj(fmt.Sprintf("Closing connection for node %s", node.Dns), true, log.DebugLevel)
	}

	node.CloseConnection()

	if global.Performance {
		global.SetPerformanceObj(fmt.Sprintf("Closing connection for node %s", node.Dns), false, log.DebugLevel)
	}

	//We decrease the counter running go routines
	wg.DecreaseCounter()
	log.Debug(fmt.Sprintf("waitingGroup decreased by node %s: , now contains #%d",node.Dns,wg.ReportCounter() ))
	return 0
}

//here we set and normalize the parameters coming from different sources for the PXC object
func (node *DataNodeImpl) setParameters() {
	node.WsrepLocalIndex = node.PxcView.LocalIndex
	node.PxcMaintMode = node.Variables["pxc_maint_mode"]
	node.WsrepConnected = global.ToBool(node.Status["wsrep_connected"], "ON")
	node.WsrepDesinccount = global.ToInt(node.Status["wsrep_desync_count"])
	node.WsrepDonorrejectqueries = global.ToBool(node.Variables["wsrep_sst_donor_rejects_queries"], "ON")
	node.WsrepGcommUuid = node.Status["wsrep_gcomm_uuid"]
	node.WsrepProvider = global.FromStringToMAp(node.Variables["wsrep_provider_options"], ";")
	node.HasPrimaryState = global.ToBool(node.Status["wsrep_cluster_status"], "Primary")

	node.WsrepClusterName = node.Variables["wsrep_cluster_name"]
	node.WsrepClusterStatus = node.Status["wsrep_cluster_status"]
	node.WsrepLocalRecvQueue = global.ToInt(node.Status["wsrep_local_recv_queue"])
	node.WsrepNodeName = node.Variables["wsrep_node_name"]
	node.WsrepClusterSize = global.ToInt(node.Status["wsrep_cluster_size"])
	node.WsrepPcWeight = global.ToInt(node.WsrepProvider["pc.weight"])
	node.WsrepReady = global.ToBool(node.Status["wsrep_ready"], "on")
	node.WsrepRejectqueries = !global.ToBool(node.Variables["wsrep_reject_queries"], "none")
	node.WsrepSegment = global.ToInt(node.WsrepProvider["gmcast.segment"])
	node.WsrepStatus = global.ToInt(node.Status["wsrep_local_state"])
	node.ReadOnly = global.ToBool(node.Variables["read_only"], "on")

}

// Sync Map
//=====================================
func NewRegularIntMap() *SyncMap {
	return &SyncMap{
		internal: make(map[string]DataNodeImpl),
	}
}

func (rm *SyncMap) Load(key string) (value DataNodeImpl, ok bool) {
	rm.RLock()
	defer rm.RUnlock()
	result, ok := rm.internal[key]

	return result, ok
}

func (rm *SyncMap) Delete(key string) {
	rm.Lock()
	defer rm.Unlock()
	delete(rm.internal, key)

}

func (rm *SyncMap) Store(key string, value DataNodeImpl) {
	rm.Lock()
	defer rm.Unlock()
	rm.internal[key] = value

}

func (rm *SyncMap) ExposeMap() map[string]DataNodeImpl {
	return rm.internal
}

//====================
//Generic
func MergeMaps(arrayOfMaps [4]map[string]DataNodeImpl) map[string]DataNodeImpl {
	mergedMap := make(map[string]DataNodeImpl)

	for i := 0; i < len(arrayOfMaps); i++ {
		map1 := arrayOfMaps[i]
		for k, v := range map1 {
			hg := strconv.Itoa(v.HostgroupId) + "_"
			mergedMap[hg+k] = v
		}
	}
	return mergedMap

}

func CopyMap(mapDest map[string]DataNodeImpl, mapSource map[string]DataNodeImpl) map[string]DataNodeImpl {

	for k, v := range mapSource {
		mapDest[k] = v
	}

	return mapDest

}
