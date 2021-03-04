package dataobjects

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"dbwrapper"
	"global"

	SQLPxc "sql/Pcx"
	SQLProxy "sql/Proxy"

	"github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

type DataNode struct {
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
}

type DataCluster struct {
	ActiveFailover    int
	FailBack          bool
	ActionNodes       map[string]DataNodePxc
	BackupReaders     map[string]DataNodePxc
	BackupWriters     map[string]DataNodePxc
	BackupHgReaderId  int
	BakcupHgWriterId  int
	CheckTimeout      int
	ClusterIdentifier int //cluster_id
	ClusterSize       int
	HasPrimary        bool
	ClusterName       string
	Comment           string
	Debug             bool
	FailOverNode      DataNodePxc
	HasFailoverNode   bool
	Haswriter         bool
	HgReaderId        int
	HgWriterId        int
	Hostgroups        map[int]Hostgroup
	//	Hosts map[string] DataNode
	MainSegment       int
	MonitorPassword   string
	MonitorUser       string
	Name              string
	NodesPxc          *SyncMap //[string] DataNodePxc // <ip:port,datanode>
	NodesPxcMaint     []DataNodePxc
	MaxNumWriters     int
	OffLineReaders    map[string]DataNodePxc
	OffLineWriters    map[string]DataNodePxc
	OffLineHgReaderID int
	OffLineHgWriterId int
	ReaderNodes       map[string]DataNodePxc
	RequireFailover   bool
	RetryDown         int
	RetryUp           int
	Singlenode        bool
	SinglePrimary     bool
	Size              int
	Ssl               *SslCertificates
	Status            int
	WriterIsReader    int
	WriterNodes       map[string]DataNodePxc
}

type SyncMap struct {
	sync.RWMutex
	internal map[string]DataNodePxc
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
func (cluster *DataCluster) init(config *global.Configuration, connectionProxy dbwrapper.DBConnection) bool {

	if global.Performance {
		global.SetPerformanceObj("data_cluster_init", true, log.InfoLevel)
	}
	//set parameters from the config file
	cluster.Debug = config.Global.Debug
	cluster.ClusterIdentifier = config.PxcCluster.ClusterID
	cluster.CheckTimeout = config.PxcCluster.CheckTimeOut
	cluster.MainSegment = config.PxcCluster.MainSegment
	cluster.ActiveFailover = config.PxcCluster.ActiveFailover
	cluster.FailBack = config.PxcCluster.FailBack

	//Enable SSL support
	if config.PxcCluster.SslClient != "" && config.PxcCluster.SslKey != "" && config.PxcCluster.SslCa != "" {
		ssl := new(SslCertificates)
		ssl.sslClient = config.PxcCluster.SslClient
		ssl.sslKey = config.PxcCluster.SslKey
		ssl.sslCa = config.PxcCluster.SslCa
		if config.PxcCluster.SslcertificatePath != "" {
			ssl.sslCertificatePath = config.PxcCluster.SslcertificatePath
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
func (cluster *DataCluster) getNodesInfo() bool {
	var waitingGroup global.MyWaitGroup

	//Before getting the information, we check if any node in the 8000 is gone lost and if so we try to add it back
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
	log.Debug("time taken :", timems, " checkTimeOut : ", cluster.CheckTimeout)
	if int(timems) > cluster.CheckTimeout {
		log.Error("CheckTimeout exceeded try to increase it above the execution time : ", timems)
		//os.Exit(1)

	}
	return true
}

/*
This functions get the nodes list from the proxysql table mysql_servers for the given HGs and check their conditions
Ony one test for IP:port is executed and status shared across HGs
In debug-dev mode information is retrieved sequentially.
In prod is parallelized

*/
func (cluster *DataCluster) loadNodes(connectionProxy dbwrapper.DBConnection) bool {
	// get list of nodes from ProxySQL
	if global.Performance {
		global.SetPerformanceObj("loadNodes", true, log.InfoLevel)
	}
	var sb strings.Builder
	sb.WriteString(strconv.Itoa(cluster.HgWriterId))
	sb.WriteString("," + strconv.Itoa(cluster.HgReaderId))
	sb.WriteString("," + strconv.Itoa(cluster.BakcupHgWriterId))
	sb.WriteString("," + strconv.Itoa(cluster.BackupHgReaderId))
	sb.WriteString("," + strconv.Itoa(cluster.OffLineHgWriterId))
	sb.WriteString("," + strconv.Itoa(cluster.OffLineHgReaderID))

	cluster.ActionNodes = make(map[string]DataNodePxc)
	cluster.NodesPxc = NewRegularIntMap() //make(map[string]DataNodePxc)
	cluster.BackupWriters = make(map[string]DataNodePxc)
	cluster.BackupReaders = make(map[string]DataNodePxc)
	cluster.WriterNodes = make(map[string]DataNodePxc)
	cluster.ReaderNodes = make(map[string]DataNodePxc)
	cluster.OffLineWriters = make(map[string]DataNodePxc)
	cluster.OffLineReaders = make(map[string]DataNodePxc)

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
		var myNode DataNodePxc
		recordset.Scan(&myNode.DataNodeBase.HostgroupId,
			&myNode.DataNodeBase.Ip,
			&myNode.DataNodeBase.Port,
			&myNode.DataNodeBase.GtidPort,
			&myNode.DataNodeBase.ProxyStatus,
			&myNode.DataNodeBase.Weight,
			&myNode.DataNodeBase.Compression,
			&myNode.DataNodeBase.MaxConnection,
			&myNode.DataNodeBase.MaxReplicationLag,
			&myNode.DataNodeBase.UseSsl,
			&myNode.DataNodeBase.MaxLatency,
			&myNode.DataNodeBase.Comment,
			&myNode.DataNodeBase.ConnUsed)
		myNode.DataNodeBase.User = cluster.MonitorUser
		myNode.DataNodeBase.Password = cluster.MonitorPassword
		myNode.DataNodeBase.Dns = myNode.DataNodeBase.Ip + ":" + strconv.Itoa(myNode.DataNodeBase.Port)
		if len(myNode.DataNodeBase.Comment) > 0 {
			myNode.DataNodeBase.getRetry(cluster.HgWriterId, cluster.HgReaderId)
		}

		//Load ssl object to node if present in cluster/config
		if cluster.Ssl != nil {
			myNode.DataNodeBase.Ssl = cluster.Ssl
		}

		switch myNode.DataNodeBase.HostgroupId {
		case cluster.HgWriterId:
			cluster.WriterNodes[myNode.DataNodeBase.Dns] = myNode
		case cluster.HgReaderId:
			cluster.ReaderNodes[myNode.DataNodeBase.Dns] = myNode
		case cluster.BakcupHgWriterId:
			cluster.BackupWriters[myNode.DataNodeBase.Dns] = myNode
		case cluster.BackupHgReaderId:
			cluster.BackupReaders[myNode.DataNodeBase.Dns] = myNode
		case cluster.OffLineHgWriterId:
			cluster.OffLineWriters[myNode.DataNodeBase.Dns] = myNode
		case cluster.OffLineHgReaderID:
			cluster.OffLineReaders[myNode.DataNodeBase.Dns] = myNode
		}

		/*
			We add only the real servers in the list to check with DB access
			we include all the HG operating like Write/Read and relevant OFFLINE special HG
		*/
		if _, ok := cluster.NodesPxc.ExposeMap()[myNode.DataNodeBase.Dns]; !ok {
			if myNode.DataNodeBase.HostgroupId == cluster.HgWriterId ||
				myNode.DataNodeBase.HostgroupId == cluster.HgReaderId ||
				myNode.DataNodeBase.HostgroupId == cluster.OffLineHgWriterId ||
				myNode.DataNodeBase.HostgroupId == cluster.OffLineHgReaderID {
				cluster.NodesPxc.Store(myNode.DataNodeBase.Dns, myNode)
			}
		}

	}
	if global.Performance {
		global.SetPerformanceObj("loadNodes", false, log.InfoLevel)
	}
	return true
}

//load values from db disk in ProxySQL
func (cluster *DataCluster) getParametersFromProxySQL(config *global.Configuration) bool {

	cluster.ClusterIdentifier = config.PxcCluster.ClusterID
	cluster.HgWriterId = config.PxcCluster.HgW
	cluster.HgReaderId = config.PxcCluster.HgR
	cluster.BakcupHgWriterId = config.PxcCluster.BckHgW
	cluster.BackupHgReaderId = config.PxcCluster.BckHgR
	cluster.SinglePrimary = config.PxcCluster.SinglePrimary
	cluster.MaxNumWriters = config.PxcCluster.MaxNumWriters
	cluster.WriterIsReader = config.PxcCluster.WriterIsAlsoReader
	cluster.RetryUp = config.PxcCluster.RetryUp
	cluster.RetryDown = config.PxcCluster.RetryDown
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
	cluster.OffLineHgReaderID = 9000 + cluster.HgReaderId
	cluster.OffLineHgWriterId = 9000 + cluster.HgWriterId
	return true
	//}

}

/*
This method is responsible to be sure that each node list Writer/read/backups/offline are aligned with the status just Identified from the live nodes
Here we align the physical server status with the role of each server.
Given a physical node is present in more then a list we need to evaluate all of them for each physical server
this is not huge as work because the loop will normally have a max of 5 physical nodes for PXC and 9 for GR
*/
func (cluster *DataCluster) consolidateNodes() bool {

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
We have only active HostGroups here like the R/W ones and the special 9000
*/
func (cluster *DataCluster) consolidateHGs() bool {
	specialWrite := cluster.HgWriterId + 9000
	specialRead := cluster.HgReaderId + 9000
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
func (cluster *DataCluster) calculateHgOnlineSize(myMap map[string]DataNodePxc) int {
	var i int
	for _, node := range myMap {
		if node.DataNodeBase.ProxyStatus == "ONLINE" {
			i++
		}
	}
	return i
}

// We align only the relevant information, not all the node
func (cluster *DataCluster) alignNodeValues(destination DataNodePxc, source DataNodePxc) DataNodePxc {
	destination.DataNodeBase.ActionType = source.DataNodeBase.ActionType
	destination.DataNodeBase.Variables = source.DataNodeBase.Variables
	destination.DataNodeBase.Status = source.DataNodeBase.Status
	destination.PxcMaintMode = source.PxcMaintMode
	destination.PxcView = source.PxcView
	//destination.DataNodeBase.RetryUp = source.DataNodeBase.RetryUp
	//destination.DataNodeBase.RetryDown = source.DataNodeBase.RetryDown
	destination.DataNodeBase.Processed = source.DataNodeBase.Processed
	destination.setParameters()
	return destination
}

/*
This method is where we initiate the analysis of the nodes an the starting point of the population of the actionList
The actionList is the object returning the list of nodes that require modification
Any modification at their status in ProxySQL is done by the ProxySQLNode object
*/
func (cluster *DataCluster) GetActionList() map[string]DataNodePxc {
	if global.Performance {
		global.SetPerformanceObj("Get Action Map (DataCluster)", true, log.DebugLevel)
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
		global.SetPerformanceObj("Get Action Map (DataCluster)", false, log.DebugLevel)
	}

	//At this point we should be able to do actions in consistent way
	return cluster.ActionNodes
}

/*
We need to check if for any reasons we left some node suspended in the maintenance groups
this can happen if script is interrupted or maual action
*/
func (cluster *DataCluster) cleanUpForLeftOver() bool {
	//arrayOfMaps := [2]map[string]DataNodePxc{cluster.WriterNodes, cluster.ReaderNodes}
	for key, node := range cluster.WriterNodes {
		if _, ok := cluster.OffLineWriters[key]; ok {
			node.DataNodeBase.HostgroupId = cluster.HgWriterId + 9000
			node.DataNodeBase.ActionType = node.DataNodeBase.DELETE_NODE()
			cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.ActionType)+"_"+strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
			delete(cluster.OffLineWriters, key)
		}
	}
	for key, node := range cluster.ReaderNodes {
		if _, ok := cluster.OffLineReaders[key]; ok {
			node.DataNodeBase.HostgroupId = cluster.HgReaderId + 9000
			node.DataNodeBase.ActionType = node.DataNodeBase.DELETE_NODE()
			cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.ActionType)+"_"+strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
			delete(cluster.OffLineReaders, key)
		}
	}

	return true
}

//just check if we have identify failover node if not notify with HUGE alert
func (cluster *DataCluster) checkFailoverIfFound() bool {
	if cluster.RequireFailover &&
		len(cluster.WriterNodes) < 1 &&
		cluster.FailOverNode.DataNodeBase.HostgroupId == 0 {
		//Huge alert
		log.Error(fmt.Sprintf("!!!!!!!!!!!!!!! NO node Found For fail-over in the main segment %d you may want to use ActiveFailover = 2 if you have another node in a different segment  !!!!!!!!!!!!!",
			cluster.MainSegment))
		return false
	}

	return true

}

//align backup HGs
func (cluster *DataCluster) alignBackupNode(node DataNodePxc) {
	if _, ok := cluster.BackupWriters[node.DataNodeBase.Dns]; ok {
		cluster.BackupWriters[node.DataNodeBase.Dns] = cluster.alignNodeValues(cluster.BackupWriters[node.DataNodeBase.Dns], node)
	}
	if _, ok := cluster.BackupReaders[node.DataNodeBase.Dns]; ok {
		cluster.BackupReaders[node.DataNodeBase.Dns] = cluster.alignNodeValues(cluster.BackupReaders[node.DataNodeBase.Dns], node)
	}

}

// We can try to add back missed nodes (from bakcupHG) and se if they are coming back
func (cluster *DataCluster) checkMissingForNodes(evalMap map[string]DataNodePxc) map[string]DataNodePxc {

	//also if adding back nodes we will always try to add them back as readers and IF they pass then could become writers
	//we merge the two Maps in one to process all together
	arrayOfMaps := [2]map[string]DataNodePxc{cluster.BackupReaders, cluster.BackupWriters}

	for i := 0; i < len(arrayOfMaps); i++ {
		for _, node := range arrayOfMaps[i] {
			key1 := node.DataNodeBase.Dns
			if _, ok := evalMap[key1]; !ok {
				node.DataNodeBase.HostgroupId = cluster.HgReaderId
				node.DataNodeBase.NodeIsNew = true
				evalMap[node.DataNodeBase.Dns] = node
			}
		}
	}
	return evalMap
}

// we will review all the nodes keeping into account the status and hostgroups
func (cluster *DataCluster) evaluateAllProcessedNodes() bool {
	var arrayOfMaps = [4]map[string]DataNodePxc{cluster.WriterNodes, cluster.ReaderNodes, cluster.OffLineWriters, cluster.OffLineReaders}
	evalMap := MergeMaps(arrayOfMaps)

	if len(evalMap) > 0 {
		for key, node := range evalMap {
			//for key, node := range cluster.NodesPxc.internal {
			log.Debug("Evaluating node ", key)
			//Only nodes that were successfully processed (got status from query) are evaluated
			if node.DataNodeBase.Processed {
				cluster.evaluateNode(node)

			} else if node.DataNodeBase.ProxyStatus == "SHUNNED" &&
				node.DataNodeBase.HostgroupId < 8000 {
				//Any Shunned Node is moved to Special HG 9000
				if cluster.RetryDown > 0 {
					node.DataNodeBase.RetryDown++
				}
				node.DataNodeBase.ActionType = node.DataNodeBase.MOVE_DOWN_HG_CHANGE()
				cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
				//If is time for action and the node is part of the writers I will remove it from here so we can fail-over
				//if  _, ok := cluster.WriterNodes[node.DataNodeBase.Dns]; ok &&
				//	node.DataNodeBase.RetryDown >= cluster.RetryDown {
				//	delete(cluster.WriterNodes,node.DataNodeBase.Dns)
				//}
				log.Warning("Node: ", node.DataNodeBase.Dns, " ", node.WsrepNodeName, " HG: ", cluster.Hostgroups[node.DataNodeBase.HostgroupId].Id, " Type ", cluster.Hostgroups[node.DataNodeBase.HostgroupId].Type, " is im PROXYSQL state ", node.DataNodeBase.ProxyStatus,
					" moving it to HG ", node.DataNodeBase.HostgroupId+9000, " given SHUNNED")

			}
			// Align with 8000 HGs
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
func (cluster *DataCluster) evaluateNode(node DataNodePxc) DataNodePxc {
	if node.DataNodeBase.Processed {
		if node.DataNodeBase.HostgroupId == cluster.HgWriterId ||
			node.DataNodeBase.HostgroupId == cluster.HgReaderId ||
			node.DataNodeBase.HostgroupId == cluster.OffLineHgWriterId ||
			node.DataNodeBase.HostgroupId == cluster.OffLineHgReaderID {

			node.DataNodeBase.ActionType = node.DataNodeBase.NOTHING_TO_DO()
			currentHg := cluster.Hostgroups[node.DataNodeBase.HostgroupId]

			// Check for Demoting actions first
			//---------------------------------------
			//#Check major exclusions
			//# 1) wsrep state
			//# 2) Node is not read only
			//# 3) at least another node in the HG

			//ony node not in config HG or special 9000 will be processed
			if node.DataNodeBase.HostgroupId < 9000 {
				// desync
				if cluster.checkWsrepDesync(node, currentHg) {
					return node
				}

				// Node is in unsafe state we will move to maintenance group 9000
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
				When a new node is coming in we do not put it online directly, but instead we will insert it in the special group 9000
				this will allow us to process it correctly and validate the state.
				Then we will put online
			*/
			if cluster.checkBackNew(node) {
				return node
			}

			//# in the case node is not in one of the declared state
			//# BUT it has the counter retry set THEN I reset it to 0 whatever it was because
			//# I assume it is ok now
			//TODO this MUST be checked I suspect it will not be act right
			cluster.checkUpSaveRetry(node, currentHg)

		}
	}
	return node
}

func (cluster *DataCluster) checkBackOffline(node DataNodePxc, currentHg Hostgroup) (DataNodePxc, bool) {
	if node.DataNodeBase.HostgroupId < 8000 &&
		node.WsrepStatus == 4 &&
		node.DataNodeBase.ProxyStatus == "OFFLINE_SOFT" &&
		!node.WsrepRejectqueries &&
		node.WsrepClusterStatus == "Primary" &&
		node.PxcMaintMode == "DISABLED" {
		if node.DataNodeBase.HostgroupId == cluster.HgWriterId && node.DataNodeBase.ReadOnly {
			return node, true
		}
		if cluster.RetryUp > 0 {
			node.DataNodeBase.RetryUp++
		}
		node.DataNodeBase.ActionType = node.DataNodeBase.MOVE_UP_OFFLINE()
		cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
		log.Warning("Node: ", node.DataNodeBase.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " coming back ONLINE from previous OFFLINE_SOFT ")
	}
	return DataNodePxc{}, false
}

func (cluster *DataCluster) checkUpSaveRetry(node DataNodePxc, currentHg Hostgroup) bool {
	if node.DataNodeBase.ActionType == node.DataNodeBase.NOTHING_TO_DO() &&
		(node.DataNodeBase.RetryUp > 0 || node.DataNodeBase.RetryDown > 0) {
		node.DataNodeBase.RetryDown = 0
		node.DataNodeBase.RetryUp = 0
		node.DataNodeBase.ActionType = node.DataNodeBase.SAVE_RETRY()
		cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
		log.Info("Node: ", node.DataNodeBase.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " resetting the retry cunters to 0, all seems fine now")
		return true
	}
	return false
}

func (cluster *DataCluster) checkBackNew(node DataNodePxc) bool {
	if node.DataNodeBase.NodeIsNew &&
		node.DataNodeBase.HostgroupId < 9000 {
		node.DataNodeBase.HostgroupId = node.DataNodeBase.HostgroupId + 9000
		node.DataNodeBase.ActionType = node.DataNodeBase.INSERT_READ()
		cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
		log.Info(fmt.Sprintf("I am going to re-insert a declared node in Backup HGs that went missed in ProxySQL mysql_server table "))
		log.Info(fmt.Sprintf("Node %s will be first inserted in special HG %d Then if it status is fine will be promoted", node.DataNodeBase.Dns, node.DataNodeBase.HostgroupId))
		return true
	}
	return false
}

func (cluster *DataCluster) checkBackPrimary(node DataNodePxc, currentHg Hostgroup) bool {
	if node.DataNodeBase.HostgroupId >= 9000 &&
		node.WsrepStatus == 4 &&
		!node.WsrepRejectqueries &&
		node.WsrepClusterStatus == "Primary" {
		if cluster.RetryUp > 0 {
			node.DataNodeBase.RetryUp++
		}
		node.DataNodeBase.ActionType = node.DataNodeBase.MOVE_UP_HG_CHANGE()
		cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
		log.Warning("Node: ", node.DataNodeBase.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " coming back ONLINE from previous Offline Special Host Group ",
			node.DataNodeBase.HostgroupId)
		return true
	}
	return false
}

func (cluster *DataCluster) checkReadOnly(node DataNodePxc, currentHg Hostgroup) bool {
	if node.DataNodeBase.HostgroupId == cluster.HgWriterId &&
		node.DataNodeBase.ReadOnly {
		if cluster.RetryDown > 0 {
			node.DataNodeBase.RetryDown++
		}
		node.DataNodeBase.ActionType = node.DataNodeBase.MOVE_SWAP_WRITER_TO_READER()
		cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
		log.Warning("Node: ", node.DataNodeBase.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " has READ_ONLY ",
			"moving it to Reader HG ")
		return true
	}
	return false
}

func (cluster *DataCluster) checkPxcMaint(node DataNodePxc, currentHg Hostgroup) bool {
	if node.PxcMaintMode != "DISABLED" &&
		node.DataNodeBase.ProxyStatus != "OFFLINE_SOFT" &&
		node.DataNodeBase.HostgroupId < 8000 {
		node.DataNodeBase.ActionType = node.DataNodeBase.MOVE_DOWN_OFFLINE()
		//when we do not increment retry is because we want an immediate action like in this case. So let us set the retry to max.
		node.DataNodeBase.RetryDown = cluster.RetryDown + 1
		cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
		log.Warning("Node: ", node.DataNodeBase.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " has PXC_maint_mode as ", node.PxcMaintMode,
			" moving it to OFFLINE_SOFT ")
		return true
	}
	return false
}

func (cluster *DataCluster) checkDonorReject(node DataNodePxc, currentHg Hostgroup) bool {
	if node.WsrepDonorrejectqueries &&
		node.WsrepStatus == 2 &&
		cluster.Hostgroups[node.DataNodeBase.HostgroupId].Size > 1 &&
		node.DataNodeBase.HostgroupId < 8000 {
		if cluster.RetryDown > 0 {
			node.DataNodeBase.RetryDown++
		}
		node.DataNodeBase.ActionType = node.DataNodeBase.MOVE_DOWN_HG_CHANGE()
		cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
		log.Warning("Node: ", node.DataNodeBase.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " has WSREP Reject queries active ",
			"moving it to HG ", node.DataNodeBase.HostgroupId+9000)
		return true

	}
	return false
}

func (cluster *DataCluster) checkRejectQueries(node DataNodePxc, currentHg Hostgroup) bool {
	if node.WsrepRejectqueries &&
		node.DataNodeBase.HostgroupId < 8000 {
		if cluster.RetryDown > 0 {
			node.DataNodeBase.RetryDown++
		}
		node.DataNodeBase.ActionType = node.DataNodeBase.MOVE_DOWN_HG_CHANGE()
		cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
		log.Warning("Node: ", node.DataNodeBase.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " has WSREP Reject queries active ",
			"moving it to HG ", node.DataNodeBase.HostgroupId+9000)
		return true
	}
	return false
}

func (cluster *DataCluster) checkNotPrimary(node DataNodePxc, currentHg Hostgroup) bool {
	if node.WsrepClusterStatus != "Primary" {
		if cluster.RetryDown > 0 {
			node.DataNodeBase.RetryDown++
		}
		node.DataNodeBase.ActionType = node.DataNodeBase.MOVE_DOWN_HG_CHANGE()
		cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
		log.Warning("Node: ", node.DataNodeBase.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " is NOT in Primary state ",
			" moving it to HG ", node.DataNodeBase.HostgroupId+9000, " given unsafe node state")
		return true
	}
	return false
}

func (cluster *DataCluster) checkAnyNotReadyStatus(node DataNodePxc, currentHg Hostgroup) bool {
	if node.WsrepStatus != 2 &&
		node.WsrepStatus != 4 {
		//if cluster retry > 0 then we manage the node as well
		if cluster.RetryDown > 0 {
			node.DataNodeBase.RetryDown++
		}
		node.DataNodeBase.ActionType = node.DataNodeBase.MOVE_DOWN_HG_CHANGE()
		cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
		log.Warning("Node: ", node.DataNodeBase.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " is in state ", node.WsrepStatus,
			"moving it to HG ", node.DataNodeBase.HostgroupId+9000, " given unsafe node state")

		return true
	}
	return false
}

func (cluster *DataCluster) checkWsrepDesync(node DataNodePxc, currentHg Hostgroup) bool {
	if node.WsrepStatus == 2 &&
		!node.DataNodeBase.ReadOnly &&
		node.DataNodeBase.ProxyStatus != "OFFLINE_SOFT" {
		if currentHg.Size <= 1 {
			log.Warning("Node: ", node.DataNodeBase.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " is in state ", node.WsrepStatus,
				" But I will not move to OFFLINE_SOFT given last node left in the Host group")
			node.DataNodeBase.ActionType = node.DataNodeBase.NOTHING_TO_DO()
			//return node
		} else { //if cluster retry > 0 then we manage the node as well
			if cluster.RetryDown > 0 {
				node.DataNodeBase.RetryDown++
			}
			node.DataNodeBase.ActionType = node.DataNodeBase.MOVE_DOWN_OFFLINE()
			cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
			log.Warning("Node: ", node.DataNodeBase.Dns, " ", node.WsrepNodeName, " HG: ", currentHg.Id, " Type ", currentHg.Type, " is in state ", node.WsrepStatus,
				" moving it to OFFLINE_SOFT given we have other nodes in the Host group")
			return true
		}

	}
	return false
}
func (cluster *DataCluster) cleanWriters() bool {
	for key, node := range cluster.WriterNodes {
		if node.DataNodeBase.ProxyStatus != "ONLINE" {
			delete(cluster.WriterNodes, key)
			log.Debug(fmt.Sprintf("Node %s is not in ONLINE state in writer HG %d removing while evaluating", key, node.DataNodeBase.HostgroupId))
		}

	}
	if len(cluster.WriterNodes) < 1 {
		cluster.RequireFailover = true
		cluster.HasPrimary = false
	}
	return true
}

//Once we have done the identification of the node status in EvalNodes we can now process the nodes by ROLE. The main point here is First identify the Good possible writer(s)
func (cluster *DataCluster) evaluateWriters() bool {
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

	//check if in special HG 9000 and if so remove it from backupWriters
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
	if cluster.FailOverNode.DataNodeBase.HostgroupId != 0 &&
		cluster.FailOverNode.DataNodeBase.HostgroupId != cluster.HgWriterId+9000 &&
		cluster.RequireFailover {
		cluster.ActionNodes[strconv.Itoa(cluster.HgWriterId)+"_"+cluster.FailOverNode.DataNodeBase.Dns] = cluster.FailOverNode
		log.Warning(fmt.Sprintf("We can try to failover from Backup Writer HG : %s I will try to add it back", cluster.FailOverNode.DataNodeBase.Dns))
	}

	return true
}

func (cluster *DataCluster) processFailoverFailBack(backupWriters map[string]DataNodePxc) {
	for key, node := range backupWriters {
		// First of all we need to be sure node was tested
		if _, ok := cluster.NodesPxc.internal[node.DataNodeBase.Dns]; ok {

			//the backup node is not present we will try to add it
			if cluster.NodesPxc.internal[node.DataNodeBase.Dns].DataNodeBase.ProxyStatus == "ONLINE" &&
				!cluster.NodesPxc.internal[node.DataNodeBase.Dns].DataNodeBase.ReadOnly &&
				cluster.NodesPxc.internal[node.DataNodeBase.Dns].DataNodeBase.Processed {

				// in this case we just have to add the node given lower number of allowed writers. But only in the same segment
				if _, ok := cluster.WriterNodes[node.DataNodeBase.Dns]; !ok &&
					len(cluster.WriterNodes) < cluster.MaxNumWriters && cluster.MaxNumWriters > 1 &&
					node.WsrepSegment == cluster.MainSegment {
					node.DataNodeBase.HostgroupId = cluster.HgWriterId
					cluster.WriterNodes[node.DataNodeBase.Dns] = node
					node.DataNodeBase.ActionType = node.DataNodeBase.DELETE_NODE()
					cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.ActionType)+"_"+strconv.Itoa(cluster.HgWriterId)+"_"+node.DataNodeBase.Dns] = node
					node.DataNodeBase.ActionType = node.DataNodeBase.INSERT_WRITE()
					cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.ActionType)+"_"+strconv.Itoa(cluster.HgWriterId)+"_"+node.DataNodeBase.Dns] = node

					//if is a failover we will evaluate the node with the one already stored in case there is a writer coming UP from offline with higher weight
					// in that case we do not want an existing node to take over but we prefer to have directly the right node up
				} else if cluster.RequireFailover &&
					len(cluster.WriterNodes) < cluster.MaxNumWriters &&
					(node.WsrepSegment == cluster.MainSegment || cluster.ActiveFailover > 1) {
					if node.DataNodeBase.Weight > cluster.FailOverNode.DataNodeBase.Weight {
						node.DataNodeBase.ActionType = node.DataNodeBase.INSERT_WRITE()
						cluster.FailOverNode = node
						log.Warning(fmt.Sprintf("Failover require node identified as candidate: %s .", key))
					}

					// If we have exceeded the number of writers, the one with lower Weight will be removed
				} else if _, ok := cluster.WriterNodes[node.DataNodeBase.Dns]; ok &&
					len(cluster.WriterNodes) > cluster.MaxNumWriters &&
					node.DataNodeBase.ProxyStatus == "ONLINE" {
					lowerNode := node
					for _, wNode := range cluster.WriterNodes {
						if wNode.DataNodeBase.Weight < lowerNode.DataNodeBase.Weight &&
							wNode.DataNodeBase.Weight < node.DataNodeBase.Weight {
							lowerNode = wNode
						}
					}
					lowerNode.DataNodeBase.HostgroupId = cluster.HgWriterId
					lowerNode.DataNodeBase.ActionType = node.DataNodeBase.DELETE_NODE()
					if _, ok := cluster.ActionNodes[strconv.Itoa(cluster.HgWriterId)+"_"+lowerNode.DataNodeBase.Dns]; !ok {
						cluster.ActionNodes[strconv.Itoa(cluster.HgWriterId)+"_"+node.DataNodeBase.Dns] = lowerNode
					}

					// Now if we have failback and we have a writer with HIGHER weight coming back we need to identify the one with lower again and remove it
				} else if len(cluster.WriterNodes) == cluster.MaxNumWriters &&
					!cluster.RequireFailover &&
					cluster.FailBack {
					//we need to loop the writers
					for _, nodeB := range cluster.WriterNodes {
						if node.DataNodeBase.Weight > nodeB.DataNodeBase.Weight &&
							(node.WsrepSegment == cluster.MainSegment || cluster.ActiveFailover > 1) {
							node.DataNodeBase.ActionType = node.DataNodeBase.INSERT_WRITE()
							node.DataNodeBase.HostgroupId = cluster.HgWriterId

							// the node with lower weight is removed
							nodeB.DataNodeBase.RetryDown = cluster.RetryDown + 1
							nodeB.DataNodeBase.ActionType = nodeB.DataNodeBase.MOVE_DOWN_OFFLINE()
							cluster.ActionNodes[strconv.Itoa(cluster.HgWriterId)+"_"+node.DataNodeBase.Dns] = node
							cluster.ActionNodes[strconv.Itoa(cluster.HgWriterId)+"_"+nodeB.DataNodeBase.Dns] = nodeB

							//let also add it to the Writers to prevent double insertion
							cluster.WriterNodes[node.DataNodeBase.Dns] = node
							//remove failover flag from cluster
							cluster.RequireFailover = false

							log.Warn(fmt.Sprintf("Failback! Node %s is going down while Node %s is coming up as Writer ",
								strconv.Itoa(nodeB.DataNodeBase.HostgroupId)+"_"+nodeB.DataNodeBase.Dns,
								strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns))
							//if found we just exit no need to loop more
							break
						}
					}
				}
			}
		}
	}
}

func (cluster *DataCluster) processUpActionMap() {
	for key, node := range cluster.ActionNodes {
		//While evaluating the nodes that are coming up we also check if it is a Failover Node
		var hgI int
		var portI = 0
		currentHg := cluster.Hostgroups[node.DataNodeBase.HostgroupId]
		hg := key[0:strings.Index(key, "_")]
		ip := key[strings.Index(key, "_")+1 : strings.Index(key, ":")]
		port := key[strings.Index(key, ":")+1:]
		hgI = global.ToInt(hg)
		portI = global.ToInt(port)
		// We process only WRITERS
		if currentHg.Type == "W" || currentHg.Type == "WREC" {
			if node.DataNodeBase.ReturnActionCategory(node.DataNodeBase.ActionType) == "UP" ||
				node.DataNodeBase.ReturnActionCategory(node.DataNodeBase.ActionType) == "SWAP_W" {
				log.Debug(fmt.Sprintf("Evaluating for UP writer node key: %s %d %s %d Status: %s", key, hgI, ip, portI, node.DataNodeBase.ReturnTextFromCode(node.DataNodeBase.ActionType)))
				//if we have retry Down > 0 we must evaluate it
				if node.DataNodeBase.RetryUp >= cluster.RetryUp {
					// we remove from backup to prevent double insertion
					//delete(backupWriters,node.DataNodeBase.Dns)
					//check if we have already a primary or if we have already the max number of writers
					if len(cluster.WriterNodes) < cluster.MaxNumWriters ||
						(len(cluster.WriterNodes) < 1 || cluster.RequireFailover) {
						if cluster.RequireFailover &&
							!cluster.HasFailoverNode &&
							!cluster.HasPrimary {
							// we also check if the weight is higher to be sure we put the nodes in order
							if node.DataNodeBase.Weight > cluster.FailOverNode.DataNodeBase.Weight &&
								(node.WsrepSegment == cluster.MainSegment || cluster.ActiveFailover > 1) {
								if cluster.SinglePrimary {
									delete(cluster.ActionNodes, strconv.Itoa(cluster.FailOverNode.DataNodeBase.HostgroupId)+"_"+cluster.FailOverNode.DataNodeBase.Dns)
									delete(cluster.WriterNodes, cluster.FailOverNode.DataNodeBase.Dns)
								}
								cluster.FailOverNode = node
								cluster.WriterNodes[node.DataNodeBase.Dns] = node
								log.Warning(fmt.Sprintf("FAILOVER!!! Cluster may have identified a Node to failover: %s", key))
							}

						} else {
							cluster.WriterNodes[node.DataNodeBase.Dns] = node
							log.Warning(fmt.Sprintf("Node %s is coming UP in writer HG %d", key, cluster.HgWriterId))
						}
						//Failback is a pain in the xxxx because it can cause a lot of bad behaviour.
						//IF cluster failback is active we need to check for coming up nodes if our node has higher WEIGHT of current writer(s) and eventually act
					} else if _, ok := cluster.BackupWriters[node.DataNodeBase.Dns]; ok &&
						cluster.FailBack {

						//if node is already coming up, must be removed from current writers
						delete(cluster.BackupWriters, node.DataNodeBase.Dns)

						//tempWriters := make(map[string]DataNodePxc)
						lowerNode := node
						for _, wNode := range cluster.WriterNodes {
							if wNode.DataNodeBase.Weight < lowerNode.DataNodeBase.Weight &&
								wNode.DataNodeBase.Weight < node.DataNodeBase.Weight {
								lowerNode = wNode
							}
						}
						//IF the lower node IS NOT our new node then we have a FAIL BACK. But only if inside same segment OR if Active failover method allow the use of other segment
						//If instead our new node is the lowest .. no action and ignore it (for writers)
						if node.DataNodeBase.Dns != lowerNode.DataNodeBase.Dns &&
							(node.WsrepSegment == cluster.MainSegment || cluster.ActiveFailover > 1) {

							//in this case we need to set the action and also the retry or it will NOT go down consistently
							lowerNode.DataNodeBase.RetryDown = cluster.RetryDown + 1
							lowerNode.DataNodeBase.ActionType = lowerNode.DataNodeBase.MOVE_DOWN_OFFLINE()
							cluster.ActionNodes[strconv.Itoa(lowerNode.DataNodeBase.HostgroupId)+"_"+lowerNode.DataNodeBase.Dns] = lowerNode

							//we remove from the writerHG the node that is going down
							delete(cluster.WriterNodes, lowerNode.DataNodeBase.Dns)

							log.Warn(fmt.Sprintf("Failback! Node %s is going down while Node %s is coming up as Writer ",
								strconv.Itoa(lowerNode.DataNodeBase.HostgroupId)+"_"+lowerNode.DataNodeBase.Dns,
								strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns))

							//let also add it to the Writers to prevent double insertion
							cluster.WriterNodes[node.DataNodeBase.Dns] = node

							//Given this is a failover I will remove the failover flag if present
							cluster.RequireFailover = false

						} else if node.DataNodeBase.Dns == lowerNode.DataNodeBase.Dns &&
							(cluster.RequireFailover || cluster.FailBack) {
							log.Warn(fmt.Sprintf("Node %s coming back online but will not be promoted to Writer because lower Weight %d",
								strconv.Itoa(lowerNode.DataNodeBase.HostgroupId)+"_"+lowerNode.DataNodeBase.Dns,
								node.DataNodeBase.Weight))
							delete(cluster.ActionNodes, key)
						}
						//else{
						////	Node should not promote back but we cannot remove it. So we will notify in the log as ERROR to be sure is reported
						//	log.Error(fmt.Sprintf("Node %s is trying to come back as writer but it has lower WEIGHT respect to existing writers. Please MANUALLY remove it from Writer group %d",
						//		strconv.Itoa(lowerNode.DataNodeBase.HostgroupId) +"_" + lowerNode.DataNodeBase.Dns , cluster.HgWriterId))
						//	delete(cluster.ActionNodes,strconv.Itoa(lowerNode.DataNodeBase.HostgroupId) +"_" + lowerNode.DataNodeBase.Dns)
						//}
					} else if len(cluster.WriterNodes) < cluster.MaxNumWriters && !cluster.RequireFailover {
						log.Warning(fmt.Sprintf("Node %s is trying to come UP in writer HG: %d but cannot promote given we already have enough writers %d ", key, cluster.HgWriterId, len(cluster.WriterNodes)))
						delete(cluster.ActionNodes, key)

					} else {
						//In this case we cannot put back another writer and node must be removed from Actionlist
						log.Warning(fmt.Sprintf("Node %s is trying to come UP in writer HG: %d but cannot promote given we already have enough writers %d ", key, cluster.HgWriterId, len(cluster.WriterNodes)))
						delete(cluster.ActionNodes, key)
					}
				} else {
					log.Debug(fmt.Sprintf("Retry still low for UP writer node key: %s %d %s %d Status: %s Retry: %d",
						key, hgI, ip, portI, node.DataNodeBase.ReturnTextFromCode(node.DataNodeBase.ActionType), node.DataNodeBase.RetryUp))
				}
			}
		}
	}
}

func (cluster *DataCluster) processDownActionMap() {
	for key, node := range cluster.ActionNodes {
		var hgI int
		var portI = 0
		currentHg := cluster.Hostgroups[node.DataNodeBase.HostgroupId]
		hg := key[0:strings.Index(key, "_")]
		ip := key[strings.Index(key, "_")+1 : strings.Index(key, ":")]
		port := key[strings.Index(key, ":")+1:]
		hgI = global.ToInt(hg)
		portI = global.ToInt(port)

		// We process only WRITERS and check for nodes marked in EvalNodes as DOWN
		if currentHg.Type == "W" || currentHg.Type == "WREC" {
			//We must first check if the node is going down, because if it is single primary we probably need to failover
			if node.DataNodeBase.ReturnActionCategory(node.DataNodeBase.ActionType) == "DOWN" ||
				node.DataNodeBase.ReturnActionCategory(node.DataNodeBase.ActionType) == "SWAP_R" {
				log.Debug(fmt.Sprintf("Evaluating for DOWN writer node key: %s %d %s %d Status: %s", key, hgI, ip, portI, node.DataNodeBase.ReturnTextFromCode(node.DataNodeBase.ActionType)))
				//if we have retry Down > 0 we must evaluate it
				if node.DataNodeBase.RetryDown >= cluster.RetryDown {
					//check if we have one writer left and is this is the writer node
					if _, ok := cluster.WriterNodes[node.DataNodeBase.Dns]; ok {
						if len(cluster.WriterNodes) == 1 {
							//if we are here this means our Writer is going down and we need to failover
							cluster.RequireFailover = true
							cluster.HasFailoverNode = false
							cluster.HasPrimary = false
							cluster.Haswriter = false
							//I remove the node from writers and backup
							delete(cluster.BackupWriters, node.DataNodeBase.Dns)
							delete(cluster.WriterNodes, node.DataNodeBase.Dns)
							log.Warning(fmt.Sprintf("FAILOVER!!! Cluster Needs a new Writer to fail-over last writer is going down %s", key))
						} else if len(cluster.WriterNodes) > 1 {
							delete(cluster.WriterNodes, node.DataNodeBase.Dns)
							delete(cluster.BackupWriters, node.DataNodeBase.Dns)
						}
					}
				} else {
					log.Debug(fmt.Sprintf("Retry still low for Down writer node key: %s %d %s %d Status: %s Retry: %d",
						key, hgI, ip, portI, node.DataNodeBase.ReturnTextFromCode(node.DataNodeBase.ActionType), node.DataNodeBase.RetryDown))
				}
			}
		}
	}
}

/*
This method identify if we have an active reader and if not will force (no matter what) the writer to be a reader.
It will also remove the writer as reader is we have WriterIsAlsoReader <> 1 and reader group with at least 1 element

*/
func (cluster *DataCluster) evaluateReaders() bool {
	readerNodes := make(map[string]DataNodePxc)
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
		if okHgR := cluster.OffLineReaders[key]; okHgR.DataNodeBase.Dns != "" || (node.DataNodeBase.ReturnActionCategory(node.DataNodeBase.ActionType) == "NOTHING_TO_DO" &&
			cluster.ReaderNodes[node.DataNodeBase.Dns].DataNodeBase.ProxyStatus == "OFFLINE_SOFT") {
			delete(readerNodes, node.DataNodeBase.Dns)
		}
	}

	//Check for readers and see if we can add
	if len(readerNodes) <= 0 {
		if len(cluster.WriterNodes) > 0 {
			for _, node := range cluster.WriterNodes {
				if node.DataNodeBase.Processed && node.DataNodeBase.ProxyStatus == "ONLINE" {
					readerNodes[node.DataNodeBase.Dns] = node
				}
			}
		}
	} else {
		// in case we need to deal with multiple readers, we need to also deal with writeris also reade flag
		cluster.processWriterIsAlsoReader(readerNodes)
	}
	//whatever is now in the readNodes map should be pushed in offline read HG to be evaluated and move back in prod if OK
	for _, node := range readerNodes {
		key := node.DataNodeBase.Dns
		if okR := cluster.ReaderNodes[key]; okR.DataNodeBase.Dns != "" || !node.DataNodeBase.Processed {
			delete(readerNodes, key)
		} else {
			cluster.pushNewNode(node)
		}
	}

	return true
}

func (cluster *DataCluster) processWriterIsAlsoReader(readerNodes map[string]DataNodePxc) {
	// WriterIsAlsoReader != 1 so we need to check if writer is in reader group and remove it in the case we have more readers
	if cluster.WriterIsReader != 1 {

		if len(readerNodes) > 1 {
			for key, node := range readerNodes {
				//if the reader node is in the writer group and we have more than 1 reader good left, then we can remove the reader node
				if _, ok := cluster.WriterNodes[key]; ok &&
					node.DataNodeBase.HostgroupId == cluster.HgReaderId {
					delete(readerNodes, key)
					node.DataNodeBase.HostgroupId = cluster.HgReaderId
					node.DataNodeBase.ActionType = node.DataNodeBase.DELETE_NODE()
					cluster.ActionNodes[strconv.Itoa(cluster.HgReaderId)+"_"+node.DataNodeBase.Dns] = node
					//But if the reader node is from backup group this means we are just checkin if we should re-insert it so we do ot need to delete, but just remove it from the list
				} else if _, ok := cluster.WriterNodes[key]; ok &&
					node.DataNodeBase.HostgroupId == cluster.BackupHgReaderId {
					delete(readerNodes, key)

				}

			}
		} else if len(readerNodes) == 1 {
			//if we see that we have 1 item left in readers and this one is also the writer and is already present, we will NOT process it otherwise we will insert back
			for key, node := range readerNodes {
				if _, ok := cluster.WriterNodes[key]; ok && node.DataNodeBase.HostgroupId == cluster.HgReaderId {
					node.DataNodeBase.Processed = false
					readerNodes[key] = node
				}
			}
		}
	}
}

func (cluster *DataCluster) processUpAndDownReaders(actionNodes map[string]DataNodePxc, readerNodes map[string]DataNodePxc) {
	for _, actionNode := range actionNodes {
		currentHg := cluster.Hostgroups[actionNode.DataNodeBase.HostgroupId]
		if currentHg.Type == "R" || currentHg.Type == "RREC" {
			if actionNode.DataNodeBase.ReturnActionCategory(actionNode.DataNodeBase.ActionType) == "DOWN" ||
				actionNode.DataNodeBase.ReturnActionCategory(actionNode.DataNodeBase.ActionType) == "SWAP_W" {
				delete(readerNodes, actionNode.DataNodeBase.Dns)
				// If node is coming up we add it to the list of readers
			} else if actionNode.DataNodeBase.ReturnActionCategory(actionNode.DataNodeBase.ActionType) == "UP" ||
				actionNode.DataNodeBase.ReturnActionCategory(actionNode.DataNodeBase.ActionType) == "SWAP_R" {
				if actionNode.DataNodeBase.Processed && actionNode.DataNodeBase.ProxyStatus == "ONLINE" {
					actionNode.DataNodeBase.HostgroupId = cluster.HgReaderId
					actionNode.DataNodeBase.Processed = false
					readerNodes[actionNode.DataNodeBase.Dns] = actionNode
				}
			}
		}
	}
}

//add a new non existing Reader but force a delete first to avoid dirty writes. This inly IF a Offline node with taht key is NOT already present
func (cluster *DataCluster) pushNewNode(node DataNodePxc) bool {
	if ok := cluster.OffLineReaders[node.DataNodeBase.Dns]; ok.DataNodeBase.Dns != "" {
		return false
	}

	node.DataNodeBase.HostgroupId = cluster.OffLineHgReaderID
	node.DataNodeBase.ActionType = node.DataNodeBase.DELETE_NODE()
	node.DataNodeBase.NodeIsNew = true
	cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.ActionType)+"_"+strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node
	node.DataNodeBase.ActionType = node.DataNodeBase.INSERT_READ()
	cluster.ActionNodes[strconv.Itoa(node.DataNodeBase.ActionType)+"_"+strconv.Itoa(node.DataNodeBase.HostgroupId)+"_"+node.DataNodeBase.Dns] = node

	return true
}

// *** DATA NODE SECTION =============================================

/*this method is used to assign a connection to a proxySQL node
return true if successful in any other case false
*/
func (node *DataNode) GetConnection() bool {
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
				log.Fatal(err)
			}
			if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
				log.Fatal("Failed to append PEM.")
			}
			clientCert := make([]tls.Certificate, 0, 1)
			certs, err := tls.LoadX509KeyPair(client, key)
			if err != nil {
				log.Fatal(err)
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

	db, err := sql.Open("mysql", node.User+":"+node.Password+"@tcp("+node.Dns+")/performance_schema"+attributes)

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

func (node *DataNode) CloseConnection() bool {
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

func (node *DataNode) getNodeInternalInformation(dml string) map[string]string {
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

func (node *DataNode) getNodeInformations(what string) map[string]string {

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

func (node *DataNode) getRetry(writeHG int, readHG int) {
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
func (node *DataNode) NOTHING_TO_DO() int {
	return 0 // move a node from OFFLINE_SOFT
}
func (node *DataNode) MOVE_UP_OFFLINE() int {
	return 1000 // move a node from OFFLINE_SOFT
}
func (node *DataNode) MOVE_UP_HG_CHANGE() int {
	return 1010 // move a node from HG 9000 (plus hg id) to reader HG
}
func (node *DataNode) MOVE_DOWN_HG_CHANGE() int {
	return 3001 // move a node from original HG to maintenance HG (HG 9000 (plus hg id) ) kill all existing connections
}

func (node *DataNode) MOVE_DOWN_OFFLINE() int {
	return 3010 // move node to OFFLINE_soft keep existing connections, no new connections.
}
func (node *DataNode) MOVE_TO_MAINTENANCE() int {
	return 3020 // move node to OFFLINE_soft keep existing connections, no new connections because maintenance.
}
func (node *DataNode) MOVE_OUT_MAINTENANCE() int {
	return 3030 // move node to OFFLINE_soft keep existing connections, no new connections because maintenance.
}
func (node *DataNode) INSERT_READ() int {
	return 4010 // Insert a node in the reader host group
}
func (node *DataNode) INSERT_WRITE() int {
	return 4020 // Insert a node in the writer host group
}
func (node *DataNode) DELETE_NODE() int {
	return 5000 // this remove the node from the hostgroup
}
func (node *DataNode) MOVE_SWAP_READER_TO_WRITER() int {
	return 5001 // remove a node from HG reader group and add it to HG writer group
}
func (node *DataNode) MOVE_SWAP_WRITER_TO_READER() int {
	return 5101 // remove a node from HG writer group and add it to HG reader group
}
func (node *DataNode) SAVE_RETRY() int {
	return 9999 // this remove the node from the hostgroup
}

func (node *DataNode) ReturnTextFromCode(code int) string {
	switch code {
	case 0:
		return "NOTHING_TO_DO"
	case 1000:
		return "MOVE_UP_OFFLINE"
	case 1010:
		return "MOVE_UP_HG_CHANGE"
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
func (node *DataNode) ReturnActionCategory(code int) string {
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

// Sync Map
//=====================================
func NewRegularIntMap() *SyncMap {
	return &SyncMap{
		internal: make(map[string]DataNodePxc),
	}
}

func (rm *SyncMap) Load(key string) (value DataNodePxc, ok bool) {
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

func (rm *SyncMap) Store(key string, value DataNodePxc) {
	rm.Lock()
	defer rm.Unlock()
	rm.internal[key] = value

}

func (rm *SyncMap) ExposeMap() map[string]DataNodePxc {
	return rm.internal
}

//====================
//Generic
func MergeMaps(arrayOfMaps [4]map[string]DataNodePxc) map[string]DataNodePxc {
	mergedMap := make(map[string]DataNodePxc)

	for i := 0; i < len(arrayOfMaps); i++ {
		map1 := arrayOfMaps[i]
		for k, v := range map1 {
			hg := strconv.Itoa(v.DataNodeBase.HostgroupId) + "_"
			mergedMap[hg+k] = v
		}
	}
	return mergedMap

}

func CopyMap(mapDest map[string]DataNodePxc, mapSource map[string]DataNodePxc) map[string]DataNodePxc {

	for k, v := range mapSource {
		mapDest[k] = v
	}

	return mapDest

}
