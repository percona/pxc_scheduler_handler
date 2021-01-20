package DataObjects

import (
	"../Global"
	SQLPxc "../Sql/Pcx"
	SQLProxy "../Sql/Proxy"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

/*
Data cluster initialization method
 */
func (cluster *DataCluster) init(config Global.Configuration, connectionProxy *sql.DB ) bool{
	//set parameters from the config file
	if Global.Performance {
		Global.SetPerformanceValue("data_cluster_init",true)
	}

	cluster.Debug = config.Global.Debug
	cluster.ClusterIdentifier = config.Pxcluster.ClusterId
	cluster.CheckTimeout = config.Pxcluster.CheckTimeOut
	cluster.MainSegment = config.Pxcluster.MainSegment

	if config.Pxcluster.SslClient != "" && config.Pxcluster.SslKey != "" && config.Pxcluster.SslCa != "" {
		ssl := new(SslCertificates)
		ssl.sslClient = config.Pxcluster.SslClient
		ssl.sslKey = config.Pxcluster.SslKey
		ssl.sslCa = config.Pxcluster.SslCa
		if config.Pxcluster.SslCertificate_path !="" {
			ssl.sslCertificatePath = config.Pxcluster.SslCertificate_path
		}else{
			path, err := os.Getwd()
			if err != nil {
				log.Error(err.Error())
				os.Exit(1)
			}
			ssl.sslCertificatePath = path
		}
		cluster.Ssl = ssl
	}
	//cluster.MaxNumWriters = config.Pxcluster.MaxNumWriters
	//cluster.SinglePrimary = config.Pxcluster.SinglePrimary
	//cluster.WriterIsReader = config.Pxcluster.WriterIsReader

	//set parameters from the disk.pxc_cluster stable
	if ! cluster.getParametersFromProxySQL(connectionProxy){
		log.Error("Cannot retrieve information from disk.pxc_clusters for cluster ", cluster.ClusterIdentifier )
		return false
	}


	if ! cluster.loadNodes(connectionProxy){
		log.Error("Cannot retrieve information from disk.pxc_clusters for cluster ", cluster.ClusterIdentifier )

		return false
	}
	if ! cluster.getNodesInfo(){
		log.Error("Cannot retrieve information from MySQL nodes ", cluster.ClusterIdentifier )
		return false
	}else{
		if ! cluster.consolidateNodes(){
			log.Error("Node Consolidation failed ", cluster.ClusterIdentifier )
			return false
		}

	}


	if Global.Performance {
		Global.SetPerformanceValue("data_cluster_init",false)
	}
	return true
}

//this method is used to parallelize the information retrieval from the datanodes.
// We will use the Nodes list with all the IP:Port pair no matter what HG to check the nodes and then will assign the information to the relevant node collection
// like Bkup(r/w) or Readers/Writers
func (cluster *DataCluster) getNodesInfo() bool{
	var waitingGroup  Global.MyWaitGroup

	for key, node := range cluster.NodesPxc.ExposeMap() {
		waitingGroup.IncreaseCounter()
		go  node.getInformation(&waitingGroup,cluster)

		if log.GetLevel() == log.DebugLevel{
			log.Debug("Retrieving information from node: ",key)
		}
	}
	start := time.Now().UnixNano()
	for i := 0; i < cluster.CheckTimeout; i++{
		time.Sleep(1 *time.Millisecond)

		if waitingGroup.ReportCounter() == 0 {
			break
		}
//		log.Debug("wait ", i)
	}
	end :=time.Now().UnixNano()
	log.Debug("time taken :" ,(end - start)/1000000, " checkTimeOut : ",cluster.CheckTimeout)
	return true
}

//We parallelize the information retrival using goroutine
func (mysqlNode DataNodePxc) getInformation(wg  *Global.MyWaitGroup,cluster *DataCluster) int{
		//time.Sleep(100 *time.Millisecond)
		mysqlNode.DataNodeBase.GetConnection()
		/*
		if connection is functioning we try to get the info
		Otherwise we go on and set node as NOT processed
		 */

		if ! mysqlNode.DataNodeBase.NodeTCPDown {
			mysqlNode.DataNodeBase.Variables = mysqlNode.DataNodeBase.getNodeInformations("variables")
			mysqlNode.DataNodeBase.Status =  mysqlNode.DataNodeBase.getNodeInformations("status")
			if mysqlNode.DataNodeBase.Variables["server_uuid"] != ""{
				mysqlNode.PxcView = mysqlNode.getPxcView(strings.ReplaceAll( SQLPxc.Dml_get_pxc_view,"?",mysqlNode.DataNodeBase.Status["wsrep_gcomm_uuid"]))
			}

			mysqlNode.DataNodeBase.Processed = true

			//set the specific monitoring parameters
			mysqlNode.setParameters()

		}else{
			mysqlNode.DataNodeBase.Processed = false
			log.Warn("Cannot load information (variables/status/pxc_view) for node: ",mysqlNode.DataNodeBase.Dns)
		}
		/*
		TODO Alert also if I have implemented the mutex, I still have issue with Panic because ma contention.
		Probably have to check the use of sync.Map
		 */
		cluster.NodesPxc.Store(mysqlNode.DataNodeBase.Dns,mysqlNode)
		log.Debug("node ", mysqlNode.DataNodeBase.Dns, " done")
		mysqlNode.DataNodeBase.CloseConnection()
		wg.DecreaseCounter()
		return 0
}

func (node *DataNodePxc) setParameters() {
	node.Wsrep_local_index = node.PxcView.LocalIndex
	node.Pxc_maint_mode = node.DataNodeBase.Variables["pxc_maint_mode"]
	node.Wsrep_connected = Global.ToBool(node.DataNodeBase.Status["wsrep_connected"], "ON")
    node.Wsrep_desinccount = Global.ToInt(node.DataNodeBase.Status["wsrep_desync_count"])
	node.Wsrep_donorrejectqueries = Global.ToBool(node.DataNodeBase.Variables["wsrep_sst_donor_rejects_queries"],"OFF")
	node.Wsrep_gcomm_uuid = node.DataNodeBase.Status["wsrep_gcomm_uuid"]
	node.Wsrep_provider = Global.FromStringToMAp(node.DataNodeBase.Variables["wsrep_provider_options"],";")

	node.Wsrep_pc_weight =   Global.ToInt(node.Wsrep_provider["pc.weight"])
	node.Wsrep_ready = Global.ToBool(node.DataNodeBase.Status["wsrep_ready"],"on")
	node.Wsrep_rejectqueries = Global.ToBool(node.DataNodeBase.Status["wsrep_reject_queries"],"none")
	node.Wsrep_segment = Global.ToInt(node.Wsrep_provider["gmcast.segment"])
	node.Wsrep_status = Global.ToInt( node.DataNodeBase.Status["wsrep_local_state"])
	node.DataNodeBase.ReadOnly= Global.ToBool( node.DataNodeBase.Variables["read_only"],"on")

}

/*
This functions get the nodes list from the proxysql table mysql_servers for the given HGs and check their conditions
Ony one test for IP:port is executed and status shared across HGs
In debug-dev mode information is retrieved sequentially.
In prod is parallelized

 */
func (cluster *DataCluster) loadNodes(connectionProxy *sql.DB) bool{
    // get list of nodes from ProxySQL
	if Global.Performance {
		Global.SetPerformanceValue("loadNodes",true)
	}
	var sb strings.Builder
	sb.WriteString(strconv.Itoa(cluster.HgWriterId) )
	sb.WriteString("," +strconv.Itoa(cluster.HgReaderId) )
	sb.WriteString("," +strconv.Itoa(cluster.BakcupHgWriterId) )
	sb.WriteString("," +strconv.Itoa(cluster.BackupHgReaderId) )
	sb.WriteString("," +strconv.Itoa(cluster.OffLineHgWriterId) )
	sb.WriteString("," +strconv.Itoa(cluster.OffLineHgReaderID) )

	cluster.ActionNodes = make(map[string]DataNodePxc)
	cluster.NodesPxc = NewRegularIntMap()//make(map[string]DataNodePxc)
	cluster.BackupWriters = make(map[string]DataNodePxc)
	cluster.BackupReaders = make(map[string]DataNodePxc)
	cluster.WriterNodes = make(map[string]DataNodePxc)
	cluster.ReaderNodes = make(map[string]DataNodePxc)
	cluster.OffLineWriters = make(map[string]DataNodePxc)
	cluster.OffLineReaders = make(map[string]DataNodePxc)

	sqlCommand := strings.ReplaceAll(SQLProxy.Dml_Select_mysql_nodes,"?",sb.String())
	recordset, err  := connectionProxy.Query(sqlCommand)
	sb.Reset()

	if err != nil{
		log.Error(err.Error())
		os.Exit(1)
	}
	//select hostgroup_id, hostname,port,gtid_port, status,weight, compression,max_connections, max_replication_lag,use_ssl,max_latency_ms,comment
	for recordset.Next() {
		var myNode DataNodePxc
		recordset.Scan(&myNode.DataNodeBase.HostgroupId,
			&myNode.DataNodeBase.Ip,
			&myNode.DataNodeBase.Port,
			&myNode.DataNodeBase.Gtid_port,
			&myNode.DataNodeBase.ProxyStatus,
			&myNode.DataNodeBase.Weight,
			&myNode.DataNodeBase.Compression,
			&myNode.DataNodeBase.MaxConnection,
			&myNode.DataNodeBase.MaxReplication_lag,
			&myNode.DataNodeBase.UseSsl,
			&myNode.DataNodeBase.MaxLatency,
			&myNode.DataNodeBase.Comment,
			&myNode.DataNodeBase.ConnUsed)
		myNode.DataNodeBase.User = cluster.MonitorUser
		myNode.DataNodeBase.Password = cluster.MonitorPassword
		myNode.DataNodeBase.Dns = myNode.DataNodeBase.Ip + ":" + strconv.Itoa(myNode.DataNodeBase.Port)
		if len(myNode.DataNodeBase.Comment) >0 {
			myNode.DataNodeBase.getRetry(cluster.HgWriterId ,cluster.HgReaderId)
		}

		//Load ssl object to node if present in cluster/config
		if cluster.Ssl != nil {
			myNode.DataNodeBase.Ssl=cluster.Ssl
		}

		switch myNode.DataNodeBase.HostgroupId {
		case cluster.HgWriterId:
				cluster.WriterNodes[ myNode.DataNodeBase.Dns ]=myNode
		case cluster.HgReaderId:
				cluster.ReaderNodes[ myNode.DataNodeBase.Dns ]=myNode
		case cluster.BakcupHgWriterId:
			    cluster.BackupWriters[myNode.DataNodeBase.Dns ]=myNode
		case cluster.BackupHgReaderId:
				cluster.BackupReaders[myNode.DataNodeBase.Dns ]=myNode
		case cluster.OffLineHgWriterId:
				cluster.OffLineWriters[myNode.DataNodeBase.Dns ]=myNode
		case cluster.OffLineHgReaderID:
				cluster.OffLineReaders[myNode.DataNodeBase.Dns ]=myNode
		}

		/*
		We add only the real servers in the list to check with DB access
		we include all the HG operating like Write/Read and relevant OFFLINE special HG
		 */
		if _, ok := cluster.NodesPxc.ExposeMap()[myNode.DataNodeBase.Dns ] ; !ok{
			if myNode.DataNodeBase.HostgroupId == cluster.HgWriterId ||
							myNode.DataNodeBase.HostgroupId == cluster.HgReaderId ||
							myNode.DataNodeBase.HostgroupId == cluster.OffLineHgWriterId ||
							myNode.DataNodeBase.HostgroupId == cluster.OffLineHgReaderID {
				cluster.NodesPxc.Store(myNode.DataNodeBase.Dns , myNode)
			}
		}


	}
	if Global.Performance {
		Global.SetPerformanceValue("loadNodes",false)
	}
	return true
}

//load values from db disk in ProxySQL
func (cluster *DataCluster) getParametersFromProxySQL(connectionProxy *sql.DB ) bool{

	sqlCommand := strings.ReplaceAll(SQLProxy.Dml_get_mysql_cluster_to_manage,"?",strconv.Itoa(cluster.ClusterIdentifier))
	recordset, err  := connectionProxy.Query(sqlCommand)

	if err != nil{
		log.Error(err.Error())
		os.Exit(1)
	}
	//elect cluster_id, hg_w, hg_r, bck_hg_w, bck_hg_r, single_writer, max_writers, writer_is_also_reader, retry_up, retry_down
	for recordset.Next() {
		recordset.Scan(&cluster.ClusterIdentifier,
						&cluster.HgWriterId,
						&cluster.HgReaderId,
						&cluster.BakcupHgWriterId,
						&cluster.BackupHgReaderId,
						&cluster.SinglePrimary,
						&cluster.MaxNumWriters,
						&cluster.WriterIsReader,
						&cluster.RetryUp,
						&cluster.RetryDown)
		if log.GetLevel() == log.DebugLevel {
			log.Debug("Cluster arguments ", " clusterid=",cluster.ClusterIdentifier,
				" hg_w:",cluster.HgWriterId,
				" hg_r:",cluster.HgReaderId,
				" bckhg_w:",cluster.BakcupHgWriterId,
				" bckhg_w:",cluster.BackupHgReaderId,
				" singlePrimary:",cluster.SinglePrimary,
				" num_writers:",cluster.MaxNumWriters,
				" writer_is_also_r:",cluster.WriterIsReader,
				" retry_up:",cluster.RetryUp,
				" retry_down:",cluster.RetryDown,
				" check_timeout:",cluster.CheckTimeout,
				" main_segment:",cluster.MainSegment)
		}
		cluster.OffLineHgReaderID = 9000 + cluster.HgReaderId
		cluster.OffLineHgWriterId = 9000 + cluster.HgWriterId
		return true
	}

	return false
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
		if _, ok := cluster.WriterNodes[key]; ok {
			cluster.WriterNodes[key]=cluster.alignNodeValues(cluster.WriterNodes[key],node)
		}
		if _, ok := cluster.ReaderNodes[key]; ok {
			cluster.ReaderNodes[key] = cluster.alignNodeValues(cluster.ReaderNodes[key],node)
		}
		if _, ok := cluster.BackupWriters[key]; ok {
			cluster.BackupWriters[key] = cluster.alignNodeValues(cluster.BackupWriters[key],node)

		}
		if _, ok := cluster.BackupReaders[key]; ok {
			cluster.BackupReaders[key] = cluster.alignNodeValues(cluster.BackupReaders[key] ,node)
		}
		if _, ok := cluster.OffLineWriters[key]; ok {
			cluster.OffLineWriters[key] = cluster.alignNodeValues(cluster.OffLineWriters[key] ,node)
		}
		if _, ok := cluster.OffLineReaders[key]; ok {
			cluster.OffLineReaders[key] = cluster.alignNodeValues(cluster.OffLineReaders[key] ,node)
		}


	}

	return true
}
// We align only the relevant information, not all the node
func (cluster *DataCluster) alignNodeValues(nodeA DataNodePxc, nodeB DataNodePxc)  DataNodePxc{
	nodeA.DataNodeBase.Variables = nodeB.DataNodeBase.Variables
	nodeA.DataNodeBase.Status = nodeB.DataNodeBase.Status
	nodeA.Pxc_maint_mode = nodeB.Pxc_maint_mode
	nodeA.PxcView = nodeB.PxcView
	nodeA.DataNodeBase.RetryUp = nodeB.DataNodeBase.RetryUp
	nodeA.DataNodeBase.RetryDown = nodeB.DataNodeBase.RetryDown
	nodeA.DataNodeBase.Processed = nodeB.DataNodeBase.Processed
	nodeA.setParameters()
	return nodeA
}

/*
This method is where we initiate the analysis of the nodes an the starting point of the population of the actionList
The actionList is the object returning the list of nodes that require modification
Any modification at their status in ProxySQL is done by the ProxySQLNode object
 */
func (cluster *DataCluster) GetActionList() map[string]DataNode {

	/*
	TODO:
		steps:
		1 check if we have a Primary writer
		2 check if we have a Primary state
		3 check if anything prevent the writer to remain such

		4 check for readers state
	 */

	cluster.Haswriter = cluster.getPrimaryWriter()

	return nil
}

func (cluster *DataCluster) getPrimaryWriter() bool{
	if len(cluster.WriterNodes) > 0 {
		for key, node := range cluster.WriterNodes {
			log.Debug("Evaluating writer node ", key )
			cluster.evaluateNode(node)
		}
	}

	return false
}

func (cluster *DataCluster) evaluateNode(node DataNodePxc) DataNodePxc{
	if node.DataNodeBase.Processed  {
		if node.DataNodeBase.HostgroupId == cluster.HgWriterId ||
			node.DataNodeBase.HostgroupId == cluster.HgReaderId ||
			node.DataNodeBase.HostgroupId == cluster.OffLineHgWriterId ||
			node.DataNodeBase.HostgroupId == cluster.OffLineHgReaderID {
			
		}
	}
	return node
}

// *** DATA NODE SECTION =============================================


/*this method is used to assign a connection to a proxySQL node
return true if successful in any other case false
*/
func (node *DataNode) GetConnection() bool{
	if Global.Performance {
		Global.SetPerformanceValue("node_connection_" + node.Dns,true)
	}
	//dns := node.User + ":" + node.Password + "@tcp(" + node.Dns + ":"+ strconv.Itoa(node.Port) +")/admin" //
	//if log.GetLevel() == log.DebugLevel {log.Debug(dns)}

	// TODO compile dns taking into account ssl
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
	attributes :="?timeout=1s"

	if node.UseSsl {
		if node.Ssl == nil {
			attributes = attributes + "&tls=skip-verify"
		}else if node.Ssl.sslCertificatePath !=""{
			ca :=  node.Ssl.sslCertificatePath + Global.Separator + node.Ssl.sslCa
			client := node.Ssl.sslCertificatePath + Global.Separator +node.Ssl.sslClient
			key := node.Ssl.sslCertificatePath + Global.Separator + node.Ssl.sslKey

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
				RootCAs: rootCertPool,
				Certificates: clientCert,
			})
			//attributes = attributes + "&tls=custom"
			attributes = attributes + "&tls=skip-verify"
		}
	}

	db, err := sql.Open("mysql", node.User + ":" + node.Password + "@tcp(" + node.Dns  +")/performance_schema"+attributes)

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
	// TODO remove this block
	//if log.GetLevel() == log.DebugLevel{
	//	recordset, err  := db.Query(SQLPxc.Dml_get_ssl_status)
	//
	//	if err != nil{
	//		log.Error(err.Error())
	//		os.Exit(1)
	//	}
	//
	//	var nameVar string
	//	var valueVar string
	//	for recordset.Next() {
	//		recordset.Scan(&nameVar,&valueVar)
	//	}
	//	log.Debug("Connection is using cert: ",valueVar)
	//}

	if Global.Performance {
		Global.SetPerformanceValue("node_connection_" + node.Dns,false)
	}
	return true
}
/*this method is call to close the connection to a proxysql node
return true if successful in any other case false
*/

func (node *DataNode) CloseConnection() bool{
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

func (node *DataNode) getNodeInternalInformation(dml string) map[string]string{
	recordset, err  := node.Connection.Query(dml)
	if err != nil{
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

func (node *DataNode) getNodeInformations(what string) map[string]string{

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



//=====================================
func NewRegularIntMap() *ProxySyncMap {
	return &ProxySyncMap{
		internal: make(map[string]DataNodePxc),
	}
}

func (rm *ProxySyncMap) Load(key string) (value DataNodePxc, ok bool) {
	rm.RLock()
	result, ok := rm.internal[key]
	rm.RUnlock()
	return result, ok
}

func (rm *ProxySyncMap) Delete(key string) {
	rm.Lock()
	delete(rm.internal, key)
	rm.Unlock()
}

func (rm *ProxySyncMap) Store(key string, value DataNodePxc) {
	rm.Lock()
	rm.internal[key] = value
	rm.Unlock()
}

func (rm *ProxySyncMap) ExposeMap() map[string]DataNodePxc{
	return rm.internal
}

func (node *DataNode) getRetry(writeHG int, readHG int ) {
	 //var valueUp string
	 comment := node.Comment
     hgRetry := strconv.Itoa(writeHG) + "_W_" + strconv.Itoa(readHG) + "_R_retry_"
     regEUp := regexp.MustCompile( hgRetry + "up=\\d;")
	 regEDw := regexp.MustCompile( hgRetry + "down=\\d;")
	 up := regEUp.FindAllString(comment, -1)
	 down := regEDw.FindAllString(comment,-1)
	 if len(up) > 0 {
	 	comment = strings.ReplaceAll(comment, up[0],"")
	 	value, err :=  strconv.Atoi(up[0][strings.Index(up[0],"=")+1:len(up[0])-1])
	 	if err == nil {
			log.Debug(" Retry up = ", value)
			node.RetryUp = value
		}
	 }
	if len(down) > 0 {
		comment = strings.ReplaceAll(comment, down[0],"")
		value, err :=  strconv.Atoi(down[0][strings.Index(down[0],"=")+1:len(down[0])-1])
		if err == nil {
			log.Debug(" Retry up = ", value)
			node.RetryDown = value
		}
	}
	node.Comment = comment
}

/*
NODE CONSTANT declaration by methods
 */
func (node *DataNode) NOTHING_TO_DO () int{
	return 0 // move a node from OFFLINE_SOFT
}
func (node *DataNode) MOVE_UP_OFFLINE () int{
	return 1000 // move a node from OFFLINE_SOFT
}
func (node *DataNode) MOVE_UP_HG_CHANGE () int{
	return 1010 // move a node from HG 9000 (plus hg id) to reader HG
}
func (node *DataNode) MOVE_DOWN_HG_CHANGE () int{
	return 3001 // move a node from original HG to maintenance HG (HG 9000 (plus hg id) ) kill all existing connections
}

func (node *DataNode) MOVE_DOWN_OFFLINE () int{
	return 3010 // move node to OFFLINE_soft keep existing connections, no new connections.
}
func (node *DataNode) MOVE_TO_MAINTENANCE () int{
	return 3020 // move node to OFFLINE_soft keep existing connections, no new connections because maintenance.
}
func (node *DataNode) MOVE_OUT_MAINTENANCE () int{
	return 3030 // move node to OFFLINE_soft keep existing connections, no new connections because maintenance.
}
func (node *DataNode) INSERT_READ () int{
	return 4010 // Insert a node in the reader host group
}
func (node *DataNode) INSERT_WRITE () int{
	return 4020 // Insert a node in the writer host group
}
func (node *DataNode) DELETE_NODE () int{
	return 5000 // this remove the node from the hostgroup
}
func (node *DataNode) MOVE_SWAP_READER_TO_WRITER () int{
	return 5001 // remove a node from HG reader group and add it to HG writer group
}
func (node *DataNode) MOVE_SWAP_WRITER_TO_READER () int{
	return 5001 // remove a node from HG writer group and add it to HG reader group
}
func (node *DataNode) SAVE_RETRY () int{
	return 9999 // this remove the node from the hostgroup
}
