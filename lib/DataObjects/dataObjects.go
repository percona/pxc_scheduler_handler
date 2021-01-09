package DataObjects

import (
	"database/sql"
	"sync"
)

type DataNode struct{
	Comment string
	Compression int
	Connection *sql.DB
	Debug bool
	Dns string
	Gtid_port int
	HostgroupId int
	Hostgroups []Hostgroup
	Ip string
	MaxConnection int
	MaxLatency int
	MaxReplication_lag int
	Name string
	NodeTCPDown bool
	Password string
	Port int
	Processed bool
	ProcessStatus int
	ProxyStatus string
	ReadOnly bool
	Ssl *SslCertificates
	Status map[string]string
	UseSsl bool
	User string
	Variables map[string]string
	Weight int
}


type DataCluster struct{
	BackupReaders map[string]DataNode
	BackupWriters map[string]DataNode
	BackupHgReaderId int
	BakcupHgWriterId int
	CheckTimeout  int
	ClusterIdentifier int //cluster_id
	Cluster_size int
	Cluster_status int
	Clustername string
	Comment string
	Debug  bool
	HasFailoverNode bool
	Haswriter bool
	HgReaderId int
	HgWriterId int
	Hostgroups map[int]Hostgroup
	//	Hosts map[string] DataNode
	MainSegment int
	MonitorPassword string
	MonitorUser string
	Name string
	NodesPxc *ProxySyncMap //[string] DataNodePxc // <ip:port,datanode>
	NodesPxcMaint []DataNodePxc
	MaxNumWriters int
	ReaderNodes map[string]DataNodePxc
	RequireFailover bool
	RetryDown int
	RetryUp int
	Singlenode bool
	SinglePrimary bool
	Size int
	Ssl *SslCertificates
	Status  int
	WriterIsReader int
	WriterNodes map[string]DataNodePxc

}

type ProxySyncMap struct {
	sync.RWMutex
	internal map[string]DataNodePxc
}

type SslCertificates struct {
	sslClient string
	sslKey string
	sslCa string
	sslCertificatePath string
}

type PxcClusterView struct {
	//'HOST_NAME', 'UUID','STATUS','LOCAL_INDEX','SEGMENT'
	HostName string
	Uuid string
	Status string
	LocalIndex int
	Segment int
}


type VariableStatus struct{
	VarName string `db:"VARIABLE_NAME"`
	VarValue string `db:"VARIABLE_VALUE"`
}