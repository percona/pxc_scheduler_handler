package lib


type DataNode struct{
	Comment string
	Compression string
	Connections int
	Debug bool
	Dns string
	Gtid_port int
	Hostgroups []Hostgroup
	Ip string
	Max_latency int
	Max_replication_lag int
	Name string
	Password string
	Port int
	Process_status int
	Proxy_status int
	Read_only bool
	Retry_down_saved int
	Retry_up_saved int
	Use_ssl bool
	User string
	Weight int
}


type DataCluster struct{
	BackupReaders []int
	BackupWriters []int
	CheckTimeout  int32
	ClusterIdentifier string //uuid
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
	Nodes map [string] DataNode // <ip_hg,datanode>
	NodesMaint []DataNode
	NumWriters int
	ReaderNodes []DataNode
	RequireFailover bool
	Singlenode bool
	SinglePrimary bool
	Size int
	Status  int
	WriterIsReader int
	WriterNodes []DataNode

}
