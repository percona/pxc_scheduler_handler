package lib

type ProxySQLCluster struct {
	Name string
	Nodes map[string]ProxySQLNode
	Active bool
	User string
	Password string
}

type ProxySQLNode struct{
	Dns string
	Hostgoups map[int]Hostgroup
	Ip string
	MonitorPassword string
	MonitorUser string
	Password string
	Port int
	User string
}

type Hostgroup struct{
	Id int
	Size int
	Type string
	Nodes []DataNode
}

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

type DataNodePxc struct{
	Pxc_maint_mode string
	Wsrep_connected bool
	Wsrep_desinccount int
	Wsrep_donorrejectqueries bool
	Wsrep_gcomm_uuid string
	Wsrep_local_index int
	Wsrep_pc_weight int
	Wsrep_provider map[string]string
	Wsrep_ready  bool
	Wsrep_rejectqueries bool
	Wsrep_segment int
	Wsrep_status int
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
