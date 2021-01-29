package DataObjects

import (
	"database/sql"

)
/*
Cluster object and methods
 */
type ProxySQLCluster struct {
	Name string
	Nodes map[string]ProxySQLNode
	Active bool
	User string
	Password string
}

func (cluster ProxySQLCluster) GetProxySQLnodes() []ProxySQLNode {
	nodes := []ProxySQLNode{}

	return nodes
}



/*
ProxySQL Node
 */

type ProxySQLNode struct{
	ActionNodeList map[string]DataNodePxc
	Dns string
	Hostgoups map[int]Hostgroup
	Ip string
	MonitorPassword string
	MonitorUser string
	Password string
	Port int
	User string
	Connection *sql.DB
	MySQLCluster *DataCluster
	Variables map[string]string

}

type Hostgroup struct{
	Id int
	Size int
	Type string
	Nodes []DataNode
}

const(
	PxcTables = "pxc_servers_original,pxc_servers_scheduler,pxc_clusters"

)

