package lib

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
	Dns string
	Hostgoups map[int]Hostgroup
	Ip string
	MonitorPassword string
	MonitorUser string
	Password string
	Port int
	User string
	Connection *sql.DB
}

type Hostgroup struct{
	Id int
	Size int
	Type string
	Nodes []DataNode
}

/*
ProxySQLNode methods
 */
func (node *ProxySQLNode) CheckTables(initTables bool) bool{


	return false
}

/*this method is used to assign a connection to a proxySQL node
return true if successful in any other case false
 */
func (node *ProxySQLNode) GetConnection() bool{
	db, err := sql.Open("mysql", node.User + ":" + node.Password + "@tcp(" + node.Dns + ":"+ "node.Port +)/main")
    node.Connection = db

	// if there is an error opening the connection, handle it
	if err != nil {
		panic(err.Error())
		return false
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