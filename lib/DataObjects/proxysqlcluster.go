package dataobjects

import (
	"strconv"

	log "github.com/sirupsen/logrus"

	"fmt"

	SQL "../Sql/Proxy"
)

type ProxySQLCluster interface {
	Nodes() map[string]ProxySQLNode
	FetchProxySQLNodes(ProxySQLNode)
}

func (cluster *proxySQLClusterImpl) Nodes() map[string]ProxySQLNode {
	return cluster.nodes
}

type proxySQLClusterImpl struct {
	name     string
	nodes    map[string]ProxySQLNode
	active   bool
	user     string
	password string
}

func NewProxySQLCluster(user string, password string) ProxySQLCluster {
	v := proxySQLClusterImpl{
		user:     user,
		password: password,
	}
	return &v
}

// FetchProxySQLNodes is responsible for fetching the list of ACTIVE ProxySQL servers.
// Nodes are collected as the ones visible by arbitrary ProxySQL cluster node provided
// as the parameter.
// Nodes available in ProxySQL cluster are accessible then in Nodes member of the object.
// Interestingly ProxySQL has not clue if a ProxySQL nodes ur down. Or at least is not reported in the proxysql_server tables or any stats table
// Given that we check if nodes are reachable opening a connection and closing it
func (cluster proxySQLClusterImpl) FetchProxySQLNodes(myNode ProxySQLNode) {
	cluster.nodes = make(map[string]ProxySQLNode)
	recordset, err := myNode.Connection().Query(SQL.Dml_select_proxy_servers)
	if err != nil {
		log.Error(err.Error())
	}

	for recordset.Next() {
		var weight int
		var hostname string
		var port int
		var comment string
		recordset.Scan(&weight, &hostname, &port, &comment)
		newNode := proxySQLNodeImpl{ // KH: todo: factory here
			ip:       hostname,
			weight:   weight,
			port:     port,
			comment:  comment,
			dns:      hostname + ":" + strconv.Itoa(port),
			user:     cluster.user,
			password: cluster.password,
		}

		// Given ProxySQL is NOT removing a non healthy node from proxySQL_cluster I must add a step here to check and eventually remove failing ProxySQL nodes
		if newNode.getConnection() {
			if newNode.Dns() != myNode.Dns() {
				newNode.CloseConnection()
			}
			cluster.nodes[newNode.Dns()] = &newNode
		} else {
			log.Error(fmt.Sprintf("ProxySQL Node %s is down or not reachable PLEASE REMOVE IT from the proxysql_servers table OR fix the issue", newNode.Dns()))
		}
	}
}
