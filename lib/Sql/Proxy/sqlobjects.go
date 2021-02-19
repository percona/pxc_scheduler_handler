package Proxy

/*
Class dealing with ALL SQL commands.
No SQL should be hardcoded in the any other class
 */


const(
	//get MySQL nodes to process based on the HG
	//+--------------+---------------+------+-----------+--------------+---------+-------------+-----------------+---------------------+---------+----------------+-------------------------------------------------------------|---------+
	//| hostgroup_id | hostname      | port | gtid_port | status       | weight  | compression | max_connections | max_replication_lag | use_ssl | max_latency_ms | comment                                                     |ConnUsed|
	Dml_Select_mysql_nodes = " select b.*, c.ConnUsed from stats_mysql_connection_pool c left JOIN runtime_mysql_servers b ON  c.hostgroup=b.hostgroup_id and c.srv_host=b.hostname and c.srv_port = b.port where hostgroup_id in (?)  order by hostgroup,srv_host desc;"

      //get Variables
    Dml_show_variables="SHOW GLOBAL VARIABLES"

    Dml_select_proxy_servers = "select weight,hostname,port,comment from runtime_proxysql_servers order by weight desc"
)