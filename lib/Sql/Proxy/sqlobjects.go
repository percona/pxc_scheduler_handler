package Proxy

/*
Class dealing with ALL SQL commands.
No SQL should be hardcoded in the any other class
 */


const(
	/*
	Table used as backup / point in time recovery before scheduler start to take control
	 */

	Ddl_Create_mysal_server_original = "CREATE TABLE disk.pxc_servers_original (" +
		"hostgroup_id INT CHECK (hostgroup_id>=0) NOT NULL DEFAULT 0, " +
		"hostname VARCHAR NOT NULL, " +
		"port INT CHECK (port >= 0 AND port <= 65535) NOT NULL DEFAULT 3306, " +
		"gtid_port INT CHECK ((gtid_port <> port OR gtid_port=0) AND gtid_port >= 0 AND gtid_port <= 65535) NOT NULL DEFAULT 0," +
		"status VARCHAR CHECK (UPPER(status) IN ('ONLINE','SHUNNED','OFFLINE_SOFT', 'OFFLINE_HARD')) NOT NULL DEFAULT 'ONLINE', " +
		"weight INT CHECK (weight >= 0 AND weight <=10000000) NOT NULL DEFAULT 1, " +
		"compression INT CHECK (compression IN(0,1)) NOT NULL DEFAULT 0, " +
		"max_connections INT CHECK (max_connections >=0) NOT NULL DEFAULT 1000, " +
		"max_replication_lag INT CHECK (max_replication_lag >= 0 AND max_replication_lag <= 126144000) NOT NULL DEFAULT 0, " +
		"use_ssl INT CHECK (use_ssl IN(0,1)) NOT NULL DEFAULT 0, " +
		"max_latency_ms INT UNSIGNED CHECK (max_latency_ms>=0) NOT NULL DEFAULT 0, " +
		"comment VARCHAR NOT NULL DEFAULT '', " +
		"PRIMARY KEY (hostgroup_id, hostname, port))  "
	/*
	This table is the working area for the scheduler to deal with the nodes while processing
	 */
	Ddl_create_mysql_servers_scheduler = "CREATE TABLE disk.pxc_servers_scheduler (" +
		"hostgroup_id INT CHECK (hostgroup_id>=0) NOT NULL DEFAULT 0, " +
		"hostname VARCHAR NOT NULL,    port INT CHECK (port >= 0 AND port <= 65535) NOT NULL DEFAULT 3306, " +
		"status VARCHAR CHECK (UPPER(status) IN ('ONLINE','SHUNNED','OFFLINE_SOFT', 'OFFLINE_HARD')) NOT NULL DEFAULT 'ONLINE',  " +
		"retry_up INT DEFAULT 0, " +
		"retry_down INT DEFAULT 0, " +
		"previous_status VARCHAR,  " +
		"backup_hg INT, " +
		"PRIMARY KEY (hostgroup_id, hostname, port))"

	/*
	Configuration table for pxc
	 */
	Ddl_create_pxc_clusters = "CREATE TABLE disk.pxc_clusters (cluster_id INTEGER PRIMARY KEY AUTOINCREMENT," +
		"hg_w INT," +
		"hg_r INT," +
		"bck_hg_w INT," +
		"bck_hg_r INT," +
		"single_writer INT DEFAULT 1," +
		"max_writers INT DEFAULT 1," +
		"writer_is_also_reader INT DEFAULT 1, " +
		"retry_up INT DEFAULT 0, " +
		"retry_down INT DEFAULT 0," +
		"locked INT DEFAULT 0" +
		"lock_timestamp )"

	/*
	cleanup of tables
	 */
	Ddl_drop_mysql_server_original = "DROP TABLE IF EXISTS disk.mysql_servers_original"
	Ddl_drop_mysql_server_scheduler = "DROP TABLE IF EXISTS disk.mysql_servers_scheduler"
	Ddl_drop_pxc_cluster = "DROP TABLE IF EXISTS disk.pxc_clusters"

	Ddl_Truncate_mysql_nodes = "truncate table from mysql_servers"
	Ddl_drop_table_generic = "DROP TABLE IF EXISTS  disk.#table-name#"

	/*
	Information retrieval
 	*/
	Info_Check_table ="show tables from disk"

	//get MySQL nodes to process based on the HG
	//+--------------+---------------+------+-----------+--------------+---------+-------------+-----------------+---------------------+---------+----------------+-------------------------------------------------------------|---------+
	//| hostgroup_id | hostname      | port | gtid_port | status       | weight  | compression | max_connections | max_replication_lag | use_ssl | max_latency_ms | comment                                                     |ConnUsed|
	Dml_Select_mysql_nodes = " select b.*, c.ConnUsed from stats_mysql_connection_pool c left JOIN runtime_mysql_servers b ON  c.hostgroup=b.hostgroup_id and c.srv_host=b.hostname and c.srv_port = b.port where hostgroup_id in (?)  order by hostgroup,srv_host desc;"

    // Get the information to deal with the cluster from pxc_cluster (ID is coming from settings cluster_id)
    Dml_get_mysql_cluster_to_manage = "select cluster_id, hg_w, hg_r, bck_hg_w, bck_hg_r, single_writer, max_writers, writer_is_also_reader, retry_up, retry_down from disk.pxc_clusters where cluster_id = ?"

    //get Variables
    Dml_show_variables="SHOW GLOBAL VARIABLES"
)