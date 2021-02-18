# This is common scenario for 1 writer, 3 reades, 2 backup writers
# 1. Setup 3 node PXC cluster
# 2. Setup ProxySQL 
#  node_1: writer/reader
#  node_2: reader/backup writer weight 1000
#  node_3: reader/backup writer weight 999
# 3. Setup ProxySQL scheduler with config passed in $proxysql_scheduler_config
# 4. Connect to $action_1_node and perform $action_1
# 5. Validate ProxySQL configuration
# 6. Connect to $action_2_node and perform $action_2
# 7. Validate ProxySQL configuration


--source include/galera_cluster_3nodes.inc

# start ProxySQL with default configuration
--source ../include/common_proxysql_init.inc

--connection node_1
--source ../include/common_pxc_init.inc

# Check if ProxySQL scheduler config exists
--file_exists $proxysql_scheduler_config_dir/$proxysql_scheduler_config

#
# ARRANGE
#

# Now setup ProxySql
--connection proxysql_admin
--let wHG = 100
--let rHG = 101

# mysql user
--eval INSERT INTO MYSQL_USERS (username,password,active,default_hostgroup,default_schema,transaction_persistent,comment) VALUES ('app_test','test',1,$wHG,'mysql',1,'application test user DC1')

# query rules
--eval INSERT INTO mysql_query_rules (rule_id,proxy_port,username,destination_hostgroup,active,retries,match_digest,apply) VALUES(1040,6033,'app_test',$wHG,1,3,'^SELECT.*FOR UPDATE',1)
--eval INSERT INTO mysql_query_rules (rule_id,proxy_port,username,destination_hostgroup,active,retries,match_digest,apply) VALUES(1042,6033,'app_test',$rHG,1,3,'^SELECT.*$',1)

--source ../include/apply_proxysql_config.inc

# PxC nodes setup
# writers
--replace_result $NODE_MYPORT_1 NODE_MYPORT_1
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',$wHG,$NODE_MYPORT_1,10000,1000,'WRITE')

# readers
--replace_result $NODE_MYPORT_1 NODE_MYPORT_1
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',$rHG,$NODE_MYPORT_1,100,1000,'READ/WRITE')
--replace_result $NODE_MYPORT_2 NODE_MYPORT_2
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',$rHG,$NODE_MYPORT_2,10000,1000,'READ')
--replace_result $NODE_MYPORT_3 NODE_MYPORT_3
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',$rHG,$NODE_MYPORT_3,10000,1000,'READ')

# backup writers
--replace_result $NODE_MYPORT_2 NODE_MYPORT_2
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',8100,$NODE_MYPORT_2,1000,1000,'Backup writer 1')
--replace_result $NODE_MYPORT_3 NODE_MYPORT_3
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',8100,$NODE_MYPORT_3,10000,999,'Backup writer 2')

# scheduler script
# allow scheduler script to write its logs
--chmod 0777 $MYSQL_TMP_DIR
#--let $scheduler_script=/home/kamil/repo/pxc/8.0/install/proxysql/galera_check.pl

# The following line works with Perl
# --replace_result $proxysql_scheduler_script SCHEDULER_SCRIPT $MYSQL_TMP_DIR MYSQL_TMP_DIR
# --eval INSERT INTO scheduler (id,active,interval_ms,filename,arg1) values (10,1,3000,"$proxysql_scheduler_script","-u=admin -p=admin -h=127.0.0.1 -H=$wHG:W,$rHG:R -P=6032 --debug=1 --active_failover=1 --retry_down=2 --writer_is_also_reader=0 --log=$MYSQL_TMP_DIR/galera_check-perl.log")
# The following line works with Golang
--replace_result $proxysql_scheduler_script SCHEDULER_SCRIPT $proxysql_scheduler_config_dir CONFIG_DIR
--eval INSERT INTO scheduler (id,active,interval_ms,filename,arg1,arg2) values (10,1,3000,"$proxysql_scheduler_script","--configfile=$proxysql_scheduler_config", "--configpath=$proxysql_scheduler_config_dir")

--source ../include/apply_proxysql_config.inc

#
# ACT 1
#

# connect as the regular user
--connect(conn_app_test, 127.0.0.1, app_test, test, , 6033)
--connection conn_app_test
CREATE TABLE t1 (a int primary key);
INSERT INTO t1 VALUES (1), (2), (3);

--echo #
--echo # $action_1_node $action_1
--echo #
--connection $action_1_node
--eval $action_1

# now let ProxySQL scheduler detect this change and do its job
--sleep 30

# Reconnect allowing ProxySQL to route to the new backend
--disconnect conn_app_test
--connect(conn_app_test, 127.0.0.1, app_test, test, , 6033)
--connection conn_app_test
INSERT INTO t1 VALUES (11), (12), (13);


#
# ASSERT 1
#

--connection proxysql_admin
--replace_result $NODE_MYPORT_1 NODE_MYPORT_1 $NODE_MYPORT_2 NODE_MYPORT_2 $NODE_MYPORT_3 NODE_MYPORT_3
--eval SELECT * FROM runtime_mysql_servers ORDER BY hostgroup_id, port;

--connection conn_app_test
# verify that writes reached the backend
SELECT COUNT(*) FROM t1;


#
# ACT 2
#

--echo #
--echo # $action_2_node $action_2
--echo #
--connection $action_2_node
--eval $action_2

# now let ProxySQL scheduler detect this change and do its job
--sleep 30

# Reconnect allowing ProxySQL to route to the new backend
--disconnect conn_app_test
--connect(conn_app_test, 127.0.0.1, app_test, test, , 6033)
--connection conn_app_test
INSERT INTO t1 VALUES (21), (22), (23);

#
# ASSERT 2
#

--connection proxysql_admin
--replace_result $NODE_MYPORT_1 NODE_MYPORT_1 $NODE_MYPORT_2 NODE_MYPORT_2 $NODE_MYPORT_3 NODE_MYPORT_3
--eval SELECT * FROM runtime_mysql_servers ORDER BY hostgroup_id, port;


--connection conn_app_test
# verify that writes reached the backend (promoted Backup writer 1)
SELECT COUNT(*) FROM t1;

#
# CLEANUP
#
# proxysql
--connection proxysql_admin
--source ../include/common_proxysql_deinit.inc
--remove_files_wildcard $MYSQL_TMP_DIR galera_check-perl.log*

# pxc
--connection node_1
DROP TABLE t1;
--source ../include/common_pxc_deinit.inc
