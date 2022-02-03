# This is common scenario for 1 writer, 1 reades, 3 backup writers, 3 backup readers
# When $node_to_kill is killed and then started again
# We cannot use the node_1 for this as it is used to boostrap the cluster (and 
# handling this in MTR is more problematic)
#
# 1. Setup 3 node PXC cluster
# 2. Setup ProxySQL 
#  node_1:          backup reader / backup writer
#  node_2: reader / backup reader / backup writer
#  node_3: writer / backup reader / backup writer
# 3. Setup ProxySQL scheduler with config passed in $pxc_scheduler_handler_config
# 4. shutdown $node_to_kill
# 5. start $node_to_kill

--source include/galera_cluster_3nodes.inc

# start ProxySQL with default configuration
--source ../include/common_proxysql_init.inc

--connection node_1
--source ../include/common_pxc_init.inc

# Check if ProxySQL scheduler config exists
--file_exists $pxc_scheduler_handler_config_dir/$pxc_scheduler_handler_config

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
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',$wHG,$NODE_MYPORT_1,$node_1_bwr_priority,1000,'WRITE')
--replace_result $NODE_MYPORT_3 NODE_MYPORT_3
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',$wHG,$NODE_MYPORT_3,$node_3_bwr_priority,1000,'WRITE')

# readers
--replace_result $NODE_MYPORT_1 NODE_MYPORT_1
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',$rHG,$NODE_MYPORT_1,$node_1_brd_priority,1000,'READ')
--replace_result $NODE_MYPORT_2 NODE_MYPORT_2
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',$rHG,$NODE_MYPORT_2,$node_2_brd_priority,1000,'READ')

# backup writers
--replace_result $NODE_MYPORT_3 NODE_MYPORT_3
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',8100,$NODE_MYPORT_3,$node_3_bwr_priority,1000,'Backup writer 3')
--replace_result $NODE_MYPORT_2 NODE_MYPORT_2
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',8100,$NODE_MYPORT_2,$node_2_bwr_priority,1000,'Backup writer 2')
--replace_result $NODE_MYPORT_1 NODE_MYPORT_1
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',8100,$NODE_MYPORT_1,$node_1_bwr_priority,1000,'Backup writer 1')

# backup readers
--replace_result $NODE_MYPORT_3 NODE_MYPORT_3
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',8101,$NODE_MYPORT_3,$node_3_brd_priority,1000,'Backup reader 3')
--replace_result $NODE_MYPORT_2 NODE_MYPORT_2
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',8101,$NODE_MYPORT_2,$node_2_brd_priority,1000,'Backup reader 2')
--replace_result $NODE_MYPORT_1 NODE_MYPORT_1
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',8101,$NODE_MYPORT_1,$node_1_brd_priority,1000,'Backup reader 1')


# scheduler script
# allow scheduler script to write its logs
--chmod 0777 $MYSQL_TMP_DIR
#--let $scheduler_script=/home/kamil/repo/pxc/8.0/install/proxysql/galera_check.pl

# The following line works with Perl
# --replace_result $pxc_scheduler_handler_script SCHEDULER_SCRIPT $MYSQL_TMP_DIR MYSQL_TMP_DIR
# --eval INSERT INTO scheduler (id,active,interval_ms,filename,arg1) values (10,1,3000,"$pxc_scheduler_handler_script","-u=admin -p=admin -h=127.0.0.1 -H=$wHG:W,$rHG:R -P=6032 --debug=1 --active_failover=1 --retry_down=2 --writer_is_also_reader=0 --log=$MYSQL_TMP_DIR/galera_check-perl.log")
# The following line works with Golang
--replace_result $pxc_scheduler_handler_script SCHEDULER_SCRIPT $pxc_scheduler_handler_config_dir CONFIG_DIR
--eval INSERT INTO scheduler (id,active,interval_ms,filename,arg1,arg2) values (10,1,2000,"$pxc_scheduler_handler_script","--configfile=$pxc_scheduler_handler_config","--configpath=$pxc_scheduler_handler_config_dir")

--source ../include/apply_proxysql_config.inc

--sleep 10

 
#
# ACT 1
#

# connect as the regular user
--connect(conn_app_test, 127.0.0.1, app_test, test, , 6033)
--connection conn_app_test
CREATE TABLE t1 (a int primary key);

--sleep 5

INSERT INTO t1 VALUES (1), (2), (3);

--echo #
--echo # $node_to_kill kill
--echo #
--connection $node_to_kill
--source include/kill_mysqld.inc

# now let ProxySQL scheduler detect this change and do its job
--sleep 70

# Reconnect allowing ProxySQL to route to the new backend
--disconnect conn_app_test
--connect(conn_app_test, 127.0.0.1, app_test, test, , 6033)
--connection conn_app_test
INSERT INTO t1 VALUES (11), (12), (13);
--sleep 20


#
# ASSERT 1
#

--connection proxysql_admin
--replace_result $NODE_MYPORT_1 NODE_MYPORT_1 $NODE_MYPORT_2 NODE_MYPORT_2 $NODE_MYPORT_3 NODE_MYPORT_3
--eval SELECT hostgroup_id,hostname,port,gtid_port,status,weight,compression,max_connections,max_replication_lag,use_ssl,max_latency_ms FROM runtime_mysql_servers WHERE hostgroup_id in ($wHG,$rHG)  ORDER BY hostgroup_id, port;

--connection conn_app_test
# verify that writes reached the backend
SELECT COUNT(*) FROM t1;


#
# ACT 2
#

--echo #
--echo # $node_to_kill back online
--echo #
--connection $node_to_kill
--source include/start_mysqld.inc
--source include/wait_wsrep_ready.inc

# now let ProxySQL scheduler detect this change and do its job
--sleep 80

# Reconnect allowing ProxySQL to route to the new backend
--disconnect conn_app_test
--connect(conn_app_test, 127.0.0.1, app_test, test, , 6033)
--connection conn_app_test
INSERT INTO t1 VALUES (21), (22), (23);
--sleep 30

#
# ASSERT 2
#

--connection proxysql_admin
--replace_result $NODE_MYPORT_1 NODE_MYPORT_1 $NODE_MYPORT_2 NODE_MYPORT_2 $NODE_MYPORT_3 NODE_MYPORT_3
--eval SELECT hostgroup_id,hostname,port,gtid_port,status,weight,compression,max_connections,max_replication_lag,use_ssl,max_latency_ms FROM runtime_mysql_servers WHERE hostgroup_id in ($wHG,$rHG)  ORDER BY hostgroup_id, port;

--connection conn_app_test
# verify that writes reached the backend
SELECT COUNT(*) FROM t1;


#
# CLEANUP
#
# proxysql
--connection proxysql_admin
--source ../include/common_proxysql_deinit.inc
--remove_files_wildcard $MYSQL_TMP_DIR galera_check-perl.log*

# pxc
--connection node_2
DROP TABLE t1;
--source ../include/common_pxc_deinit.inc
