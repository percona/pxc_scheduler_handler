# This is common scenario for 1 writer, 2 reades, 2 backup writers
# 1. Setup 3 node PXC cluster
# 2. Setup ProxySQL 
#  node_1: reader/backup wrtier weight 1000
#  node_2: writer
#  node_3: reader/backup writer weight 999
# 3. Setup ProxySQL scheduler with config passed in $proxysql_scheduler_config
# 4. shutdown writer
#   => node_3 promoted to writer
#   => node_2 status changed to SHUNED, node moved to hostgroup 9100


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
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',$wHG,$NODE_MYPORT_2,10000,1000,'WRITE')
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',$rHG,$NODE_MYPORT_2,100,1000,'READ/WRITE')
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',$rHG,$NODE_MYPORT_1,10000,1000,'READ')
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',$rHG,$NODE_MYPORT_3,10000,1000,'READ')

--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',8100,$NODE_MYPORT_1,1000,1000,'Backup writer 1')
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',8100,$NODE_MYPORT_3,10000,999,'Backup writer 2')
--eval INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('127.0.0.1',8101,$NODE_MYPORT_3,10000,999,'Backup reader 1')

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
--echo # remove reader from active HG $rHG
--echo #
--connection proxysql_admin
--eval DELETE FROM mysql_servers where hostgroup_id=$rHG
--source ../include/apply_proxysql_config.inc

# now let ProxySQL scheduler detect this change and do its job
--sleep 60

#
# ASSERT 1
#
--disconnect proxysql_admin
--connect(proxysql_admin, 127.0.0.1, admin, admin, , 6032)
--connection proxysql_admin

--eval SELECT * FROM runtime_mysql_servers ORDER BY hostgroup_id, port;

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