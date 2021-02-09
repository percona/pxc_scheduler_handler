
--connection proxysql_admin
--echo #
--echo # Loading default ProxySQL configuration
--echo #

--disable_query_log
DELETE FROM mysql_servers;
DELETE FROM mysql_users;
DELETE FROM proxysql_servers;
DELETE FROM mysql_query_rules;
DELETE FROM scheduler;
SAVE MYSQL SERVERS TO DISK;
SAVE MYSQL USERS TO DISK;
SAVE PROXYSQL SERVERS TO DISK;
SAVE MYSQL QUERY RULES TO DISK;
SAVE SCHEDULER TO DISK;

LOAD MYSQL SERVERS FROM DISK;
LOAD MYSQL SERVERS TO RUNTIME;

LOAD MYSQL USERS FROM DISK;
LOAD MYSQL USERS TO RUNTIME;

LOAD ADMIN VARIABLES FROM DISK;
LOAD ADMIN VARIABLES TO RUNTIME;

LOAD MYSQL VARIABLES FROM DISK;
LOAD MYSQL VARIABLES TO RUNTIME;

LOAD PROXYSQL SERVERS FROM DISK;
LOAD PROXYSQL SERVERS TO RUNTIME;

LOAD MYSQL QUERY RULES FROM DISK;
LOAD MYSQL QUERY RULES TO RUNTIME;

LOAD SCHEDULER FROM DISK;
LOAD SCHEDULER TO RUNTIME;
--enable_query_log

# We expect clean ProxySQL configuration at this point
# Cannot use inluce/assert.inc because it uses mysql specific features (LOCATE)
# so let MTR framework validate the output produced by queries.
SELECT COUNT(*) FROM runtime_mysql_servers;
SELECT COUNT(*) FROM runtime_mysql_users;
SELECT COUNT(*) FROM runtime_proxysql_servers;
SELECT COUNT(*) FROM runtime_mysql_query_rules;
SELECT COUNT(*) FROM runtime_scheduler;
