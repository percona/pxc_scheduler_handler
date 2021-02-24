# pxcScheduler
## Concept
pxcScheduler, is a script to manage integration between ProxySQL and Galera (from Codership), including its different implementations like PXC. Galera and its implementations like Percona Cluster (PCX), use the data-centric concept, as such the status of a node is relvant in relation to a cluster.

In ProxySQL is possible to represent a cluster and its segments using HostGroups. PxcScheduler is design to manage a X number of nodes that belong to a given Hostgroup (HG). In pxcScheduler it is also important to qualify the HG. This is true in case of multi-node like PXC or in case of use of Replication HG.

PxcScheduler can now also manage single writer in PXC and fail-over/fail-back when in the need to. This is implemented usings:

- Backup Node definition (using hostgroup. Backup Nodes are configuration nodes
- ~~Use PXC_CLUSTER_VIEW table (in PXC only)~~

PxcScheduler works by HG and as such it will perform isolated actions/checks by HG. It is not possible to have more than one check running on the same HG.

To prevent to have multiple test running , the check create a lock file {ClusterId}_${hg}.pid} that will be used by the check to prevent duplicates. PxcScheduler will connect to the ProxySQL node and retrieve all the information regarding the Nodes/proxysql configuration. It will then check in parallel each node and will retrieve the status and configuration.

At the moment galera_check analyze and manage the following:

Node states:
 * pxc_main_mode
 * read_only
 * wsrep_status
 * wsrep_rejectqueries
 * wsrep_donorrejectqueries
 * wsrep_connected
 * wsrep_desinccount
 * wsrep_ready
 * wsrep_provider
 * wsrep_segment
 * Number of nodes in by segment
 * Retry loop
 * PXC cluster state:

### Fail-over options:

Presence of active node in the special backup Hostgroup (8000 + original HG id).
Special HostGoup 8000 is used also for READERS, to define which should be checked and eventually add to the pool if missed

If a node is the only one in a segment, the check will behave accordingly. IE if a node is the only one in the MAIN segment, it will not put the node in OFFLINE_SOFT when the node become donor to prevent the cluster to become unavailable for the applications. As mention is possible to declare a segment as MAIN, quite useful when managing prod and DR site.

The check can be configure to perform retry in a X interval. Where X is the time define in the ProxySQL scheduler. As such if the check is set to have 2 retry for UP and 4 for down, it will loop that number before doing anything.

This feature is very useful in some not well known cases where Galera behave weird. IE whenever a node is set to READ_ONLY=1, galera desync and resync the node. A check not taking this into account will cause a node to be set OFFLINE and back for no reason.

Another important differentiation for this check is that it use special HGs for maintenance, all in range of 9000. So if a node belong to HG 10 and the check needs to put it in maintenance mode, the node will be moved to HG 9010. Once all is normal again, the Node will be put back on his original HG.

The special group of 8000 is instead used for configuration, this is it you will need to insert the 8XXXX referring to your WRITER HG and READER HG as the configuration the script needs to refer to. To be clear 8XXX where X are the digit of your Hostgroup id ie 20 -> 8020, 1 -> 8001 etc .

This check does NOT modify any state of the Nodes. Meaning __It will NOT__ modify any variables or settings in the original node. It will ONLY change states in ProxySQL.

### Multi PXC node and Single writer. 
ProxySQL can easily move traffic read or write from a node to another in case of a node failure. Normally playing with the weight will allow us to have a (stable enough) scenario. But this will not guarantee the FULL 100% isolation in case of the need to have single writer. When there is that need, using only the weight will not be enough given ProxySQL will direct some writes also to the other nodes, few indeed, but still some of them, as such no 100% isolated. Unless you use single_writer option (ON by default), in that case your PXC setup will rely on one Writer a time.

To manage that and also to provide a good way to set/define what and how to fail-over in case of need, I had implement the feature: __activeFailover__Valid values are:
- 0 [default] do not make fail-over 
- 1 make fail-over only if HG 8000 is specified in ProxySQL mysl_servers 

__Note__ The previous perl version had also 
- 2 use PXC_CLUSTER_VIEW to identify a server in the same segment
- 3 do whatever to keep service up also fail-over to another segment (use PXC_CLUSTER_VIEW)

Given it was reported to me they were almost never used I had not implemented them in this GO version. 
__But__ if there is a real need I will put them back.

### Example of proxySql setup 
Assuming we have 3 nodes:
- node4 : `192.168.4.22`
- node5 : `192.168.4.23`
- node6 : `192.168.4.233`

As Hostgroup:
- HG 100 for Writes
- HG 101 for Reads
We have to configure also nodes in 8XXX:
- HG 8100 for Writes 
- HG 8101 for Reads

We will need to :
```MySQL
delete from mysql_servers where hostgroup_id in (100,101,8100,8101);
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.22',100,3306,1000,2000,'Preferred writer');
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.23',100,3306,999,2000,'Second preferred ');
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.233',100,3306,998,2000,'Las chance');
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.22',101,3306,998,2000,'last reader');
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.23',101,3306,1000,2000,'reader1');    
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.233',101,3306,1000,2000,'reader2');        

INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.22',8100,3306,1000,2000,'Failover server preferred');
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.23',8100,3306,999,2000,'Second preferred');    
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.233',8100,3306,998,2000,'Thirdh and last in the list');      

INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.22',8101,3306,998,2000,'Failover server preferred');
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.23',8101,3306,999,2000,'Second preferred');    
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.233',8101,3306,1000,2000,'Thirdh and last in the list');      


LOAD MYSQL SERVERS TO RUNTIME; SAVE MYSQL SERVERS TO DISK;    

```
Will create entries in ProxySQL for 2 main HG (100 for write and 101 for read) It will also create 3 entries for the SPECIAL group 8100. This groups will be used by the script to manage the fail-over of HG 100. It will also create 3 entries for the SPECIAL group 8101. This group will be used by the script to manage the read nodes in HG 51.

In the above example, what will happen is that if you have set *active_failover*=1, the script will check the nodes, if node 192.168.4.22',100 is not up, the script will try to identify another node within the same segment that has the higest weight IN THE 8100 HG. In this case it will elect as new writer the node'192.168.4.23',8100.

Please note that active_failover=1, is the __only deterministic__ method to failover, based on what YOU define. If set correctly across a ProxySQL cluster, all nodes will act the same. Yes a possible delay given the check interval may exists, but that cannot be avoided.

## How to configure
There are many options that can be set, and I foresee we will have to add even more. Given that I have abandoned the previous style of using command line and move to config-file definition. Yes this is a bit more expensive, given the file access but it is minimal.

First let us see what we have:
- 3 sections
    - Global 
    - pxccluster
    - proxysql
    
#### Global:
```[global]
debug = true
logLevel = "debug"
logTarget = "stdout" #stdout | file
logFile = "/Users/marcotusa/work/temp/pscheduler.log"
daemonize = true
daemonInterval = 2000
performance = true
OS = "na"
```

- debug : [false] will active some additional features to debug locally as more verbose logs
- daemonize : [false] Will allow the script to run in a loop without the need to be call by ProxySQL scheduler 
- daemonInterval : Define in ms the time for looping when in daemon mode
- loglevel : [error] Define the log level to be used 
- logTarget : [stdout] Can be either a file or stdout 
- logFile : In case file for loging define the target 
- OS : for future use
-  lockfiletimeout  Time ins seconds after which the file lock is considered expired [local instance lock]
-  lockclustertimeout Time in seconds after which the cluster lock is considered expired

#### ProxySQL
- port : [6032] Port used to connect 
- host : [127.0.0.1] IP address used to connect to ProxySQL
- user : [] User able to connect to ProxySQL
- password : [] Password 
- clustered : [false] If this is __NOT__ a single instance then we need to put a lock on the running scheduler (see [Working with ProxySQL cluster](Working-with-ProxySQL-cluster) section)
- initialized : not used (for the moment) 

#### Pxccluster
- activeFailover : [1] Failover method
- failBack : [false] If we should fail-back automatically or wait for manual intervention 
- checkTimeOut : [4000] This is one of the most important settings. When checking the Backend node (MySQL), it is possible that the node will not be able to answer in a consistent amount of time, due the different level of load. If this exceeds the Timeout, a warning will be print in the log, and the node will not be processed. Parsing the log it is possible to identify which is the best value for checkTimeOut to satisfy the need of speed and at the same time to give the nodes the time they need to answer.
- mainSegment : [1] This is another very important value to set, it defines which is the MAIN segment for failover
- sslClient : "client-cert.pem" In case of use of SSL for backend we need to be able to use the right credential
- sslKey : "client-key.pem" In case of use of SSL for backend we need to be able to use the right credential
- sslCa : "ca.pem" In case of use of SSL for backend we need to be able to use the right credential
- sslCertificatePath : ["/full-path/ssl_test"] Full path for the SSL certificates
- hgW : Writer HG
- hgR : Reader HG 
- bckHgW : Backup HG in the 8XXX range (hgW + 8000)
- bckHgR :  Backup HG in the 8XXX range (hgR + 8000)
- singlePrimary : [true] This is the recommended way, always use Galera in Single Primary to avoid write conflicts
- maxNumWriters : [1] If SinglePrimary is false you can define how many nodes to have as Writers at the same time
- writerIsAlsoReader : [1] Possible values 0 - 1. The default is 1, if you really want to exclude the writer from read set it to 0. When the cluster will lose its last reader, the writer will be elected as Reader, no matter what. 
- retryUp : [0] Number of retry the script should do before restoring a failed node
- retryDown : [0] Number of retry the script should do to put DOWN a failing node
- clusterId : 10 the ID for the cluster 
 
## Examples of configurations in ProxySQL
Simply pass max 2 arguments 

```MySQL 
INSERT  INTO scheduler (id,active,interval_ms,filename,arg1,arg2) values (10,0,2000,"/var/lib/proxysql/pxcScheduler","--configfile=config.toml","--configpath=<path to config>");
LOAD SCHEDULER TO RUNTIME;SAVE SCHEDULER TO DISK;
```


To Activate it:
```MySQL 
update scheduler set active=1 where id=10;
LOAD SCHEDULER TO RUNTIME;
```

## Logic Rules used in the check:
Set to offline_soft :

- any non 4 or 2 state, read only =ON donor node reject queries - 0 size of cluster > 2 of nodes in the same segments more then one writer, node is NOT read_only

Change HG for maintenance HG:

- Node/cluster in non primary wsrep_reject_queries different from NONE Donor, node reject queries =1 size of cluster

Node comes back from offline_soft when (all of them):
- Node state is 4
- wsrep_reject_queries = none
-Primary state

Node comes back from maintenance HG when (all of them):
- node state is 4
- wsrep_reject_queries = none
- Primary state
- PXC (pxc_maint_mode).

PXC_MAIN_MODE is fully supported. Any node in a state different from pxc_maint_mode=disabled will be set in OFFLINE_SOFT for all the HostGroup.

__Single Writer__
You can define IF you want to have multiple writers. Default is 1 writer only (I strongly recommend you to do not use multiple writers unless you know very well what are you doing), but you can now have multiple writers at the same time.


## Working with ProxySQL cluster
Working with ProxySQL cluster is a real challenge give A LOT of things that should be there are not. 

ProxySQL do not even has idea if a ProxySQL cluster node is up or down. To be precise it knows internally but do not expose this at any level, only as very noisy log entry.
Given this the script must not only get the list of ProxySQL nodes but validate them. 

In terms of checks we do:
We check if the node were we are has a lock or if can acquire one.
If not we will return nil to indicate the program must exit given either there is already another node holding the lock or this node is not in a good state to acquire a lock.
All the DB operations are done connecting locally to the ProxySQL node running the scheduler.

Find lock method review all the nodes existing in the Proxysql for an active Lock it checks only nodes that are reachable.
Checks for:
- an existing lock locally
- lock on another node
- lock time comparing it with lockclustertimeout parameter

__Related Parameters__

```editorconfig
[proxysql]
clustered = true

[global]
lockfiletimeout = 60 #seconds 
lockclustertimeout = 600 # seconds
```


ProxySQL Documentation reports:
```
TODO
 - add support for MySQL Group Replication
 - add support for Scheduler
Roadmap
This is an overview of the features related to clustering, and not a complete list. None the following is impletemented yet.
Implementation may be different than what is listed right now:
 - support for master election: the word master was intentionally chosen instead of leader
 - only master proxy is writable/configurable
 - implementation of MySQL-like replication from master to slaves, allowing to push configuration in real-time instead of pulling it
 - implementation of MySQL-like replication from master to candidate-masters
 - implementation of MySQL-like replication from candidate-masters to slaves
 - creation of a quorum with only candidate-masters: normal slaves are not part of the quorum
```
None of the above is implemented 

## Download and compile from source
Once you have GO installed and running (version 1.15.8 is recommended)

Clone from github: `git clone https://github.com/Tusamarco/proxysql_scheduler.git`

Install the needed library within go:
```bash
go get github.com/Tusamarco/toml
go get github.com/go-sql-driver/mysql
go get github.com/sirupsen/logrus
go get golang.org/x/text/language
go get golang.org/x/text/message

go build .

```
First thing to do then is to run `./proxysql_scheduler --help`
to navigate the parameters.

Then adjust the config file in `./config/config.toml` better to do a copy and modify for what u need
Then to test it OUTSIDE the ProxySQL scheduler script, in the config file `[Global]` section change `daemonize=false` to `true`. 
The script will auto-loop as if call by the scheduler. 

 
#### Details


![function flow](./docs/flow-Funtions-calls-flow.png "Function flow")

<!--
![overview](./docs/flow-overall.png "overview")
![nodes check](./docs/flow-check_nodes.png "Nodes check")
-->