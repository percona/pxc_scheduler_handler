# Release Notes
Pxc Scheduler Handler, follow the [release numeric standard as for ...](https://semver.org/).
Given a version number MAJOR.MINOR.PATCH, increment the:
- MAJOR version when you make incompatible API changes,
- MINOR version when you add functionality in a backwards compatible manner, and
- PATCH version when you make backwards compatible bug fixes. 

Additional labels for pre-release and build metadata are available as extensions to the MAJOR.MINOR.PATCH format.

## Release 1.3.0
This release see only the addition of IPv6 support in the pxc_scheduler_handler.
You just need to add the proper IPv6 in the mysql_server table:
```
mysql> select hostgroup_id,hostname,port from mysql_servers ;
+--------------+---------------------+------+
| hostgroup_id | hostname            | port |
+--------------+---------------------+------+
| 100          | 2001:db8:0:f101::5  | 3306 |
| 101          | 2001:db8:0:f101::21 | 3306 |
| 101          | 2001:db8:0:f101::31 | 3306 |
| 101          | 2001:db8:0:f101::5  | 3306 |
| 8100         | 2001:db8:0:f101::21 | 3306 |
| 8100         | 2001:db8:0:f101::31 | 3306 |
| 8100         | 2001:db8:0:f101::5  | 3306 |
| 8101         | 2001:db8:0:f101::21 | 3306 |
| 8101         | 2001:db8:0:f101::31 | 3306 |
| 8101         | 2001:db8:0:f101::5  | 3306 |
+--------------+---------------------+------+
```
To use IPv6 with PXC you need to modify your my.cnf as:
```
wsrep-cluster-address                                       = gcomm://[2001:db8:0:f101::5]:4567,[2001:db8:0:f101::21]:4567,[2001:db8:0:f101::31]:4567
wsrep-node-address                                          = [2001:db8:0:f101::31]:4567
wsrep-node-incoming-address                         = [2001:db8:0:f101::31]:3306
wsrep-provider-options                                      = "gmcast.listen_addr=tcp://[::]:4567"
```
and be sure to add this section as well:
```
[sst]
sockopt=,pf=ip6
```
## Release 1.2.0
In this release we have implemented two Feature Requests (FR) coming directly from Percona Global service (GS).
The first one is [FR-28](https://github.com/Tusamarco/pxc_scheduler_handler/issues/28) which was requested by Percona Support.
The second FR is [FR-30](https://github.com/Tusamarco/pxc_scheduler_handler/issues/30) requested by Percona Professional Service.


### FR-28
The FR-28 was requesting to have the values of the two special groups (8000 and 9000) configurable.
The group 8000 is internally used for configuration purpose, while the group 9000 is used to put a node offline for maintenance.

The FR was implemented adding two new parameters and deprecating other two:</br>
New
- configHgRange
- maintenanceHgRange

Deprecated
- bckHgW
- bckHgWR

The implementation is compatible with the previous behaviour so if the new params are not specified they will be default to old hardcoded values. 
If instead they are specified the new values are used.
For instance
```
  hgW = 100
  hgR = 101
  configHgRange = 88000
  maintenanceHgRange = 99000
```
In this case the value of the Configuration Hostgroups will be 88000 + Hg(W|R) = 88100 | 88101
The same will happen with the Maintenance Hostgroups 99000 + Hg(W|R) = 99100 | 99101

Of course the configuration hostgroups must be present when creating/adding the servers in Proxysql mysql_server tables:
```bigquery
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.22',88100,3306,1000,2000,'Failover server preferred');
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.23',88100,3306,999,2000,'Second preferred');    
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.233',88100,3306,998,2000,'Thirdh and last in the list');      

INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.22',88101,3306,998,2000,'Failover server preferred');
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.23',88101,3306,999,2000,'Second preferred');    
INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.233',88101,3306,1000,2000,'Thirdh and last in the list');      


LOAD MYSQL SERVERS TO RUNTIME; SAVE MYSQL SERVERS TO DISK;    
```


### FR-30 
FR-30 was asking to have the possibility to do not alter the status of a node when it is Desync IF a value for `max_replication_lag` is specified.
The original behaviour was that any node in `desync` state and with another node in the same hostgroup, is moved to `OFFLINE_SOFT`. 
That would allow the node to continue to serve existing request, but will prevent it to accept new ones.

With the implementation of this feature, node without `max_replication_lag` specified will continue to behave exactly the same.

If instead a node has `max_replication_lag` is specified, the node will NOT be set as `OFFLINE_SOFT` unless its `wsrep_local_recv_queue` exceed the value of the `max_replication_lag` for `retryDown` times.

The node will come back from `OFFLINE_SOFT` if its `wsrep_local_recv_queue` is __HALF__ of the `max_replication_lag` for `retryUp` times.

IE: 
```
max_replication_lag = 500
retryDown =2
retryUp =1

wsrep_local_recv_queue = 300 --> Node will remain in ONLINE state
wsrep_local_recv_queue = 500 --> retryDown = 1 --> ONLINE
wsrep_local_recv_queue = 500 --> retryDown = 2 --> OFFLINE_SOFT

wsrep_local_recv_queue = 500 --> OFFLINE_SOFT
wsrep_local_recv_queue = 300 --> OFFLINE_SOFT
wsrep_local_recv_queue = 230 --> retryUp = 1 --> ONLINE

```

## Release 1.1.0
Persist Primary Values
In pxc_scheduler_handler is possible to ask the application to keep the values assigned to the Primary Writer also when another node is elected, as in case of fail-over.

There are few conditions for this to work consistently:

Cluster must be Single Primary
When using Fail Back given the Primary will have the same weight of the node coming back, FailBack will NOT automatically work.
Said that you can define if you want ONLY the value for the WRITER changed, or if you want to modify also the corresponding READER.
Let us use an example to have this clear.
see [Release notes 1.1.0](https://github.com/Tusamarco/pxc_scheduler_handler/releases/tag/v1.1.0)


