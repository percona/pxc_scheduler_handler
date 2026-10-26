/*
 * Copyright (c) Marco Tusa 2021 - present
 *                     GNU GENERAL PUBLIC LICENSE
 *                        Version 3, 29 June 2007
 *
 *  Copyright (C) 2007 Free Software Foundation, Inc. <https://fsf.org/>
 *  Everyone is permitted to copy and distribute verbatim copies
 *  of this license document, but changing it is not allowed.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package global

import "fmt"

type HelpText struct {
	inParams  [2]string
	license   string
	helpShort string
}

func (help *HelpText) Init() {
	help.inParams = [2]string{"configfile", "configPath"}
}
func (help *HelpText) PrintLicense() {
	fmt.Println(help.GetHelpText())
}

func (help *HelpText) GetHelpText() string {
	helpText := `pxcScheduler

Parameters for the executable --configfile <file name> --configpath <full path> --help


Parameters in the config file:
Global:
	logLevel = "info"
	logTarget = "stdout"        #stdout | file
	logFile = "/var/log/pscheduler.log"
	daemonize = false
	daemonInterval = 2000
	performance = true
	lockFileTimeout = 60
	lockClusterTimeout = 600
	lockRefreshTime = 400

	daemonize         : [false] Run in a loop without needing to be called repeatedly by the ProxySQL scheduler
	daemonInterval    : [2000] Loop interval in milliseconds when daemonize=true
	logLevel          : [info] Log verbosity: error | warning | info | debug
	logTarget         : [stdout] Log destination: stdout | file
	logFile           : Full path to the log file when logTarget=file
	lockFileTimeout   : [60] Seconds after which the local file lock is considered expired
	lockClusterTimeout: [600] Seconds after which the ProxySQL cluster lock is considered expired (minimum 60)
	lockRefreshTime   : Time in seconds to refresh the lock epoch in ProxySQL; auto-set to 3/4 of lockClusterTimeout if not specified or too large
	performance       : [true] Enable execution phase timing; printed at INFO level even when logLevel=error/warning

ProxySQL
	port                    : [6032] Port used to connect to ProxySQL admin interface
	host                    : [127.0.0.1] IP address of the ProxySQL instance; must match proxysql_servers table when clustered=true
	user                    : User with access to the ProxySQL admin interface
	password                : Password for the ProxySQL admin user
	clustered               : [false] Enable distributed locking for ProxySQL cluster deployments
	lockFilePath            : [/tmp] Directory where the local file lock is created; defaults to /tmp with a warning if unset
	respectManualOfflineSoft: [false] When true, nodes in OFFLINE_SOFT are never modified by the scheduler (unsafe — see documentation)
	pingTimeout             : [1000] Connection timeout in milliseconds for the ProxySQL admin connection; auto-adjusted to checkTimeOut/2 if >= checkTimeOut

Pxccluster
	user               : MySQL monitor user for connecting to PXC nodes and querying performance_schema (required)
	password           : Password for the MySQL monitor user (required)
	activeFailover     : [1] Failover behaviour:
	                       0 = disabled (warning logged on writer loss)
	                       1 = failover within the same Galera segment (recommended)
	                       2 = failover across segments (multi-datacenter)
	failBack           : [false] Automatically re-elect the original Primary when it returns (off by default — see documentation)
	checkTimeOut       : [4000] Max milliseconds to wait for a PXC node response; node is skipped if exceeded
	mainSegment        : [1] Galera segment treated as PRIMARY for failover decisions
	sslClient          : Client certificate filename (e.g. client-cert.pem)
	sslKey             : Client private key filename (e.g. client-key.pem)
	sslCa              : CA certificate filename (e.g. ca.pem)
	sslCertificatePath : Full directory path containing the SSL certificate files
	hgW                : Hostgroup ID for Writes
	hgR                : Hostgroup ID for Reads
	configHgRange      : [8000] Base for Configuration Hostgroups: configHgRange+hgW and configHgRange+hgR
	maintenanceHgRange : [9000] Base for Maintenance Hostgroups: maintenanceHgRange+hgW and maintenanceHgRange+hgR
	(Deprecated) bckHgW: replaced by configHgRange; a warning is logged if set
	(Deprecated) bckHgR: replaced by configHgRange; a warning is logged if set
	singlePrimary      : [true] Enforce single-writer mode (strongly recommended)
	maxNumWriters      : [1] Maximum simultaneous writers when singlePrimary=false
	writerIsAlsoReader : [1] 0=writer excluded from reader HG; 1=writer also serves reads. Cannot be 0 when maxNumWriters>1
	retryUp            : [0] Consecutive healthy checks required before a node is restored to ONLINE
	retryDown          : [0] Consecutive failing checks required before a node is moved to OFFLINE_SOFT
	clusterId          : [10] Numeric cluster identifier; used to scope the distributed lock
	persistPrimarySettings: [0] Copy Primary node values to the new Primary on failover:
	                       0 = disabled
	                       1 = copy writer HG values only
	                       2 = copy both writer and reader HG values
	pingTimeout        : [1000] Connection timeout in milliseconds for PXC node connections; auto-adjusted to checkTimeOut/2 if >= checkTimeOut

	Examples of configurations in ProxySQL
	Simply pass max 2 arguments




Example of proxySql setup
Assuming we have 3 nodes:

	node4 : 192.168.4.22
	node5 : 192.168.4.23
	node6 : 192.168.4.233
As Hostgroup:

	HG 100 for Writes
	HG 101 for Reads We have to configure also nodes in 8XXX:
	HG 8100 for Writes
	HG 8101 for Reads

We will need to :
	INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.22',100,3306,1000,2000,'Preferred writer');
	INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.23',100,3306,999,2000,'Second preferred ');
	INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.233',100,3306,998,2000,'Last chance');
	INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.22',101,3306,998,2000,'Last reader');
	INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.23',101,3306,1000,2000,'Reader1');    
	INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.233',101,3306,1000,2000,'Reader2');        
	
	INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.22',8100,3306,1000,2000,'Failover server preferred');
	INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.23',8100,3306,999,2000,'Second preferred');    
	INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.233',8100,3306,998,2000,'Third and last in the list');      
	
	INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.22',8101,3306,998,2000,'Failover server preferred');
	INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.23',8101,3306,999,2000,'Second preferred');    
	INSERT INTO mysql_servers (hostname,hostgroup_id,port,weight,max_connections,comment) VALUES ('192.168.4.233',8101,3306,1000,2000,'Third and last in the list');      

	LOAD MYSQL SERVERS TO RUNTIME; SAVE MYSQL SERVERS TO DISK;
`
	return helpText
}
