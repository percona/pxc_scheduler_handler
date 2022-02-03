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

package Global

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
	logTarget = "stdout" #stdout | file
	logFile = "/Users/marcotusa/work/temp/pscheduler.log"
	daemonize = false
	daemonInterval = 2000
	performance = true
	OS = "na"
	daemonize : [false] Will allow the script to run in a loop without the need to be call by ProxySQL scheduler
	daemonInterval : Define in ms the time for looping when in development mode
	loglevel : [info] Define the log level to be used
	logTarget : [stdout] Can be either a file or stdout
	logFile : In case file for loging define the target
	OS : for future use
ProxySQL
	port : [6032] Port used to connect
	host : [127.0.0.1] IP address used to connect to ProxySQL
	user : [] User able to connect to ProxySQL
	password : [] Password
	clustered : [false] If this is NOT a single instance then we need to put a lock on the running scheduler (see Working with Cluster section)
	initialized : not used (for the moment)
Pxccluster
	activeFailover : [1] Failover method
	failBack : [false] If we should fail-back automatically or wait for manual intervention
	checkTimeOut : [4000] This is one of the most important settings. When checking the Backend node (MySQL), it is possible that the node will not be able to answer in a consistent amount of time, due the different level of load. If this exceeds the Timeout, a warning will be printed in the log, and the node will not be processed. Parsing the log it is possible to identify which is the best value for checkTimeOut to satisfy the need of speed and at the same time to give the nodes the time they need to answer.
	debug : [0] Some additional debug specific for the pxc cluster
	mainSegment : [1] This is another very important value to set, it defines which is the MAIN segment for failover
	sslClient : "client-cert.pem" In case of use of SSL for backend we need to be able to use the right credential
	sslKey : "client-key.pem" In case of use of SSL for backend we need to be able to use the right credential
	sslCa : "ca.pem" In case of use of SSL for backend we need to be able to use the right credential
	sslCertificatePath : ["/full-path/ssl_test"] Full path for the SSL certificates
	hgW : Writer HG
	hgR : Reader HG
    configHgRange =8000      : The starting value of the configuration groups (hgW + configHgRange) and (hgR + configHgRange) 
	maintenanceHgRange =9000 : The starting value of the maintenance groups (hgW + maintenanceHgRange) and (hgR + maintenanceHgRange)
	(Deprecated) bckHgW : Backup HG in the 8XXX range (hgW + 8000)
	(Deprecated) bckHgR : Backup HG in the 8XXX range (hgR + 8000)
	singlePrimary : [true] This is the recommended way, always use Galera in Single Primary to avoid write conflicts
	maxNumWriters : [1] If SinglePrimary is false you can define how many nodes to have as Writers at the same time
	writerIsAlsoReader : [1] Possible values 0 - 1. The default is 1, if you really want to exclude the writer from read set it to 0. When the cluster will lose its last reader, the writer will be elected as Reader, no matter what.
	retryUp : [0] Number of retries the application should do before restoring a failed node
	retryDown : [0] Number of retries the application should do to put DOWN a failing node
	clusterId : 10 the ID for the cluster

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
