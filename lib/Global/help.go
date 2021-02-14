package Global

import "fmt"


type HelpText struct{
	inParams [2]string
	license string
	helpShort string
}
func (help *HelpText) Init(){
	help.inParams = [2]string{"configfile","configPath"}
}
func (help *HelpText)PrintLicense(){
		fmt.Println(help.GetHelpText())
}

func (help *HelpText)GetHelpText() string{
 helpText := `

# ############################################################################
# Documentation
# #################
=pod

=head1 NAME
galera_check.pl

=head1 OPTIONS

=over

galera_check.pl -u=admin -p=admin -h=192.168.1.50 -H=500:W,501:R -P=3310 --main_segment=1 --debug=0  --log <full_path_to_file> --help
sample [options] [file ...]
 Options:
   -u|user            user to connect to the proxy
   -p|password        Password for the proxy
   -h|host            Proxy host
   -H                 Hostgroups with role definition. List comma separated.
		      Definition R = reader; W = writer [500:W,501:R]
   --main_segment     If segments are in use which one is the leading at the moment
   --retry_up         The number of loop/test the check has to do before moving a node up (default 0)
   --retry_down       The number of loop/test the check has to do before moving a node Down (default 0)
   --log	      Full path to the log file ie (/var/log/proxysql/galera_check_) the check will add
		      the identifier for the specific HG.
   --active_failover  A value from 0 to 3, indicating what level/kind of fail-over the script must perform.
                       active_failover
                       Valid values are:
                          0 [default] do not make failover
                          1 make failover only if HG 8000 is specified in ProxySQL mysl_servers
                          2 use PXC_CLUSTER_VIEW to identify a server in the same segment
                          3 do whatever to keep service up also failover to another segment (use PXC_CLUSTER_VIEW)
   --single_writer    Active by default [single_writer = 1 ] if disable will allow to have multiple writers       
   
   
   Performance parameters 
   --check_timeout    This parameter set in ms then time the script can alow a thread connecting to a MySQL node to wait, before forcing a returnn.
                      In short if a node will take longer then check_timeout its entry will be not filled and it will eventually ignored in the evaluation.
                      Setting the debug option  =1 and look for [WARN] Check timeout Node ip : Information will tell you how much your nodes are exceeding the allowed limit.
                      You can use the difference to correctly set the check_timeout 
                      Default is 800 ms
   
   --help              help message
   --debug             When active the log will have a lot of information about the execution. Parse it for ERRORS if you have problems
   --print_execution   Active by default, it will print the execution time the check is taking in the log. This can be used to tune properly the scheduler time, and also the --check_timeout
   
   --development      When set to 1 you can run the script in a loop from bash directly and test what is going to happen
   --development_time Time in seconds that the loop wait to execute when in development mode (default 2 seconds)
   
   SSL support
   Now the script identify if the node in the ProxySQL table mysql_servers has use_ssl = 1 and will set SSL to be used for that specific entry.
   This means that SSL connection is by ProxySQL mysql_server entry NOT by IP:port combination.
   
   --ssl_certs_path This parameter allow you to specify a DIRECTORY to use to assign specific certificates.
                    At the moment is NOT possible to change the files names and ALL these 3 files must be there and named as follow:
                     -  client-key.pem
                     -  client-cert.pem
                     -  ca.pem
                     Script will exit with an error if ssl_certs_pathis declared but not filled properly
                     OR if the user running the script doesn't have acces.
   !!NOTE!! SSL connection requires more time to be established. This script is a check that needs to run very fast and constantly.
            force it to use ssl WILL impact in the performance of the check. Tune properly the check_timeout parameter.

=back    
   
=head1 DESCRIPTION

Galera check is a script to manage integration between ProxySQL and Galera (from Codership).
Galera and its implementations like Percona Cluster (PCX), use the data-centric concept, as such the status of a node is relvant in relation to a cluster.

In ProxySQL is possible to represent a cluster and its segments using HostGroups.
Galera check is design to manage a X number of nodes that belong to a given Hostgroup (HG). 
In Galera_check it is also important to qualify the HG in case of use of Replication HG.

galera_check works by HG and as such it will perform isolated actions/checks by HG. 
It is not possible to have more than one check running on the same HG. The check will create a lock file {proxysql_galera_check_${hg}.pid} that will be used by the check to prevent duplicates.

Galera_check will connect to the ProxySQL node and retrieve all the information regarding the Nodes/proxysql configuration. 
It will then check in parallel each node and will retrieve the status and configuration.

At the moment galera_check analyze and manage the following:

Node states: 
  read_only 
  wsrep_status 
  wsrep_rejectqueries 
  wsrep_donorrejectqueries 
  wsrep_connected 
  wsrep_desinccount 
  wsrep_ready 
  wsrep_provider 
  wsrep_segment 
  Number of nodes in by segment
  Retry loop
  
- Number of nodes in by segment
If a node is the only one in a segment, the check will behave accordingly. 
IE if a node is the only one in the MAIN segment, it will not put the node in OFFLINE_SOFT when the node become donor to prevent the cluster to become unavailable for the applications. 
As mention is possible to declare a segment as MAIN, quite useful when managing prod and DR site.

-The check can be configure to perform retry in a X interval. 
Where X is the time define in the ProxySQL scheduler. 
As such if the check is set to have 2 retry for UP and 4 for down, it will loop that number before doing anything. Given that Galera does some action behind the hood.
This feature is very useful in some not well known cases where Galera bhave weird.
IE whenever a node is set to READ_ONLY=1, galera desync and resync the node. 
A check not taking this into account will cause a node to be set OFFLINE and back for no reason.

Another important differentiation for this check is that it use special HGs for maintenance, all in range of 9000. 
So if a node belong to HG 10 and the check needs to put it in maintenance mode, the node will be moved to HG 9010. 
Once all is normal again, the Node will be put back on his original HG.

This check does NOT modify any state of the Nodes. 
Meaning It will NOT modify any variables or settings in the original node. It will ONLY change states in ProxySQL. 
    
The check is still a prototype and is not suppose to go to production (yet).


=over

=item 1

Note that galera_check is also Segment aware, as such the checks on the presence of Writer /reader is done by segment, respecting the MainSegment as primary.

=back

=head1 Configure in ProxySQL


INSERT  INTO scheduler (id,active,interval_ms,filename,arg1) values (10,0,2000,"/var/lib/proxysql/galera_check.pl","-u=remoteUser -p=remotePW -h=192.168.1.50 -H=500:W,501:R -P=6032 --retry_down=2 --retry_up=1 --main_segment=1 --debug=0 --active_failover=1 --single_writer=1 --log=/var/lib/proxysql/galeraLog");
LOAD SCHEDULER TO RUNTIME;SAVE SCHEDULER TO DISK;

To activate it
update scheduler set active=1 where id=10;
LOAD SCHEDULER TO RUNTIME;SAVE SCHEDULER TO DISK;

To update the parameters you must pass all of them not only the ones you want to change(IE enabling debug)
update scheduler set arg1="-u=remoteUser -p=remotePW -h=192.168.1.50 -H=500:W,501:R -P=6032 --retry_down=2 --retry_up=1 --main_segment=1 --debug=1 --active_failover=1 --single_writer=1 --log=/var/lib/proxysql/galeraLog" where id =10;  
LOAD SCHEDULER TO RUNTIME;SAVE SCHEDULER TO DISK;


delete from scheduler where id=10;
LOAD SCHEDULER TO RUNTIME;SAVE SCHEDULER TO DISK;



=head1 Rules:

=over

=item 1

Set to offline_soft :
    
    any non 4 or 2 state, read only =ON
    donor node reject queries - 0 size of cluster > 2 of nodes in the same segments more then one writer, node is NOT read_only.
    
    Changes to pxc_maint_mode to anything else DISABLED

=item 2

change HG t maintenance HG:
    
    Node/cluster in non primary
    wsrep_reject_queries different from NONE
    Donor, node reject queries =1 size of cluster 

=item 3

Node comes back from offline_soft when (all of them):

     1) Node state is 4
     3) wsrep_reject_queries = none
     4) Primary state


=item 4

 Node comes back from maintenance HG when (all of them):

     1) node state is 4
     3) wsrep_reject_queries = none
     4) Primary state


=item 5

 active_failover
      Valid values are:
          0 [default] do not make failover
          1 make failover only if HG 8000 is specified in ProxySQL mysl_servers
          2 use PXC_CLUSTER_VIEW to identify a server in the same segment
          3 do whatever to keep service up also failover to another segment (use PXC_CLUSTER_VIEW) 

=item 6
 PXC_MAIN_MODE is fully supported.
 Any node in a state different from pxc_maint_mode=disabled will be set in OFFLINE_SOFT for all the HostGroup.
 
=item 7
 internally shunning node.
 While I am trying to rely as much as possible on ProxySQL, given few inefficiencies there are cases when I have to set a node to SHUNNED because ProxySQL doesn't recognize it correctly.  
          
=back
=cut	


`
return helpText
}