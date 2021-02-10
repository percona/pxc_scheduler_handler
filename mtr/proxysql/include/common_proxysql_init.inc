--disable_query_log
--error 0, 2003, 2013
--connect(proxysql_admin, 127.0.0.1, admin, admin, , 6032)
if ($mysql_errno) {
  --die "Cannot connect to ProxySQL"
}
--enable_query_log
--source ../include/clean_proxysql_config.inc
--disable_query_log
--connection proxysql_admin
UPDATE global_variables SET Variable_Value='8.0.22' WHERE Variable_name='mysql-server_version';
UPDATE global_variables SET Variable_Value='utf8' WHERE Variable_name='mysql-default_charset';

--enable_query_log

#
# Setup variable 'proxysql_scheduler_script' containing scheduler script path
#
--let _PROXYSQL_SCHEDULER_VAR_IMPORT_FILE = $MYSQL_TMP_DIR/proxysql_scheduler_var_import
--perl
  use strict;
  my $out_file = $ENV{'_PROXYSQL_SCHEDULER_VAR_IMPORT_FILE'} or die "_PROXYSQL_SCHEDULER_VAR_IMPORT_FILE is not set";
  open(OUT_FILE, '>', $out_file) or die("Unable to open out file for writing: $!\n");
  print OUT_FILE "--let \$proxysql_scheduler_script=$ENV{'PROXYSQL_SCHEDULER_SCRIPT'}\n";
  close(OUT_FILE);
EOF
--source $_PROXYSQL_SCHEDULER_VAR_IMPORT_FILE
--remove_file $_PROXYSQL_SCHEDULER_VAR_IMPORT_FILE

if (!$proxysql_scheduler_script) {
  --die "ProxySQL scheduler script not specified. Set env variable PROXYSQL_SCHEDULER_SCRIPT and restart the test" 
}
--file_exists $proxysql_scheduler_script


#
# Temporary solution (we need command line parameters for scheduler script)
#
#
# Setup variable 'proxysql_scheduler_config_dir' containing scheduler script config files
#
--let _PROXYSQL_SCHEDULER_VAR_IMPORT_FILE = $MYSQL_TMP_DIR/proxysql_scheduler_var_import
--perl
  use strict;
  my $out_file = $ENV{'_PROXYSQL_SCHEDULER_VAR_IMPORT_FILE'} or die "_PROXYSQL_SCHEDULER_VAR_IMPORT_FILE is not set";
  open(OUT_FILE, '>', $out_file) or die("Unable to open out file for writing: $!\n");
  print OUT_FILE "--let \$proxysql_scheduler_config_dir=$ENV{'PROXYSQL_SCHEDULER_CONFIG_DIR'}\n";
  close(OUT_FILE);
EOF
--source $_PROXYSQL_SCHEDULER_VAR_IMPORT_FILE
--remove_file $_PROXYSQL_SCHEDULER_VAR_IMPORT_FILE

if (!$proxysql_scheduler_config_dir) {
  --die "ProxySQL scheduler script not specified. Set env variable PROXYSQL_SCHEDULER_CONFIG_DIR and restart the test" 
}

