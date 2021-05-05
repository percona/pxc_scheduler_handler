# pxc_scheduler_handler automated functional tests

## Background
This directory contains tests that can be executed by standard PXC MTR framework.
We found that automating pxc_scheduler_handler tests is needed to make them repeatable and much faster to execute. Standard MTR famework which allows interactions with mysql daemons, clients and arbitrary software is used to achieve this goal.
The idea is to not modify the framework already existing in PXC repository. We provide dedicated MTR suite for pxc_scheduler_handler testing together with several convenience include files for common tasks.

## How to use
1. Install and start ProxySQL service.
Admin credentials: admin/admin
Admin port: 6032
Connection port: 6033
2. Next step is to have PXC binaries,  tests and MTR framework. They can be installed from PXC package or build from sources.
3. Clone `pxc_scheduler_handler` project
4. Inside PXC mtr test suites directory create link to the `mtr/proxysql` directory from `pxc_scheduler_handler`. 
The link should look like:
```
$ pwd
/repo/percona-xtradb-cluster/mysql-test/suite
$ ls -l proxysql
proxysql -> /repo/pxc_scheduler_handler/mtr/proxysql
```
5. Environment variable PXC_SCHEDULER_HANDLER_SCRIPT pointing to `pxc_scheduler_handler` binary:
```
$export PXC_SCHEDULER_HANDLER_SCRIPT=/path/to/pxc_scheduler_handler/<binary>
```
6.Environment variable PXC_SCHEDULER_HANDLER_CONFIG_DIR pointing to the directory where pxc_scheduler_handler config files used by tests are located.
```
export PXC_SCHEDULER_HANDLER_CONFIG_DIR=/repo/percona-xtradb-cluster/mysql-test/suite/proxysql/config
```
7. Execute mtr tests
```
./mtr writer_to_maintenance reader_to_maintenance --parallel=1
```
or
```
./mtr --suite=proxysql --parallel=1
```

## Limitations
1. Tests cannot be executed in parallel because they use the same ProxySQL instance