package Pcx


const(
/*
Retrieve main information from Data nodes
 */

	Dml_get_variables = "select * from performance_schema.global_variables"
	Dml_get_status = "select * from performance_schema.global_status"
	Dml_get_pxc_view ="select * from performance_schema.pxc_cluster_view where UUID = '?'"

	Dml_get_ssl_status = "SHOW SESSION STATUS LIKE 'Ssl_cipher'"


)
