package DataObjects

type DataNodePxc struct{
	DataNodeBase DataNode
	Pxc_maint_mode string
	Wsrep_connected bool
	Wsrep_desinccount int
	Wsrep_donorrejectqueries bool
	Wsrep_gcomm_uuid string
	Wsrep_local_index int
	Wsrep_pc_weight int
	Wsrep_provider map[string]string
	Wsrep_ready  bool
	Wsrep_rejectqueries bool
	Wsrep_segment int
	Wsrep_status int
}
