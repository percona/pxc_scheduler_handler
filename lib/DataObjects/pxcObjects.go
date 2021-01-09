package DataObjects

import log "github.com/sirupsen/logrus"

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
	PxcView PxcClusterView
}

func (node *DataNodePxc) getPxcView(dml string) PxcClusterView{
	recordset, err  := node.DataNodeBase.Connection.Query(dml)
	if err != nil{
		log.Error(err.Error())
	}
		var pxcView PxcClusterView
		for recordset.Next() {
			recordset.Scan(&pxcView.HostName,
				&pxcView.Uuid,
				&pxcView.Status,
				&pxcView.LocalIndex,
				&pxcView.Segment)
		}
	return pxcView

}

