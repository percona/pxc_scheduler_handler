package DataObjects

import (
	"database/sql"

	"../Global"
	SQLPxc "../Sql/Pcx"

	log "github.com/sirupsen/logrus"
)

type PxcDataNodeFactory struct {
}

func (f PxcDataNodeFactory) createDataNode() DataNode {
	var node DataNode
	node.PxcNode = new(DataNodePxc)
	return node
}

type DataNodePxc struct {
	PxcMaintMode            string
	WsrepConnected          bool
	WsrepDesinccount        int
	WsrepDonorrejectqueries bool
	WsrepGcommUuid          string
	WsrepLocalIndex         int
	WsrepPcWeight           int
	WsrepProvider           map[string]string
	WsrepReady              bool
	WsrepRejectqueries      bool
	WsrepSegment            int
	WsrepStatus             int
	WsrepClusterSize        int
	WsrepClusterName        string
	WsrepClusterStatus      string
	WsrepNodeName           string
	HasPrimaryState         bool
	PxcView                 PxcClusterView
}

// KH: passing fn can be probably changed to passing the interface
type retriever func(string) map[string]string

func (node *DataNodePxc) GetVariablesInformation(fn retriever, connection *sql.DB) map[string]string {
	return fn(SQLPxc.Dml_get_variables)
}
func (node *DataNodePxc) GetStatusInformation(fn retriever) map[string]string {
	return fn(SQLPxc.Dml_get_status)
}

func (node *DataNodePxc) getPxcView(connection *sql.DB, dml string) PxcClusterView {
	recordset, err := connection.Query(dml)
	if err != nil {
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

func (node *DataNodePxc) setParameters(variables map[string]string, status map[string]string) {
	node.WsrepLocalIndex = node.PxcView.LocalIndex

	node.PxcMaintMode = variables["pxc_maint_mode"]
	node.WsrepConnected = Global.ToBool(status["wsrep_connected"], "ON")
	node.WsrepDesinccount = Global.ToInt(status["wsrep_desync_count"])
	node.WsrepDonorrejectqueries = Global.ToBool(variables["wsrep_sst_donor_rejects_queries"], "OFF")
	node.WsrepGcommUuid = status["wsrep_gcomm_uuid"]
	node.WsrepProvider = Global.FromStringToMAp(variables["wsrep_provider_options"], ";")
	node.HasPrimaryState = Global.ToBool(status["wsrep_cluster_status"], "Primary")

	node.WsrepClusterName = variables["wsrep_cluster_name"]
	node.WsrepClusterStatus = status["wsrep_cluster_status"]
	node.WsrepNodeName = variables["wsrep_node_name"]
	node.WsrepClusterSize = Global.ToInt(status["wsrep_cluster_size"])
	node.WsrepPcWeight = Global.ToInt(node.WsrepProvider["pc.weight"])
	node.WsrepReady = Global.ToBool(status["wsrep_ready"], "on")
	node.WsrepRejectqueries = !Global.ToBool(variables["wsrep_reject_queries"], "none")
	node.WsrepSegment = Global.ToInt(node.WsrepProvider["gmcast.segment"])
	node.WsrepStatus = Global.ToInt(status["wsrep_local_state"])
}
