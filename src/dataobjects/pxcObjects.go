package dataobjects

import (
	"fmt"
	"strings"

	"global"

	SQLPxc "sql/Pcx"

	log "github.com/sirupsen/logrus"
)

type DataNodePxc struct {
	DataNodeBase            DataNode
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

func (node *DataNodePxc) getPxcView(dml string) PxcClusterView {
	recordset, err := node.DataNodeBase.Connection.Query(dml)
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

//We parallelize the information retrieval using goroutine
func (node DataNodePxc) getInfo(wg *global.MyWaitGroup, cluster *DataCluster) int {
	if global.Performance {
		global.SetPerformanceObj(fmt.Sprintf("Get info for node %s", node.DataNodeBase.Dns), true, log.DebugLevel)
	}
	// Get the connection
	node.DataNodeBase.GetConnection()
	/*
		if connection is functioning we try to get the info
		Otherwise we go on and set node as NOT processed
	*/
	// get variables and status first then pxc_view
	if !node.DataNodeBase.NodeTCPDown {
		node.DataNodeBase.Variables = node.DataNodeBase.getNodeInformations("variables")
		node.DataNodeBase.Status = node.DataNodeBase.getNodeInformations("status")
		if node.DataNodeBase.Variables["server_uuid"] != "" {
			node.PxcView = node.getPxcView(strings.ReplaceAll(SQLPxc.Dml_get_pxc_view, "?", node.DataNodeBase.Status["wsrep_gcomm_uuid"]))
		}

		node.DataNodeBase.Processed = true

		//set the specific monitoring parameters
		node.setParameters()
		if global.Performance {
			global.SetPerformanceObj(fmt.Sprintf("Get info for node %s", node.DataNodeBase.Dns), false, log.DebugLevel)
		}
	} else {
		node.DataNodeBase.Processed = false
		log.Warn("Cannot load information (variables/status/pxc_view) for node: ", node.DataNodeBase.Dns)
	}

	cluster.NodesPxc.Store(node.DataNodeBase.Dns, node)
	log.Debug("node ", node.DataNodeBase.Dns, " done")

	// we close the connection as soon as done
	node.DataNodeBase.CloseConnection()

	//We decrease the counter running go routines
	wg.DecreaseCounter()
	return 0
}

//here we set and normalize the parameters coming from different sources for the PXC object
func (node *DataNodePxc) setParameters() {
	node.WsrepLocalIndex = node.PxcView.LocalIndex
	node.PxcMaintMode = node.DataNodeBase.Variables["pxc_maint_mode"]
	node.WsrepConnected = global.ToBool(node.DataNodeBase.Status["wsrep_connected"], "ON")
	node.WsrepDesinccount = global.ToInt(node.DataNodeBase.Status["wsrep_desync_count"])
	node.WsrepDonorrejectqueries = global.ToBool(node.DataNodeBase.Variables["wsrep_sst_donor_rejects_queries"], "ON")
	node.WsrepGcommUuid = node.DataNodeBase.Status["wsrep_gcomm_uuid"]
	node.WsrepProvider = global.FromStringToMAp(node.DataNodeBase.Variables["wsrep_provider_options"], ";")
	node.HasPrimaryState = global.ToBool(node.DataNodeBase.Status["wsrep_cluster_status"], "Primary")

	node.WsrepClusterName = node.DataNodeBase.Variables["wsrep_cluster_name"]
	node.WsrepClusterStatus = node.DataNodeBase.Status["wsrep_cluster_status"]
	node.WsrepNodeName = node.DataNodeBase.Variables["wsrep_node_name"]
	node.WsrepClusterSize = global.ToInt(node.DataNodeBase.Status["wsrep_cluster_size"])
	node.WsrepPcWeight = global.ToInt(node.WsrepProvider["pc.weight"])
	node.WsrepReady = global.ToBool(node.DataNodeBase.Status["wsrep_ready"], "on")
	node.WsrepRejectqueries = !global.ToBool(node.DataNodeBase.Variables["wsrep_reject_queries"], "none")
	node.WsrepSegment = global.ToInt(node.WsrepProvider["gmcast.segment"])
	node.WsrepStatus = global.ToInt(node.DataNodeBase.Status["wsrep_local_state"])
	node.DataNodeBase.ReadOnly = global.ToBool(node.DataNodeBase.Variables["read_only"], "on")

}
