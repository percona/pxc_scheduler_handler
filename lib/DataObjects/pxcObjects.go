package DataObjects

import log "github.com/sirupsen/logrus"

type DataNodePxc struct{
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
	WsrepClusterSize		int
	WsrepClusterName		string
	WsrepClusterStatus		string
	WsrepNodeName			string
	HasPrimaryState         bool
	PxcView                 PxcClusterView
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

