//go:build hegel

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

package hegel

import (
	"testing"

	"pxc_scheduler_handler/internal/cluster"

	"hegel.dev/go/hegel"
)

// ── checkWsrepDesync ──────────────────────────────────────────────────────────

// WsrepStatus != 2 means the node is not donating; desync check must never act.
func TestProp_checkWsrepDesync_NonDonorStatusNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		ht.Assume(node.WsrepStatus != 2)

		c := newTestCluster()
		if c.HegelCheckWsrepDesync(node, hg) {
			ht.Fatalf("checkWsrepDesync returned true for WsrepStatus=%d (only fires on status=2)", node.WsrepStatus)
		}
	})
}

// A ReadOnly node is already routing reads; it must never be sent OFFLINE_SOFT
// by the desync check regardless of HG size.
func TestProp_checkWsrepDesync_ReadOnlyNodeNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.WsrepStatus = 2
		node.ReadOnly = true

		c := newTestCluster()
		if c.HegelCheckWsrepDesync(node, hg) {
			ht.Fatal("checkWsrepDesync acted on ReadOnly=true node")
		}
	})
}

// A node already at OFFLINE_SOFT must not trigger a second move-offline.
func TestProp_checkWsrepDesync_AlreadyOfflineSoftNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.WsrepStatus = 2
		node.ReadOnly = false
		node.ProxyStatus = "OFFLINE_SOFT"

		c := newTestCluster()
		if c.HegelCheckWsrepDesync(node, hg) {
			ht.Fatal("checkWsrepDesync acted on node that is already OFFLINE_SOFT")
		}
	})
}

// A single-node HG must never be taken offline — there is no failover target.
func TestProp_checkWsrepDesync_SingleNodeHGNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		node.WsrepStatus = 2
		node.ReadOnly = false
		node.ProxyStatus = "ONLINE"
		hg := cluster.HegelNewHostgroup(100, 1, "W")

		c := newTestCluster()
		if c.HegelCheckWsrepDesync(node, hg) {
			ht.Fatal("checkWsrepDesync acted with HG size=1 (last node, must never go offline)")
		}
	})
}

// When MaxReplicationLag is configured and recv_queue is below it, the node is
// within tolerance — no offline action should occur.
func TestProp_checkWsrepDesync_RecvQueueBelowLagThresholdNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		lag := hegel.Draw(ht, hegel.Integers[int](1, 1000))
		queue := hegel.Draw(ht, hegel.Integers[int](0, lag-1))
		node := hegel.Draw(ht, nodeGen())
		node.WsrepStatus = 2
		node.ReadOnly = false
		node.ProxyStatus = "ONLINE"
		node.MaxReplicationLag = lag
		node.WsrepLocalRecvQueue = queue
		hg := cluster.HegelNewHostgroup(100, 3, "W")

		c := newTestCluster()
		if c.HegelCheckWsrepDesync(node, hg) {
			ht.Fatalf("checkWsrepDesync acted with recv_queue=%d < MaxReplicationLag=%d (within tolerance)", queue, lag)
		}
	})
}

// ── checkAnyNotReadyStatus ────────────────────────────────────────────────────

// Status 2 (Donor/Desynced) and 4 (Synced) are valid operational states;
// this function must never flag them.
func TestProp_checkAnyNotReadyStatus_ValidStatesNeverFlagged(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.WsrepStatus = hegel.Draw(ht, hegel.SampledFrom([]int{2, 4}))

		c := newTestCluster()
		if c.HegelCheckAnyNotReadyStatus(node, hg) {
			ht.Fatalf("checkAnyNotReadyStatus flagged WsrepStatus=%d (2 and 4 are valid)", node.WsrepStatus)
		}
	})
}

// Status 1 (Joining), 3 (Joined) and 5+ are not production-ready;
// they must always be flagged.
func TestProp_checkAnyNotReadyStatus_UnreadyStatusAlwaysFlagged(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.WsrepStatus = hegel.Draw(ht, hegel.SampledFrom([]int{1, 3, 5}))

		c := newTestCluster()
		if !c.HegelCheckAnyNotReadyStatus(node, hg) {
			ht.Fatalf("checkAnyNotReadyStatus returned false for WsrepStatus=%d (must flag non-{2,4})", node.WsrepStatus)
		}
	})
}

// ── checkNotPrimary ───────────────────────────────────────────────────────────

// Primary status is always safe; checkNotPrimary must never flag it.
func TestProp_checkNotPrimary_PrimaryNeverFlagged(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.WsrepClusterStatus = "Primary"

		c := newTestCluster()
		if c.HegelCheckNotPrimary(node, hg) {
			ht.Fatal("checkNotPrimary flagged a Primary-status node")
		}
	})
}

// Every non-Primary status must be flagged immediately.
func TestProp_checkNotPrimary_NonPrimaryAlwaysFlagged(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.WsrepClusterStatus = hegel.Draw(ht, hegel.SampledFrom([]string{"Non-Primary", "Disconnected", ""}))

		c := newTestCluster()
		if !c.HegelCheckNotPrimary(node, hg) {
			ht.Fatalf("checkNotPrimary returned false for WsrepClusterStatus=%q", node.WsrepClusterStatus)
		}
	})
}

// ── checkRejectQueries ────────────────────────────────────────────────────────

// When WsrepRejectqueries is false the check must always be a no-op.
func TestProp_checkRejectQueries_NoRejectNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.WsrepRejectqueries = false

		c := newTestCluster()
		if c.HegelCheckRejectQueries(node, hg) {
			ht.Fatal("checkRejectQueries returned true with WsrepRejectqueries=false")
		}
	})
}

// Nodes in a backup or maintenance HG (HostgroupId >= ConfigHgRange=8000)
// must be skipped even when WsrepRejectqueries is true.
func TestProp_checkRejectQueries_NonProductionHGNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.WsrepRejectqueries = true
		node.HostgroupId = hegel.Draw(ht, hegel.SampledFrom([]int{8100, 8101, 9100, 9101}))

		c := newTestCluster()
		if c.HegelCheckRejectQueries(node, hg) {
			ht.Fatalf("checkRejectQueries acted on backup/maintenance HG id=%d", node.HostgroupId)
		}
	})
}

// A production node (HostgroupId < 8000) with active reject must always be moved.
func TestProp_checkRejectQueries_ProductionNodeWithRejectAlwaysActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.WsrepRejectqueries = true
		node.HostgroupId = hegel.Draw(ht, hegel.SampledFrom([]int{100, 101}))

		c := newTestCluster()
		if !c.HegelCheckRejectQueries(node, hg) {
			ht.Fatalf("checkRejectQueries returned false for production node HG=%d with reject=true", node.HostgroupId)
		}
	})
}

// ── checkDonorReject ──────────────────────────────────────────────────────────

// Without the donor-reject flag, no action should ever occur.
func TestProp_checkDonorReject_NoDonorFlagNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.WsrepDonorrejectqueries = false

		c := newTestCluster()
		if c.HegelCheckDonorReject(node, hg) {
			ht.Fatal("checkDonorReject returned true with WsrepDonorrejectqueries=false")
		}
	})
}

// Donor-reject only matters while the node is actually donating (WsrepStatus=2).
func TestProp_checkDonorReject_NonDonorStatusNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.WsrepDonorrejectqueries = true
		node.WsrepStatus = hegel.Draw(ht, hegel.SampledFrom([]int{1, 3, 4, 5}))

		c := newTestCluster()
		if c.HegelCheckDonorReject(node, hg) {
			ht.Fatalf("checkDonorReject acted on non-donor WsrepStatus=%d", node.WsrepStatus)
		}
	})
}

// When the HG has only one node there is no other node to serve traffic;
// the donor cannot be taken offline.
func TestProp_checkDonorReject_SingleNodeHGNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		node.WsrepDonorrejectqueries = true
		node.WsrepStatus = 2
		node.HostgroupId = 100
		hg := cluster.HegelNewHostgroup(100, 1, "W")

		c := newTestCluster()
		if c.HegelCheckDonorReject(node, hg) {
			ht.Fatal("checkDonorReject acted with HG size=1 (no failover target)")
		}
	})
}

// ── checkPxcMaint ─────────────────────────────────────────────────────────────

// DISABLED and empty modes mean maintenance is off; the check must be a no-op.
func TestProp_checkPxcMaint_DisabledOrEmptyNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.PxcMaintMode = hegel.Draw(ht, hegel.SampledFrom([]string{"DISABLED", ""}))

		c := newTestCluster()
		if c.HegelCheckPxcMaint(node, hg) {
			ht.Fatalf("checkPxcMaint acted with PxcMaintMode=%q", node.PxcMaintMode)
		}
	})
}

// A node already at OFFLINE_SOFT must not be moved again.
func TestProp_checkPxcMaint_AlreadyOfflineSoftNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.PxcMaintMode = "ENABLED"
		node.ProxyStatus = "OFFLINE_SOFT"
		node.HostgroupId = 100

		c := newTestCluster()
		if c.HegelCheckPxcMaint(node, hg) {
			ht.Fatal("checkPxcMaint acted on an already OFFLINE_SOFT node")
		}
	})
}

// ── checkReadOnly ─────────────────────────────────────────────────────────────

// ReadOnly=false means the node is write-eligible; no action should follow.
func TestProp_checkReadOnly_NotReadOnlyNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.ReadOnly = false

		c := newTestCluster()
		if c.HegelCheckReadOnly(node, hg) {
			ht.Fatal("checkReadOnly returned true for ReadOnly=false")
		}
	})
}

// Only nodes in the writer HG (HostgroupId == HgWriterId=100) are subject to
// this check; all other HGs must be skipped.
func TestProp_checkReadOnly_NonWriterHGNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.ReadOnly = true
		node.HostgroupId = hegel.Draw(ht, hegel.SampledFrom([]int{101, 8100, 8101, 9100, 9101}))

		c := newTestCluster()
		if c.HegelCheckReadOnly(node, hg) {
			ht.Fatalf("checkReadOnly acted on non-writer HG id=%d", node.HostgroupId)
		}
	})
}

// ── checkBackOffline ──────────────────────────────────────────────────────────

// The node must be sitting at OFFLINE_SOFT before it can come back online.
func TestProp_checkBackOffline_NotOfflineSoftNeverComeBack(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.ProxyStatus = hegel.Draw(ht, hegel.SampledFrom([]string{"ONLINE", "OFFLINE_HARD"}))

		c := newTestCluster()
		if _, ok := c.HegelCheckBackOffline(node, hg); ok {
			ht.Fatalf("checkBackOffline returned true for ProxyStatus=%q (must be OFFLINE_SOFT)", node.ProxyStatus)
		}
	})
}

// Only a fully synced node (WsrepStatus=4) is eligible to come back.
func TestProp_checkBackOffline_NonSyncedStatusNeverComeBack(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.ProxyStatus = "OFFLINE_SOFT"
		node.HostgroupId = 100
		node.WsrepStatus = hegel.Draw(ht, hegel.SampledFrom([]int{1, 2, 3, 5}))

		c := newTestCluster()
		if _, ok := c.HegelCheckBackOffline(node, hg); ok {
			ht.Fatalf("checkBackOffline returned true for WsrepStatus=%d (only 4 allowed)", node.WsrepStatus)
		}
	})
}

// Active reject_queries blocks the return path unconditionally.
func TestProp_checkBackOffline_RejectQueriesBlocksReturn(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.ProxyStatus = "OFFLINE_SOFT"
		node.WsrepStatus = 4
		node.WsrepClusterStatus = "Primary"
		node.PxcMaintMode = "DISABLED"
		node.WsrepRejectqueries = true
		node.HostgroupId = 100

		c := newTestCluster()
		if _, ok := c.HegelCheckBackOffline(node, hg); ok {
			ht.Fatal("checkBackOffline returned true with WsrepRejectqueries=true")
		}
	})
}

// ── checkBackNew ──────────────────────────────────────────────────────────────

// An already-known node (NodeIsNew=false) must never be re-inserted.
func TestProp_checkBackNew_KnownNodeNeverInserted(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		node.NodeIsNew = false

		c := newTestCluster()
		if c.HegelCheckBackNew(node) {
			ht.Fatal("checkBackNew returned true for NodeIsNew=false")
		}
	})
}

// A node already in a maintenance HG (HostgroupId >= 9000) must not be
// re-inserted there.
func TestProp_checkBackNew_MaintenanceHGNodeNeverInserted(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		node.NodeIsNew = true
		node.HostgroupId = hegel.Draw(ht, hegel.SampledFrom([]int{9100, 9101}))

		c := newTestCluster()
		if c.HegelCheckBackNew(node) {
			ht.Fatalf("checkBackNew acted on node already in maintenance HG id=%d", node.HostgroupId)
		}
	})
}

// ── checkBackPrimary ──────────────────────────────────────────────────────────

// Only nodes parked in a maintenance HG (HostgroupId >= 9000) can be promoted.
func TestProp_checkBackPrimary_ProductionHGNodeNeverPromoted(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.HostgroupId = hegel.Draw(ht, hegel.SampledFrom([]int{100, 101, 8100, 8101}))

		c := newTestCluster()
		if c.HegelCheckBackPrimary(node, hg) {
			ht.Fatalf("checkBackPrimary promoted node from non-maintenance HG id=%d", node.HostgroupId)
		}
	})
}

// WsrepStatus must be 4 (Synced) before a node can re-join production.
func TestProp_checkBackPrimary_UnsyncedNodeNeverPromoted(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.HostgroupId = hegel.Draw(ht, hegel.SampledFrom([]int{9100, 9101}))
		node.WsrepStatus = hegel.Draw(ht, hegel.SampledFrom([]int{1, 2, 3, 5}))

		c := newTestCluster()
		if c.HegelCheckBackPrimary(node, hg) {
			ht.Fatalf("checkBackPrimary promoted node with WsrepStatus=%d (must be 4)", node.WsrepStatus)
		}
	})
}

// ── checkBackDesyncButUnderReplicaLag ─────────────────────────────────────────

// Without MaxReplicationLag configured, desync tolerance cannot be computed.
func TestProp_checkBackDesync_ZeroMaxReplicationLagNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.WsrepStatus = 2
		node.MaxReplicationLag = 0

		c := newTestCluster()
		if c.HegelCheckBackDesyncButUnderReplicaLag(node, hg) {
			ht.Fatal("checkBackDesyncButUnderReplicaLag acted with MaxReplicationLag=0 (cannot compute tolerance)")
		}
	})
}

// When recv_queue >= MaxReplicationLag/2 the lag is too high for a comeback.
func TestProp_checkBackDesync_HighQueueNeverActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		lag := hegel.Draw(ht, hegel.Integers[int](2, 1000))
		queue := hegel.Draw(ht, hegel.Integers[int](lag/2, lag*2))
		node := hegel.Draw(ht, nodeGen())
		node.WsrepStatus = 2
		node.ProxyStatus = "OFFLINE_SOFT"
		node.WsrepClusterStatus = "Primary"
		node.PxcMaintMode = "DISABLED"
		node.WsrepRejectqueries = false
		node.MaxReplicationLag = lag
		node.WsrepLocalRecvQueue = queue
		node.HostgroupId = 100
		hg := cluster.HegelNewHostgroup(101, 2, "R")

		c := newTestCluster()
		if c.HegelCheckBackDesyncButUnderReplicaLag(node, hg) {
			ht.Fatalf("checkBackDesyncButUnderReplicaLag acted with recv_queue=%d >= lag/2=%d", queue, lag/2)
		}
	})
}

// ── checkActiveFailover ───────────────────────────────────────────────────────

func TestProp_checkActiveFailover_ZeroOrNegativeDisables(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		af := hegel.Draw(ht, hegel.Integers[int](-10, 0))
		c := newTestCluster()
		c.ActiveFailover = af
		if c.HegelCheckActiveFailover() {
			ht.Fatalf("checkActiveFailover returned true for ActiveFailover=%d", af)
		}
	})
}

func TestProp_checkActiveFailover_PositiveEnables(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		af := hegel.Draw(ht, hegel.Integers[int](1, 10))
		c := newTestCluster()
		c.ActiveFailover = af
		if !c.HegelCheckActiveFailover() {
			ht.Fatalf("checkActiveFailover returned false for ActiveFailover=%d", af)
		}
	})
}

// ── checkUpSaveRetry ──────────────────────────────────────────────────────────

// When no retries are pending, there is nothing to persist.
func TestProp_checkUpSaveRetry_NoRetryCountersNeverSaves(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.RetryUp = 0
		node.RetryDown = 0
		node.ActionType = 0 // NOTHING_TO_DO

		c := newTestCluster()
		if c.HegelCheckUpSaveRetry(node, hg) {
			ht.Fatal("checkUpSaveRetry saved retry with no counters set")
		}
	})
}

// When ActionType is already non-zero (node already being acted on), retry
// reset must not be triggered.
func TestProp_checkUpSaveRetry_ActiveActionNeverSaves(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.RetryUp = hegel.Draw(ht, hegel.Integers[int](1, 5))
		node.ActionType = hegel.Draw(ht, hegel.SampledFrom([]int{3001, 3010, 3020, 1000, 1010}))

		c := newTestCluster()
		if c.HegelCheckUpSaveRetry(node, hg) {
			ht.Fatalf("checkUpSaveRetry saved retry for node with active ActionType=%d", node.ActionType)
		}
	})
}
