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

// Package hegel — missing-coverage tests identified from README gap analysis.
// Priority tags:
//   [HIGH]   — failure here means a critical cluster safety invariant is broken
//   [MEDIUM] — failure here means an important management feature misbehaves

package hegel

import (
	"testing"

	"pxc_scheduler_handler/internal/cluster"

	"hegel.dev/go/hegel"
)

// ═══════════════════════════════════════════════════════════════════════════════
// [HIGH] checkWsrepDesync — positive action paths
// README: "wsrep_status=2 and another node in HG → OFFLINE_SOFT"
// ═══════════════════════════════════════════════════════════════════════════════

// Without MaxReplicationLag, any donor in a multi-node HG must always be moved.
func TestProp_checkWsrepDesync_MultiNodeNoLagAlwaysActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		node.WsrepStatus = 2
		node.ReadOnly = false
		node.ProxyStatus = "ONLINE"
		node.MaxReplicationLag = 0
		hg := cluster.HegelNewHostgroup(100, hegel.Draw(ht, hegel.Integers[int](2, 10)), "W")

		c := newTestCluster()
		if !c.HegelCheckWsrepDesync(node, hg) {
			ht.Fatalf("checkWsrepDesync did not act for donor (status=2) in multi-node HG (size=%d) with no lag tolerance", hg.Size)
		}
	})
}

// When recv_queue EXCEEDS max_replication_lag the donor must be moved offline.
func TestProp_checkWsrepDesync_RecvQueueExceedsLagAlwaysActs(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		lag := hegel.Draw(ht, hegel.Integers[int](1, 500))
		queue := hegel.Draw(ht, hegel.Integers[int](lag+1, lag*3))
		node := hegel.Draw(ht, nodeGen())
		node.WsrepStatus = 2
		node.ReadOnly = false
		node.ProxyStatus = "ONLINE"
		node.MaxReplicationLag = lag
		node.WsrepLocalRecvQueue = queue
		hg := cluster.HegelNewHostgroup(100, hegel.Draw(ht, hegel.Integers[int](2, 10)), "W")

		c := newTestCluster()
		if !c.HegelCheckWsrepDesync(node, hg) {
			ht.Fatalf("checkWsrepDesync did not act for donor with recv_queue=%d > max_replication_lag=%d", queue, lag)
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════
// [HIGH] checkBackDesyncButUnderReplicaLag — positive comeback path
// README: "node comes back when recv_queue < max_replication_lag/2"
// ═══════════════════════════════════════════════════════════════════════════════

// When all come-back conditions are met and recv_queue is below half-lag,
// the node must be scheduled to return to ONLINE.
func TestProp_checkBackDesync_QueueBelowHalfLagAlwaysComesBack(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		lag := hegel.Draw(ht, hegel.Integers[int](4, 1000)) // ≥4 so lag/2 ≥ 2
		queue := hegel.Draw(ht, hegel.Integers[int](0, (lag/2)-1))
		node := hegel.Draw(ht, nodeGen())
		node.WsrepStatus = 2
		node.ProxyStatus = "OFFLINE_SOFT"
		node.WsrepClusterStatus = "Primary"
		node.PxcMaintMode = "DISABLED"
		node.WsrepRejectqueries = false
		node.MaxReplicationLag = lag
		node.WsrepLocalRecvQueue = queue
		// Must be a production HG (< MaintenanceHgRange=9000)
		node.HostgroupId = hegel.Draw(ht, hegel.SampledFrom([]int{100, 101}))
		hg := cluster.HegelNewHostgroup(node.HostgroupId, 2, "W")

		c := newTestCluster()
		if !c.HegelCheckBackDesyncButUnderReplicaLag(node, hg) {
			ht.Fatalf("checkBackDesyncButUnderReplicaLag did not allow comeback: recv_queue=%d, lag=%d, half=%d", queue, lag, lag/2)
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════
// [HIGH] checkBackOffline — positive comeback path
// README: "node comes back from OFFLINE_SOFT when wsrep=4, Primary, DISABLED, no reject"
// ═══════════════════════════════════════════════════════════════════════════════

// All comeback conditions met → checkBackOffline must return true.
func TestProp_checkBackOffline_AllConditionsMetAllowsReturn(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		node.WsrepStatus = 4
		node.ProxyStatus = "OFFLINE_SOFT"
		node.WsrepClusterStatus = "Primary"
		node.PxcMaintMode = "DISABLED"
		node.WsrepRejectqueries = false
		node.ReadOnly = false
		// HostgroupId must be < ConfigHgRange (8000)
		node.HostgroupId = hegel.Draw(ht, hegel.SampledFrom([]int{100, 101}))
		hg := cluster.HegelNewHostgroup(node.HostgroupId, 2, "W")

		c := newTestCluster()
		if _, ok := c.HegelCheckBackOffline(node, hg); !ok {
			ht.Fatal("checkBackOffline returned false when all comeback conditions are satisfied")
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════
// [HIGH] checkUpSaveRetry — positive save path
// README: "when node stabilises with non-zero counters they are reset and saved"
// ═══════════════════════════════════════════════════════════════════════════════

// ActionType=0 (NOTHING_TO_DO) with non-zero RetryUp must trigger a save.
func TestProp_checkUpSaveRetry_NonZeroRetryUpTriggersReset(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.ActionType = 0
		node.RetryUp = hegel.Draw(ht, hegel.Integers[int](1, 10))
		node.RetryDown = 0

		c := newTestCluster()
		if !c.HegelCheckUpSaveRetry(node, hg) {
			ht.Fatalf("checkUpSaveRetry returned false with ActionType=0 RetryUp=%d (must save retry)", node.RetryUp)
		}
	})
}

// ActionType=0 with non-zero RetryDown must also trigger a save.
func TestProp_checkUpSaveRetry_NonZeroRetryDownTriggersReset(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		hg := hegel.Draw(ht, hostgroupGen())
		node.ActionType = 0
		node.RetryUp = 0
		node.RetryDown = hegel.Draw(ht, hegel.Integers[int](1, 10))

		c := newTestCluster()
		if !c.HegelCheckUpSaveRetry(node, hg) {
			ht.Fatalf("checkUpSaveRetry returned false with ActionType=0 RetryDown=%d (must save retry)", node.RetryDown)
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════
// [HIGH] cleanWriters — non-ONLINE writer must be removed
// README: "fail-over occurs whenever a Primary writer node needs to be demoted"
// ═══════════════════════════════════════════════════════════════════════════════

// A writer that is not ONLINE must be removed from WriterNodes and cleanWriters
// must return true to signal a change occurred.
func TestProp_cleanWriters_NonOnlineWriterIsRemoved(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := cluster.HegelNewDefaultNode("10.0.0.1:3306")
		node.HostgroupId = 100
		node.ProxyStatus = hegel.Draw(ht, hegel.SampledFrom([]string{"OFFLINE_SOFT", "OFFLINE_HARD"}))

		c := newTestCluster()
		c.WriterNodes["10.0.0.1:3306"] = node

		if !c.HegelCleanWriters() {
			ht.Fatalf("cleanWriters returned false after a non-ONLINE writer (status=%q) was present", node.ProxyStatus)
		}
		if _, still := c.WriterNodes["10.0.0.1:3306"]; still {
			ht.Fatal("cleanWriters did not remove the non-ONLINE writer from WriterNodes")
		}
	})
}

// When all writers are gone and ActiveFailover > 0, RequireFailover must be set
// and HasPrimary must be false — this is what triggers the failover path.
func TestProp_cleanWriters_NoWritersWithFailoverTriggersFailoverFlag(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		af := hegel.Draw(ht, hegel.Integers[int](1, 3))
		node := cluster.HegelNewDefaultNode("10.0.0.1:3306")
		node.HostgroupId = 100
		node.ProxyStatus = "OFFLINE_SOFT"

		c := newTestCluster()
		c.ActiveFailover = af
		c.WriterNodes["10.0.0.1:3306"] = node

		c.HegelCleanWriters()

		if !c.RequireFailover {
			ht.Fatalf("cleanWriters: RequireFailover not set after last writer removed with ActiveFailover=%d", af)
		}
		if c.HasPrimary {
			ht.Fatal("cleanWriters: HasPrimary still true after all writers removed")
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════
// [MEDIUM] checkFailoverIfFound — failover candidate detection
// README: "if no node found for fail-over in main segment → critical error"
// ═══════════════════════════════════════════════════════════════════════════════

// No failover required → always true (healthy operation).
func TestProp_checkFailoverIfFound_NoFailoverRequiredAlwaysTrue(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		c := newTestCluster()
		c.RequireFailover = false
		if !c.HegelCheckFailoverIfFound() {
			ht.Fatal("checkFailoverIfFound returned false when RequireFailover=false")
		}
	})
}

// Failover required but a candidate node was found (FailOverNode.HostgroupId > 0)
// → true; a valid replacement exists.
func TestProp_checkFailoverIfFound_FailoverRequiredWithCandidateIsTrue(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		hgId := hegel.Draw(ht, hegel.Integers[int](1, 9999))
		c := newTestCluster()
		c.RequireFailover = true
		// FailOverNode.HostgroupId != 0 means a candidate was identified.
		candidate := cluster.HegelNewDefaultNode("10.0.0.2:3306")
		candidate.HostgroupId = hgId
		c.FailOverNode = candidate

		if !c.HegelCheckFailoverIfFound() {
			ht.Fatalf("checkFailoverIfFound returned false with a valid FailOverNode (hg=%d)", hgId)
		}
	})
}

// Failover required, no writers, no failover candidate → false (cluster danger).
func TestProp_checkFailoverIfFound_NoWritersNoCandidateReturnsFalse(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		c := newTestCluster()
		c.RequireFailover = true
		// WriterNodes is empty by default from newTestCluster.
		// FailOverNode.HostgroupId == 0 means no candidate was found.
		c.FailOverNode = cluster.DataNodeImpl{} // zero value → HostgroupId=0

		if c.HegelCheckFailoverIfFound() {
			ht.Fatal("checkFailoverIfFound returned true with RequireFailover=true, no writers, no failover candidate")
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════
// [MEDIUM] processWriterIsAlsoReader — writerIsAlsoReader option
// README: "writerIsAlsoReader=0 → writer excluded from reader HG when > 1 readers exist"
// ═══════════════════════════════════════════════════════════════════════════════

// WriterIsReader=1 (default) → writer is always kept in reader HG, no action.
func TestProp_processWriterIsAlsoReader_WriterIsReaderOneNeverRemoves(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		writerDns := "10.0.0.1:3306"
		writer := cluster.HegelNewDefaultNode(writerDns)
		writer.HostgroupId = 101 // in reader HG

		reader2 := cluster.HegelNewDefaultNode("10.0.0.2:3306")
		reader2.HostgroupId = 101

		readerNodes := map[string]cluster.DataNodeImpl{
			writerDns:        writer,
			"10.0.0.2:3306": reader2,
		}

		c := newTestCluster()
		c.WriterIsReader = 1
		c.WriterNodes[writerDns] = writer

		if c.HegelProcessWriterIsAlsoReader(readerNodes) {
			ht.Fatal("processWriterIsAlsoReader acted with WriterIsReader=1 (writer must stay in reader HG)")
		}
	})
}

// WriterIsReader=0 and > 1 readers → writer must be scheduled for removal from reader HG.
func TestProp_processWriterIsAlsoReader_WriterNotReaderWithMultipleReadersRemoves(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		writerDns := "10.0.0.1:3306"
		writer := cluster.HegelNewDefaultNode(writerDns)
		writer.HostgroupId = 101

		reader2 := cluster.HegelNewDefaultNode("10.0.0.2:3306")
		reader2.HostgroupId = 101

		readerNodes := map[string]cluster.DataNodeImpl{
			writerDns:        writer,
			"10.0.0.2:3306": reader2,
		}

		c := newTestCluster()
		c.WriterIsReader = 0
		c.WriterNodes[writerDns] = writer

		if !c.HegelProcessWriterIsAlsoReader(readerNodes) {
			ht.Fatal("processWriterIsAlsoReader did not remove writer from reader HG with WriterIsReader=0 and 2 readers")
		}
	})
}

// WriterIsReader=0 but only 1 reader left (the writer itself) → must NOT remove it
// to avoid leaving the reader HG completely empty.
func TestProp_processWriterIsAlsoReader_WriterNotReaderLastReaderNeverRemoves(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		writerDns := "10.0.0.1:3306"
		writer := cluster.HegelNewDefaultNode(writerDns)
		writer.HostgroupId = 101

		readerNodes := map[string]cluster.DataNodeImpl{
			writerDns: writer, // only one reader
		}

		c := newTestCluster()
		c.WriterIsReader = 0
		c.WriterNodes[writerDns] = writer

		if c.HegelProcessWriterIsAlsoReader(readerNodes) {
			ht.Fatal("processWriterIsAlsoReader removed the last reader (the writer itself) — reader HG would be empty")
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════
// [MEDIUM] identifyLowerNodeToRemove — failover candidate selection
// README: "scheduler chooses the next node with highest weight in the 8XXX HG"
// (equivalent: the LOWEST-weight current writer is removed when a new one comes in)
// ═══════════════════════════════════════════════════════════════════════════════

// Single writer in WriterNodes: the function identifies the node as its own
// "lower" (no better candidate exists) and returns false.
func TestProp_identifyLowerNodeToRemove_SingleWriterReturnsFalse(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := cluster.HegelNewDefaultNode("10.0.0.1:3306")
		node.HostgroupId = 100
		node.Weight = hegel.Draw(ht, hegel.Integers[int](1, 10000))

		c := newTestCluster()
		c.WriterNodes["10.0.0.1:3306"] = node

		if c.HegelIdentifyLowerNodeToRemove(node) {
			ht.Fatal("identifyLowerNodeToRemove returned true for a single writer (no lower node exists)")
		}
	})
}

// With two writers at different weights, the lower-weight node must be scheduled
// for removal and the function must return true.
func TestProp_identifyLowerNodeToRemove_LowerWeightWriterScheduledForRemoval(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		highWeight := hegel.Draw(ht, hegel.Integers[int](2, 10000))
		lowWeight := hegel.Draw(ht, hegel.Integers[int](1, highWeight-1))

		highNode := cluster.HegelNewDefaultNode("10.0.0.1:3306")
		highNode.HostgroupId = 100
		highNode.Weight = highWeight

		lowNode := cluster.HegelNewDefaultNode("10.0.0.2:3306")
		lowNode.HostgroupId = 100
		lowNode.Weight = lowWeight

		c := newTestCluster()
		c.WriterNodes["10.0.0.1:3306"] = highNode
		c.WriterNodes["10.0.0.2:3306"] = lowNode

		// Call with the high-weight node as the incoming candidate.
		if !c.HegelIdentifyLowerNodeToRemove(highNode) {
			ht.Fatalf("identifyLowerNodeToRemove returned false: should have scheduled removal of lower-weight node (high=%d, low=%d)", highWeight, lowWeight)
		}
	})
}

// ═══════════════════════════════════════════════════════════════════════════════
// [MEDIUM] processUpAndDownReaders — reader HG management
// README: "if the last reader disappears the writer is temporarily promoted"
// ═══════════════════════════════════════════════════════════════════════════════

// A reader node with a DOWN action must be removed from readerNodes.
func TestProp_processUpAndDownReaders_DownActionRemovesReader(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		dns := "10.0.0.1:3306"
		node := cluster.HegelNewDefaultNode(dns)
		node.HostgroupId = 101
		node.ActionType = 3010 // MOVE_DOWN_OFFLINE → category "DOWN"

		actionNodes := map[string]cluster.DataNodeImpl{dns + "_key": node}
		readerNodes := map[string]cluster.DataNodeImpl{dns: node}

		c := newTestCluster()
		// Register HG 101 as type "R" so the function matches the condition.
		c.Hostgroups[101] = cluster.HegelNewHostgroup(101, 2, "R")

		if !c.HegelProcessUpAndDownReaders(actionNodes, readerNodes) {
			ht.Fatal("processUpAndDownReaders returned false for a DOWN reader action")
		}
		if _, still := readerNodes[dns]; still {
			ht.Fatal("processUpAndDownReaders did not remove DOWN reader from readerNodes")
		}
	})
}

// A reader node with an UP action that is Processed+ONLINE must be added to readerNodes.
func TestProp_processUpAndDownReaders_UpActionAddsReader(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		dns := "10.0.0.2:3306"
		node := cluster.HegelNewDefaultNode(dns)
		node.HostgroupId = 101
		node.ActionType = 1000 // MOVE_UP_OFFLINE → category "UP"
		node.Processed = true
		node.ProxyStatus = "ONLINE"

		actionNodes := map[string]cluster.DataNodeImpl{dns + "_key": node}
		readerNodes := map[string]cluster.DataNodeImpl{}

		c := newTestCluster()
		c.Hostgroups[101] = cluster.HegelNewHostgroup(101, 1, "R")

		if !c.HegelProcessUpAndDownReaders(actionNodes, readerNodes) {
			ht.Fatal("processUpAndDownReaders returned false for an UP reader action")
		}
		if _, found := readerNodes[dns]; !found {
			ht.Fatal("processUpAndDownReaders did not add UP reader to readerNodes")
		}
	})
}

// No action nodes → always returns false (nothing to process).
func TestProp_processUpAndDownReaders_EmptyActionNodesReturnsFalse(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		c := newTestCluster()
		readerNodes := map[string]cluster.DataNodeImpl{}

		if c.HegelProcessUpAndDownReaders(map[string]cluster.DataNodeImpl{}, readerNodes) {
			ht.Fatal("processUpAndDownReaders returned true for empty actionNodes")
		}
	})
}
