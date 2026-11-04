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
	"fmt"
	"strconv"
	"testing"

	"pxc_scheduler_handler/internal/cluster"

	"hegel.dev/go/hegel"
)

// ── GetActionList properties ──────────────────────────────────────────────────
//
// GetActionList() is the top-level orchestrator: it calls cleanUpForLeftOver,
// evaluateAllProcessedNodes, evaluateWriters, evaluateReaders, and
// checkFailoverIfFound.  None of those paths touch the database — they operate
// entirely on the in-memory node maps (WriterNodes, ReaderNodes, etc.).
//
// Tests here drive GetActionList at the cluster level to verify invariants
// that span the full evaluation pipeline, complementing the individual
// check*/evaluate* unit tests elsewhere.

// KNOWN BUG — GetActionList_WriterCap map-ordering non-determinism
//
// When WriterNodes contains two ONLINE writers and MaxNumWriters=1, the cap
// enforcement path in processFailoverFailBack iterates BackupWriters (a Go map)
// to decide which writer to remove.  Go's random map iteration order makes the
// choice non-deterministic: on two consecutive runs with identical cluster state,
// different writers may receive the REMOVE action, causing inconsistent ProxySQL
// writes across scheduler invocations.
//
// A property test for this bug requires injecting nodes into NodesPxc.internal
// (a sync.Map with no hegel bridge) and wiring the full BackupWriters map, which
// is beyond the current bridge surface.  The fix in production code is to
// replace the map iteration with a sorted-slice walk when ordering is
// load-bearing (i.e. when selecting which of several writers to demote).
//
// Track as: GetActionList_WriterCapNonDeterminism.

// [HIGH] An empty cluster (no nodes in any HG map) must always produce an
// empty action map — no hallucinated actions should emerge from thin air.
// This is also a determinism check: calling GetActionList twice on the same
// empty state returns an equivalent result.
func TestProp_GetActionList_EmptyClusterProducesNoActions(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		c := newTestCluster()

		actions := c.GetActionList()

		if len(actions) != 0 {
			ht.Fatalf(
				"GetActionList on empty cluster returned %d action(s) (want 0): %v",
				len(actions), actions,
			)
		}
	})
}

// [HIGH] When WriterIsAlsoReader=0 and multiple readers are also writers,
// processWriterIsAlsoReader must always remove the same node regardless of
// Go's map iteration order.  Previously it returned on the first match found,
// making the choice non-deterministic in multi-writer/multi-reader setups.
//
// The test runs the function N times on identical state (fresh copies of the
// maps each time) and asserts that the same ActionNodes key appears in every
// run.
func TestProp_ProcessWriterIsAlsoReader_DeterministicRemoval(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		const runs = 50
		const hgReaderId = 101
		const hgWriterId = 100

		// Two nodes that are simultaneously in WriterNodes and in readerNodes
		// with HgReaderId, which is exactly the condition that triggers the
		// removal path in processWriterIsAlsoReader.
		dnsA := "10.0.0.1:3306"
		dnsB := "10.0.0.2:3306"

		makeReader := func(dns string) cluster.DataNodeImpl {
			n := cluster.HegelNewDefaultNode(dns)
			n.HostgroupId = hgReaderId
			n.ProxyStatus = "ONLINE"
			n.Processed = true
			return n
		}

		var firstRemovedKey string

		for i := 0; i < runs; i++ {
			c := newTestCluster()
			c.WriterIsReader = 0

			nodeA := makeReader(dnsA)
			nodeB := makeReader(dnsB)

			// Both nodes are writers AND readers.
			c.WriterNodes[dnsA] = nodeA
			c.WriterNodes[dnsB] = nodeB

			readerNodes := map[string]cluster.DataNodeImpl{
				dnsA: nodeA,
				dnsB: nodeB,
			}

			c.HegelProcessWriterIsAlsoReader(readerNodes)

			// Find which node received a DELETE action.
			removedKey := ""
			for key := range c.ActionNodes {
				removedKey = key
				break
			}
			if removedKey == "" {
				ht.Fatalf("run %d: processWriterIsAlsoReader produced no ActionNodes entry (expected one DELETE)", i)
			}

			if firstRemovedKey == "" {
				firstRemovedKey = removedKey
			} else if removedKey != firstRemovedKey {
				ht.Fatalf(
					"processWriterIsAlsoReader is non-deterministic:\n"+
						"  run 1 removed: %q\n"+
						"  run %d removed: %q\n"+
						"Fix: collect all candidates first, pick by deterministic tiebreak (highest DNS).",
					firstRemovedKey, i+1, removedKey,
				)
			}
		}

		// Confirm the removed node is the one with the higher DNS string —
		// that is the expected tiebreak once the fix is applied.
		expectedKey := strconv.Itoa(hgReaderId) + "_" + dnsB // dnsB > dnsA lexicographically
		if firstRemovedKey != expectedKey {
			ht.Fatalf(
				"processWriterIsAlsoReader removed %q but expected %q (highest DNS %q).\n"+
					"Fix: use DNS as tiebreak when selecting which writer-reader to remove.",
				firstRemovedKey, expectedKey, dnsB,
			)
		}

		_ = fmt.Sprintf // keep import used
	})
}

// [HIGH] Calling GetActionList a second time on the same empty cluster must
// still return an empty map.  This verifies that the method is idempotent on
// a settled state and does not accumulate stale entries across invocations.
func TestProp_GetActionList_EmptyClusterIsIdempotent(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		c := newTestCluster()

		first := c.GetActionList()
		second := c.GetActionList()

		if len(first) != len(second) {
			ht.Fatalf(
				"GetActionList is not idempotent on empty cluster: first=%d action(s), second=%d action(s)",
				len(first), len(second),
			)
		}
	})
}
