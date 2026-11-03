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
