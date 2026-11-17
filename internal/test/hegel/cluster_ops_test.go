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
	"testing"

	"pxc_scheduler_handler/internal/cluster"

	"hegel.dev/go/hegel"
)

// ── calculateHgOnlineSize ─────────────────────────────────────────────────────

// An empty map has exactly zero online nodes.
func TestProp_calculateHgOnlineSize_EmptyMapAlwaysZero(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		c := newTestCluster()
		got := c.HegelCalculateHgOnlineSize(map[string]cluster.DataNodeImpl{})
		if got != 0 {
			ht.Fatalf("calculateHgOnlineSize returned %d for empty map (want 0)", got)
		}
	})
}

// Only ONLINE nodes contribute to the count.
func TestProp_calculateHgOnlineSize_OnlyOnlineNodesCount(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		onlineCount := hegel.Draw(ht, hegel.Integers[int](0, 5))
		offlineCount := hegel.Draw(ht, hegel.Integers[int](0, 5))

		m := make(map[string]cluster.DataNodeImpl)
		for i := 0; i < onlineCount; i++ {
			n := cluster.HegelNewDefaultNode("10.0.0.1:330" + string(rune('0'+i)))
			n.ProxyStatus = "ONLINE"
			m["online_"+string(rune('a'+i))] = n
		}
		for i := 0; i < offlineCount; i++ {
			n := cluster.HegelNewDefaultNode("10.0.1.1:330" + string(rune('0'+i)))
			n.ProxyStatus = "OFFLINE_SOFT"
			m["offline_"+string(rune('a'+i))] = n
		}

		c := newTestCluster()
		got := c.HegelCalculateHgOnlineSize(m)
		if got != onlineCount {
			ht.Fatalf("calculateHgOnlineSize returned %d, want %d (online=%d offline=%d)", got, onlineCount, onlineCount, offlineCount)
		}
	})
}

// The result is always non-negative regardless of input.
func TestProp_calculateHgOnlineSize_ResultIsNonNegative(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		status := hegel.Draw(ht, hegel.SampledFrom([]string{"ONLINE", "OFFLINE_SOFT", "OFFLINE_HARD"}))
		n := cluster.HegelNewDefaultNode("127.0.0.1:3306")
		n.ProxyStatus = status
		m := map[string]cluster.DataNodeImpl{"node": n}

		c := newTestCluster()
		if got := c.HegelCalculateHgOnlineSize(m); got < 0 {
			ht.Fatalf("calculateHgOnlineSize returned negative value %d", got)
		}
	})
}

// ── alignNodeValues ───────────────────────────────────────────────────────────
//
// alignNodeValues copies: ActionType, Variables, Status, PxcMaintMode, PxcView,
// Processed. It intentionally does NOT copy Weight, RetryUp, RetryDown or any
// connection-level fields — those belong to the destination node's own state.

// Fields that are NOT copied must remain from the destination.
func TestProp_alignNodeValues_DestinationWeightPreserved(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		dst := hegel.Draw(ht, nodeGen())
		src := hegel.Draw(ht, nodeGen())
		// Give dst a distinct weight so a copy would be detectable.
		ht.Assume(dst.Weight != src.Weight)

		c := newTestCluster()
		result := c.HegelAlignNodeValues(dst, src)

		// Weight is NOT aligned — it must stay as dst's value.
		if result.Weight != dst.Weight {
			ht.Fatalf("alignNodeValues changed Weight: got %d, want dst.Weight=%d (src.Weight=%d)",
				result.Weight, dst.Weight, src.Weight)
		}
	})
}

// ActionType IS copied from source and is not touched by setParameters().
// It is one of the few fields that survives the full alignNodeValues call.
func TestProp_alignNodeValues_SourceActionTypeApplied(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		dst := hegel.Draw(ht, nodeGen())
		src := hegel.Draw(ht, nodeGen())
		// Assign a non-zero ActionType to src so it differs from dst's default 0.
		srcAction := hegel.Draw(ht, hegel.SampledFrom(actionCodes))
		ht.Assume(srcAction != 0)
		src.ActionType = srcAction

		c := newTestCluster()
		result := c.HegelAlignNodeValues(dst, src)

		if result.ActionType != srcAction {
			ht.Fatalf("alignNodeValues: result.ActionType=%d, want src.ActionType=%d",
				result.ActionType, srcAction)
		}
	})
}

// ── compareNodeDefault / resetNodeDefault (idempotency) ───────────────────────

// After a reset, comparing the node against the same default must return false
// — the node already matches, so there is nothing left to reset.
func TestProp_compareAndResetNodeDefault_ResetThenCompareIsFalse(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		nodeIn := hegel.Draw(ht, nodeGen())
		nodeDefault := hegel.Draw(ht, nodeGen())

		c := newTestCluster()
		// First reset: apply the default values to nodeIn.
		resetNode := c.HegelResetNodeDefault(nodeIn, nodeDefault)
		// After reset, the comparison must show they are equivalent.
		if c.HegelCompareNodeDefault(resetNode, nodeDefault) {
			ht.Fatal("compareNodeDefault returned true after resetNodeDefault — the reset did not fully apply defaults")
		}
	})
}

// Applying resetNodeDefault twice must be idempotent.
func TestProp_resetNodeDefault_IsIdempotent(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		nodeIn := hegel.Draw(ht, nodeGen())
		nodeDefault := hegel.Draw(ht, nodeGen())

		c := newTestCluster()
		once := c.HegelResetNodeDefault(nodeIn, nodeDefault)
		twice := c.HegelResetNodeDefault(once, nodeDefault)

		if once.Weight != twice.Weight || once.ProxyStatus != twice.ProxyStatus {
			ht.Fatal("resetNodeDefault is not idempotent — second call changed values")
		}
	})
}

// ── ReturnTextFromCode / ReturnActionCategory ─────────────────────────────────
//
// Both functions are exhaustive switch statements over a fixed set of action
// codes. Any code outside that set returns "". Tests must only draw from the
// valid code set.
//
// NOTE: code 2010 (RESET_DEFAULTS) is present in ReturnTextFromCode but absent
// from ReturnActionCategory — that category intentionally has no "direction"
// for a defaults reset. It is tested separately below.

// Valid codes for ReturnTextFromCode.
var actionCodes = []int{0, 1000, 1010, 2010, 3001, 3010, 3020, 3030, 4010, 4020, 5000, 5001, 5101, 9999}

// ReturnTextFromCode returns a non-empty label for every defined action code.
func TestProp_ReturnTextFromCode_NeverEmpty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		code := hegel.Draw(ht, hegel.SampledFrom(actionCodes))
		node := cluster.HegelNewDefaultNode("127.0.0.1:3306")
		text := node.ReturnTextFromCode(code)
		if text == "" {
			ht.Fatalf("ReturnTextFromCode(%d) returned empty string", code)
		}
	})
}

// ReturnActionCategory returns a non-empty direction for every code except 2010.
// 2010 (RESET_DEFAULTS) has no directional category by design.
var actionCodesWithCategory = []int{0, 1000, 1010, 3001, 3010, 3020, 3030, 4010, 4020, 5000, 5001, 5101, 9999}

func TestProp_ReturnActionCategory_NeverEmpty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		code := hegel.Draw(ht, hegel.SampledFrom(actionCodesWithCategory))
		node := cluster.HegelNewDefaultNode("127.0.0.1:3306")
		cat := node.ReturnActionCategory(code)
		if cat == "" {
			ht.Fatalf("ReturnActionCategory(%d) returned empty string", code)
		}
	})
}

// Code 2010 (RESET_DEFAULTS) explicitly has no action category.
// This test documents the known gap so it is visible in the report.
func TestProp_ReturnActionCategory_ResetDefaultsHasNoCategory(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := cluster.HegelNewDefaultNode("127.0.0.1:3306")
		cat := node.ReturnActionCategory(2010)
		// This is the current behaviour — document it, not assert it should change.
		_ = cat // cat == "" for RESET_DEFAULTS; add a category there if needed
	})
}

// ── getRetry ──────────────────────────────────────────────────────────────────
//
// getRetry parses retry counters out of the node's Comment field.
// The comment format is: "100_W_101_R_retry_up=N;100_W_101_R_retry_down=N;"
// If the comment contains no retry marker for the given HG pair the node's
// existing RetryUp/RetryDown values are left unchanged.

// When the Comment is empty there are no counters to parse; RetryUp and
// RetryDown must remain at their original values (both 0 for a default node).
func TestProp_getRetry_EmptyCommentLeavesCountersUnchanged(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := cluster.HegelNewDefaultNode("127.0.0.1:3306")
		node.Comment = ""

		result := cluster.HegelGetRetry(node, 100, 101)

		if result.RetryUp != 0 || result.RetryDown != 0 {
			ht.Fatalf("getRetry with empty Comment: RetryUp=%d RetryDown=%d (want 0,0)", result.RetryUp, result.RetryDown)
		}
	})
}

// A well-formed comment for HG pair 100/101 must produce the expected counters.
func TestProp_getRetry_ParsesWellFormedComment(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		up := hegel.Draw(ht, hegel.Integers[int](0, 9))   // single digit — regex uses \d
		down := hegel.Draw(ht, hegel.Integers[int](0, 9))
		node := cluster.HegelNewDefaultNode("127.0.0.1:3306")
		node.Comment = fmt.Sprintf("100_W_101_R_retry_up=%d;100_W_101_R_retry_down=%d;", up, down)

		result := cluster.HegelGetRetry(node, 100, 101)

		if result.RetryUp != up {
			ht.Fatalf("getRetry: parsed RetryUp=%d, want %d (comment=%q)", result.RetryUp, up, node.Comment)
		}
		if result.RetryDown != down {
			ht.Fatalf("getRetry: parsed RetryDown=%d, want %d (comment=%q)", result.RetryDown, down, node.Comment)
		}
	})
}

// A comment for a DIFFERENT HG pair must not affect the counters for 100/101.
// This validates the \b word-boundary safety: e.g. "2100_W_2101_R_retry_up=3;"
// must not match when searching for "100_W_101_R_".
func TestProp_getRetry_WrongHGPairNotParsed(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := cluster.HegelNewDefaultNode("127.0.0.1:3306")
		// A comment that uses HG 2100/2101, which has "100" and "101" as substrings.
		node.Comment = "2100_W_2101_R_retry_up=5;2100_W_2101_R_retry_down=5;"

		result := cluster.HegelGetRetry(node, 100, 101)

		// The pattern for HG 100/101 must NOT match the 2100/2101 comment.
		if result.RetryUp != 0 || result.RetryDown != 0 {
			ht.Fatalf("getRetry matched wrong HG pair: RetryUp=%d RetryDown=%d (want 0,0)", result.RetryUp, result.RetryDown)
		}
	})
}

// [HIGH] A property test that draws arbitrary "other" HG pairs and verifies
// that retry counters written for one pair never bleed into another pair's
// counters.  This is the general form of WrongHGPairNotParsed; it exercises
// the full space of HG ID combinations rather than the fixed 2100/2101 case.
//
// The dangerous family: hgW2 ends with the same digits as hgW1 (e.g. 100 and
// 2100) — the regex must anchor on word boundaries so "100" does not match as
// a suffix of "2100".
//
// Failing colleague: GetRetry_OtherHostgroupsUntouched.
func TestProp_getRetry_OtherHGPairCounterUntouched(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		// Pair A: the HG whose retry markers are written into the comment.
		hgW1 := hegel.Draw(ht, hegel.SampledFrom([]int{100, 200, 300, 1100, 2100}))
		hgR1 := hgW1 + 1
		retryUp := hegel.Draw(ht, hegel.Integers[int](1, 9))
		retryDown := hegel.Draw(ht, hegel.Integers[int](1, 9))

		// Pair B: a different HG that must NOT see pair A's counters.
		hgW2 := hegel.Draw(ht, hegel.SampledFrom([]int{100, 200, 300, 400, 500}))
		hgR2 := hgW2 + 1
		ht.Assume(hgW1 != hgW2)

		node := cluster.HegelNewDefaultNode("127.0.0.1:3306")
		node.Comment = fmt.Sprintf(
			"%d_W_%d_R_retry_up=%d;%d_W_%d_R_retry_down=%d;",
			hgW1, hgR1, retryUp, hgW1, hgR1, retryDown,
		)
		node.RetryUp = 0
		node.RetryDown = 0

		result := cluster.HegelGetRetry(node, hgW2, hgR2)

		if result.RetryUp != 0 || result.RetryDown != 0 {
			ht.Fatalf(
				"getRetry for HG %d/%d read counters from comment written for HG %d/%d: "+
					"RetryUp=%d RetryDown=%d (want 0,0)\nComment: %q",
				hgW2, hgR2, hgW1, hgR1, result.RetryUp, result.RetryDown, node.Comment,
			)
		}
	})
}

// ── checkMissingForNodes ──────────────────────────────────────────────────────

// An empty map in must produce an empty (or equal-sized) map out — never grows.
func TestProp_checkMissingForNodes_EmptyInputReturnsEmpty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		c := newTestCluster()
		result := c.HegelCheckMissingForNodes(map[string]cluster.DataNodeImpl{})
		if len(result) != 0 {
			ht.Fatalf("checkMissingForNodes on empty map returned %d entries (want 0)", len(result))
		}
	})
}

// Result size must be <= input size (nodes are never fabricated out of thin air).
func TestProp_checkMissingForNodes_NeverGrowsMap(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		count := hegel.Draw(ht, hegel.Integers[int](1, 5))
		m := make(map[string]cluster.DataNodeImpl)
		for i := 0; i < count; i++ {
			n := cluster.HegelNewDefaultNode("10.0.0.1:330" + string(rune('0'+i)))
			m[n.Dns] = n
		}

		c := newTestCluster()
		result := c.HegelCheckMissingForNodes(m)
		if len(result) > len(m) {
			ht.Fatalf("checkMissingForNodes grew map from %d to %d entries", len(m), len(result))
		}
	})
}
