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
	"strings"
	"testing"

	"pxc_scheduler_handler/internal/cluster"

	"hegel.dev/go/hegel"
)

// ── SQL builder property invariants ──────────────────────────────────────────
//
// None of these methods execute SQL — they return formatted strings.
// Properties therefore focus on structural correctness:
//   - The output always references the supplied hostgroup id, IP and port.
//   - Values injected into the SQL never produce an empty string.
//   - Retry counts embedded in SaveRetry comments are non-negative integers.

func ipAndPortGen() (hegel.Generator[string], hegel.Generator[int]) {
	ips := hegel.SampledFrom([]string{"10.0.0.1", "192.168.1.10", "127.0.0.1"})
	ports := hegel.Integers[int](3306, 3320)
	return ips, ports
}

func hgIdGen() hegel.Generator[int] {
	return hegel.SampledFrom([]int{100, 101, 9100, 9101})
}

// ── MoveNodeUpFromOfflineSoft ─────────────────────────────────────────────────

func TestProp_MoveNodeUpFromOfflineSoft_ContainsHGIpPort(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		pn := newProxySQLNode()
		ipGen, portGen := ipAndPortGen()
		ip := hegel.Draw(ht, ipGen)
		port := hegel.Draw(ht, portGen)
		hg := hegel.Draw(ht, hgIdGen())

		sql := pn.MoveNodeUpFromOfflineSoft(node, hg, ip, port)

		if sql == "" {
			ht.Fatal("MoveNodeUpFromOfflineSoft returned empty SQL")
		}
		if !strings.Contains(sql, strconv.Itoa(hg)) {
			ht.Fatalf("SQL missing hg=%d: %s", hg, sql)
		}
		if !strings.Contains(sql, ip) {
			ht.Fatalf("SQL missing ip=%s: %s", ip, sql)
		}
		if !strings.Contains(sql, strconv.Itoa(port)) {
			ht.Fatalf("SQL missing port=%d: %s", port, sql)
		}
		if !strings.Contains(strings.ToUpper(sql), "ONLINE") {
			ht.Fatalf("SQL does not set status to ONLINE: %s", sql)
		}
	})
}

// ── MoveNodeDownToOfflineSoft ─────────────────────────────────────────────────

func TestProp_MoveNodeDownToOfflineSoft_ContainsHGIpPort(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		pn := newProxySQLNode()
		ipGen, portGen := ipAndPortGen()
		ip := hegel.Draw(ht, ipGen)
		port := hegel.Draw(ht, portGen)
		hg := hegel.Draw(ht, hgIdGen())

		sql := pn.MoveNodeDownToOfflineSoft(node, hg, ip, port)

		if sql == "" {
			ht.Fatal("MoveNodeDownToOfflineSoft returned empty SQL")
		}
		if !strings.Contains(strings.ToUpper(sql), "OFFLINE_SOFT") {
			ht.Fatalf("SQL does not set status to OFFLINE_SOFT: %s", sql)
		}
		if !strings.Contains(sql, strconv.Itoa(hg)) {
			ht.Fatalf("SQL missing hg=%d: %s", hg, sql)
		}
	})
}

// ── MoveNodeDownToHGCange / MoveNodeUpFromHGCange ─────────────────────────────

// The HG in MoveNodeDownToHGCange must be the original hg PLUS MaintenanceHgRange.
func TestProp_MoveNodeDownToHGCange_TargetHGIsMaintenanceOffset(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		pn := newProxySQLNode()
		ipGen, portGen := ipAndPortGen()
		ip := hegel.Draw(ht, ipGen)
		port := hegel.Draw(ht, portGen)
		hg := hegel.Draw(ht, hegel.SampledFrom([]int{100, 101}))

		sql := pn.MoveNodeDownToHGCange(node, hg, ip, port)

		maintHg := hg + pn.Config.Pxcluster.MaintenanceHgRange
		if !strings.Contains(sql, strconv.Itoa(maintHg)) {
			ht.Fatalf("MoveNodeDownToHGCange SQL missing target hg=%d (original=%d maintenance_range=%d): %s",
				maintHg, hg, pn.Config.Pxcluster.MaintenanceHgRange, sql)
		}
	})
}

// The HG in MoveNodeUpFromHGCange must be the original hg MINUS MaintenanceHgRange.
func TestProp_MoveNodeUpFromHGCange_TargetHGIsProductionOffset(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		pn := newProxySQLNode()
		ipGen, portGen := ipAndPortGen()
		ip := hegel.Draw(ht, ipGen)
		port := hegel.Draw(ht, portGen)
		hg := hegel.Draw(ht, hegel.SampledFrom([]int{9100, 9101}))

		sql := pn.MoveNodeUpFromHGCange(node, hg, ip, port)

		prodHg := hg - pn.Config.Pxcluster.MaintenanceHgRange
		if !strings.Contains(sql, strconv.Itoa(prodHg)) {
			ht.Fatalf("MoveNodeUpFromHGCange SQL missing target hg=%d (original=%d): %s", prodHg, hg, sql)
		}
	})
}

// ── DeleteDataNode ────────────────────────────────────────────────────────────

func TestProp_DeleteDataNode_NeverEmpty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		pn := newProxySQLNode()
		ipGen, portGen := ipAndPortGen()
		ip := hegel.Draw(ht, ipGen)
		port := hegel.Draw(ht, portGen)
		hg := hegel.Draw(ht, hgIdGen())

		sql := pn.DeleteDataNode(node, hg, ip, port)

		if sql == "" {
			ht.Fatal("DeleteDataNode returned empty SQL")
		}
		if !strings.Contains(strings.ToUpper(sql), "DELETE") {
			ht.Fatalf("DeleteDataNode SQL missing DELETE keyword: %s", sql)
		}
		if !strings.Contains(sql, strconv.Itoa(hg)) {
			ht.Fatalf("DeleteDataNode SQL missing hg=%d: %s", hg, sql)
		}
		if !strings.Contains(sql, ip) {
			ht.Fatalf("DeleteDataNode SQL missing ip=%s: %s", ip, sql)
		}
	})
}

// ── ResetNodeDefaults ─────────────────────────────────────────────────────────

func TestProp_ResetNodeDefaults_NeverEmpty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		pn := newProxySQLNode()
		ipGen, portGen := ipAndPortGen()
		ip := hegel.Draw(ht, ipGen)
		port := hegel.Draw(ht, portGen)
		hg := hegel.Draw(ht, hgIdGen())

		sql := pn.ResetNodeDefaults(node, hg, ip, port)

		if sql == "" {
			ht.Fatal("ResetNodeDefaults returned empty SQL")
		}
		if !strings.Contains(strings.ToUpper(sql), "UPDATE") {
			ht.Fatalf("ResetNodeDefaults SQL missing UPDATE keyword: %s", sql)
		}
	})
}

// ── InsertRead ────────────────────────────────────────────────────────────────

// For non-new nodes, InsertRead must target the plain reader HG (101).
func TestProp_InsertRead_ExistingNodeTargetsReaderHG(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		node.NodeIsNew = false
		pn := newProxySQLNode()
		ipGen, portGen := ipAndPortGen()
		ip := hegel.Draw(ht, ipGen)
		port := hegel.Draw(ht, portGen)

		sql := pn.InsertRead(node, 101, ip, port)

		expectedHG := pn.MySQLCluster.HgReaderId
		if !strings.Contains(sql, fmt.Sprintf("VALUES(%d,", expectedHG)) {
			ht.Fatalf("InsertRead for existing node: expected HG %d in SQL: %s", expectedHG, sql)
		}
	})
}

// For new nodes, InsertRead must target HgReaderId + MaintenanceHgRange (backup HG).
func TestProp_InsertRead_NewNodeTargetsBackupReaderHG(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		node.NodeIsNew = true
		pn := newProxySQLNode()
		ipGen, portGen := ipAndPortGen()
		ip := hegel.Draw(ht, ipGen)
		port := hegel.Draw(ht, portGen)

		sql := pn.InsertRead(node, 101, ip, port)

		backupHG := pn.MySQLCluster.HgReaderId + pn.Config.Pxcluster.MaintenanceHgRange
		if !strings.Contains(sql, fmt.Sprintf("VALUES(%d,", backupHG)) {
			ht.Fatalf("InsertRead for new node: expected backup HG %d in SQL: %s", backupHG, sql)
		}
	})
}

// InsertRead must always emit an INSERT statement.
func TestProp_InsertRead_AlwaysEmitsInsert(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		pn := newProxySQLNode()
		ipGen, portGen := ipAndPortGen()
		ip := hegel.Draw(ht, ipGen)
		port := hegel.Draw(ht, portGen)

		sql := pn.InsertRead(node, 101, ip, port)

		if !strings.Contains(strings.ToUpper(sql), "INSERT") {
			ht.Fatalf("InsertRead did not emit INSERT: %s", sql)
		}
	})
}

// ── InsertWrite ───────────────────────────────────────────────────────────────

// For non-new nodes, InsertWrite must target the plain writer HG (100).
func TestProp_InsertWrite_ExistingNodeTargetsWriterHG(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		node.NodeIsNew = false
		pn := newProxySQLNode()
		ipGen, portGen := ipAndPortGen()
		ip := hegel.Draw(ht, ipGen)
		port := hegel.Draw(ht, portGen)

		sql := pn.InsertWrite(node, 100, ip, port)

		expectedHG := pn.MySQLCluster.HgWriterId
		if !strings.Contains(sql, fmt.Sprintf("VALUES(%d,", expectedHG)) {
			ht.Fatalf("InsertWrite for existing node: expected HG %d in SQL: %s", expectedHG, sql)
		}
	})
}

// For new nodes, InsertWrite must target HgWriterId + MaintenanceHgRange (backup HG).
func TestProp_InsertWrite_NewNodeTargetsBackupWriterHG(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		node.NodeIsNew = true
		pn := newProxySQLNode()
		ipGen, portGen := ipAndPortGen()
		ip := hegel.Draw(ht, ipGen)
		port := hegel.Draw(ht, portGen)

		sql := pn.InsertWrite(node, 100, ip, port)

		backupHG := pn.MySQLCluster.HgWriterId + pn.Config.Pxcluster.MaintenanceHgRange
		if !strings.Contains(sql, fmt.Sprintf("VALUES(%d,", backupHG)) {
			ht.Fatalf("InsertWrite for new node: expected backup HG %d in SQL: %s", backupHG, sql)
		}
	})
}

// ── SaveRetry ─────────────────────────────────────────────────────────────────

// SaveRetry embeds RetryUp and RetryDown in the comment; the values must
// appear verbatim and be non-negative.
func TestProp_SaveRetry_EmbeddedCountersAreNonNegative(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		node.RetryUp = hegel.Draw(ht, hegel.Integers[int](0, 10))
		node.RetryDown = hegel.Draw(ht, hegel.Integers[int](0, 10))
		pn := newProxySQLNode()
		ipGen, portGen := ipAndPortGen()
		ip := hegel.Draw(ht, ipGen)
		port := hegel.Draw(ht, portGen)

		sql := pn.SaveRetry(node, 100, ip, port)

		if sql == "" {
			ht.Fatal("SaveRetry returned empty SQL")
		}
		upStr := fmt.Sprintf("retry_up=%d", node.RetryUp)
		if !strings.Contains(sql, upStr) {
			ht.Fatalf("SaveRetry SQL missing %q: %s", upStr, sql)
		}
		downStr := fmt.Sprintf("retry_down=%d", node.RetryDown)
		if !strings.Contains(sql, downStr) {
			ht.Fatalf("SaveRetry SQL missing %q: %s", downStr, sql)
		}
	})
}

// SaveRetry must always emit an UPDATE statement.
func TestProp_SaveRetry_AlwaysEmitsUpdate(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := hegel.Draw(ht, nodeGen())
		pn := newProxySQLNode()
		ipGen, portGen := ipAndPortGen()
		ip := hegel.Draw(ht, ipGen)
		port := hegel.Draw(ht, portGen)

		sql := pn.SaveRetry(node, 100, ip, port)

		if !strings.Contains(strings.ToUpper(sql), "UPDATE") {
			ht.Fatalf("SaveRetry did not emit UPDATE: %s", sql)
		}
	})
}

// ── SaveRetry idempotency ─────────────────────────────────────────────────────

// [HIGH] SaveRetry always appends a new retry marker to whatever is in
// dataNode.Comment, even when the comment already contains the marker from a
// previous call. This means calling SaveRetry twice on the same node produces
// a comment with TWO markers — a bug that causes the comment column to grow
// indefinitely and eventually breaks getRetry parsing.
//
// This test DOCUMENTS the current (broken) behaviour: it asserts that after
// two calls the comment contains exactly one marker.  The test is expected to
// FAIL until the bug is fixed by stripping the old marker before appending.
func TestProp_SaveRetry_CommentDoesNotGrow(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		pn := newProxySQLNode()
		ip := "10.0.0.1"
		port := 3306
		hg := pn.MySQLCluster.HgWriterId   // 100
		rid := pn.MySQLCluster.HgReaderId  // 101

		node := cluster.HegelNewDefaultNode(fmt.Sprintf("%s:%d", ip, port))
		node.RetryUp = hegel.Draw(ht, hegel.Integers[int](0, 5))
		node.RetryDown = hegel.Draw(ht, hegel.Integers[int](0, 5))
		node.Comment = ""

		// First call — embeds the retry marker.
		sql1 := pn.SaveRetry(node, hg, ip, port)

		// Extract the comment value that would be stored by executing sql1.
		const pfx = "SET comment='"
		const sfx = "' WHERE"
		s1 := strings.Index(sql1, pfx)
		e1 := strings.LastIndex(sql1, sfx)
		if s1 < 0 || e1 < 0 {
			ht.Fatalf("SaveRetry SQL1 missing expected comment markers: %s", sql1)
		}
		commentAfterFirst := sql1[s1+len(pfx) : e1]

		// Second call — uses the comment that contains the retry marker.
		node.Comment = commentAfterFirst
		sql2 := pn.SaveRetry(node, hg, ip, port)

		s2 := strings.Index(sql2, pfx)
		e2 := strings.LastIndex(sql2, sfx)
		if s2 < 0 || e2 < 0 {
			ht.Fatalf("SaveRetry SQL2 missing expected comment markers: %s", sql2)
		}
		commentAfterSecond := sql2[s2+len(pfx) : e2]

		// The retry marker must appear exactly once after each call.
		marker := fmt.Sprintf("%d_W_%d_R_retry_up=", hg, rid)
		countFirst := strings.Count(commentAfterFirst, marker)
		countSecond := strings.Count(commentAfterSecond, marker)

		if countSecond > countFirst {
			ht.Fatalf(
				"SaveRetry comment grows on repeated calls: after 1st call=%d marker(s), after 2nd call=%d marker(s).\n"+
					"Fix: strip the existing marker before appending the new one.\n"+
					"Comment after 1st: %q\nComment after 2nd: %q",
				countFirst, countSecond, commentAfterFirst, commentAfterSecond,
			)
		}
	})
}

// ── SQL builder robustness ────────────────────────────────────────────────────

// [HIGH] All SQL builders must produce non-empty SQL regardless of what text
// appears in dataNode.Comment.  This is a baseline liveness check; correctness
// of escaping is tested separately.
func TestProp_SQLBuilders_ArbitraryCommentNeverEmpty(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		comment := hegel.Draw(ht, hegel.SampledFrom([]string{
			"", "normal", "  spaces  ", "tab\there", "line\nbreak",
			"unicode: ✓ ← →", "100% safe", "a b c d e f",
		}))
		node := cluster.HegelNewDefaultNode("10.0.0.1:3306")
		node.Comment = comment
		pn := newProxySQLNode()

		sqls := []string{
			pn.InsertRead(node, 101, "10.0.0.1", 3306),
			pn.InsertWrite(node, 100, "10.0.0.1", 3306),
			pn.SaveRetry(node, 100, "10.0.0.1", 3306),
			pn.MoveNodeUpFromOfflineSoft(node, 100, "10.0.0.1", 3306),
			pn.MoveNodeDownToOfflineSoft(node, 100, "10.0.0.1", 3306),
			pn.DeleteDataNode(node, 100, "10.0.0.1", 3306),
			pn.ResetNodeDefaults(node, 100, "10.0.0.1", 3306),
		}
		for i, sql := range sqls {
			if sql == "" {
				ht.Fatalf("SQL builder[%d] returned empty string for comment=%q", i, comment)
			}
		}
	})
}

// [HIGH] None of the SQL builders escape single quotes in dataNode.Comment.
// A comment like "it's here" is interpolated verbatim via fmt.Sprintf, which
// produces broken SQL that would cause a syntax error when executed by ProxySQL.
//
// This test DOCUMENTS the SQL injection surface: it fails when a single quote
// in the comment reaches the SQL string unescaped, making the bug visible in
// the Hegel report as a HIGH-priority finding.  Fix by replacing ' with '' or
// by switching to parameterised MySQL driver calls.
func TestProp_SQLBuilders_SingleQuoteInCommentIsNotEscaped(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		node := cluster.HegelNewDefaultNode("10.0.0.1:3306")
		node.Comment = "it's a trap"
		pn := newProxySQLNode()

		for _, sql := range []string{
			pn.SaveRetry(node, 100, "10.0.0.1", 3306),
			pn.InsertRead(node, 101, "10.0.0.1", 3306),
			pn.InsertWrite(node, 100, "10.0.0.1", 3306),
		} {
			// A correctly-escaped SQL would contain "it''s a trap".
			// Currently the builders produce "it's a trap" — unescaped.
			if strings.Contains(sql, "it's a trap") && !strings.Contains(sql, "it''s a trap") {
				ht.Fatalf(
					"VULNERABILITY: SQL builder embeds single-quoted comment without escaping.\n"+
						"SQL: %s\n"+
						"Fix: escape single quotes (replace ' with '') or use prepared statements.",
					sql,
				)
			}
		}
	})
}
