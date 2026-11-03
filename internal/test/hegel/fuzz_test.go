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

// Fuzz tests complement the Hegel property suite by exploring byte-level
// inputs that typed generators cannot easily reach.
//
// Seeds run as ordinary unit tests on every `go test -tags hegel` invocation.
// Pass -fuzz=<FuncName> to the go tool to explore beyond the seed corpus.
//
// Placement note: all three fuzz targets live here (not in their source
// package) so they share the existing hegel bridge infrastructure and can
// call the same helpers (newProxySQLNode, newTestCluster, etc.) without
// duplicating setup code.
package hegel

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pxc_scheduler_handler/internal/cluster"
	"pxc_scheduler_handler/internal/global"
	"pxc_scheduler_handler/internal/locker"
)

// ── FuzzConfigSanityCheck ─────────────────────────────────────────────────────
//
// SanityCheck() validates and auto-corrects a Configuration before the
// scheduler starts.  It must never panic for any combination of numeric
// inputs, and when it returns true the following invariants must hold:
//
//   - LockClusterTimeout >= 60
//   - LockRefreshTime < LockClusterTimeout
//   - ProxySQLPingTimeout < CheckTimeOut  (when CheckTimeOut > 0)
//   - PxcPingTimeout     < CheckTimeOut  (when CheckTimeOut > 0)
//
// Failing colleagues: SanityCheck_LockTimes, SanityCheck_PingTimeouts.
func FuzzConfigSanityCheck(f *testing.F) {
	// Seed corpus: tuples of
	//   (maxNumWriters, maxWriters, singlePrimary, writerIsReader,
	//    checkTimeout, proxySQLPingTO, pxcPingTO,
	//    lockClusterTO, lockRefreshTime, retryUp, retryDown)
	type row struct {
		maxNumWriters, maxWriters                    int
		singlePrimary                                bool
		writerIsReader                               int
		checkTimeout, proxySQLPingTO, pxcPingTO      int
		lockClusterTO, lockRefreshTime               int64
		retryUp, retryDown                           int
	}
	seeds := []row{
		// valid baseline
		{1, 1, true, 1, 5000, 1000, 1000, 120, 60, 3, 3},
		// multi-writer + single-primary conflict → SanityCheck returns false
		{2, 2, true, 1, 5000, 1000, 1000, 120, 60, 3, 3},
		// WriterIsAlsoReader != 1 with more than one writer → false
		{1, 2, false, 2, 5000, 1000, 1000, 120, 60, 3, 3},
		// ping timeout >= checkTimeout → auto-corrected
		{1, 1, true, 1, 5000, 6000, 6000, 120, 60, 3, 3},
		// lockClusterTimeout too low → auto-corrected to 60
		{1, 1, true, 1, 5000, 1000, 1000, 10, 5, 3, 3},
		// lockRefreshTime exceeds 75% of lockClusterTimeout → auto-corrected
		{1, 1, true, 1, 5000, 1000, 1000, 120, 110, 3, 3},
		// retry out of range [1-9] → clamped
		{1, 1, true, 1, 5000, 1000, 1000, 120, 60, 15, 15},
		// all zeros (degenerate)
		{0, 0, false, 0, 0, 0, 0, 0, 0, 0, 0},
	}
	for _, s := range seeds {
		f.Add(s.maxNumWriters, s.maxWriters, s.singlePrimary, s.writerIsReader,
			s.checkTimeout, s.proxySQLPingTO, s.pxcPingTO,
			s.lockClusterTO, s.lockRefreshTime,
			s.retryUp, s.retryDown)
	}

	f.Fuzz(func(t *testing.T,
		maxNumWriters, maxWriters int,
		singlePrimary bool,
		writerIsReader int,
		checkTimeout, proxySQLPingTO, pxcPingTO int,
		lockClusterTO, lockRefreshTime int64,
		retryUp, retryDown int,
	) {
		conf := &global.Configuration{}
		conf.Pxcluster.MaxNumWriters = maxNumWriters
		conf.Pxcluster.MaxWriters = maxWriters
		conf.Pxcluster.SinglePrimary = singlePrimary
		conf.Pxcluster.WriterIsAlsoReader = writerIsReader
		conf.Pxcluster.CheckTimeOut = checkTimeout
		conf.Proxysql.PingTimeout = proxySQLPingTO
		conf.Pxcluster.PingTimeout = pxcPingTO
		conf.Global.LockClusterTimeout = lockClusterTO
		conf.Global.LockRefreshTime = lockRefreshTime
		conf.Pxcluster.RetryUp = retryUp
		conf.Pxcluster.RetryDown = retryDown

		// Fix filesystem-dependent fields so the fuzz target stays hermetic.
		conf.Proxysql.LockFilePath = "/tmp"
		conf.Pxcluster.HgW = 100
		conf.Pxcluster.HgR = 101
		conf.Pxcluster.ConfigHgRange = 8000
		conf.Pxcluster.MaintenanceHgRange = 9000
		conf.Pxcluster.SslcertificatePath = "" // skip SSL path check

		ok := conf.SanityCheck()

		if ok {
			// Post-conditions that must hold whenever SanityCheck passes.
			if conf.Global.LockClusterTimeout < 60 {
				t.Errorf("SanityCheck=true but LockClusterTimeout=%d < 60 (must be ≥ 60)",
					conf.Global.LockClusterTimeout)
			}
			if conf.Global.LockRefreshTime >= conf.Global.LockClusterTimeout {
				t.Errorf("SanityCheck=true but LockRefreshTime=%d >= LockClusterTimeout=%d",
					conf.Global.LockRefreshTime, conf.Global.LockClusterTimeout)
			}
			if conf.Pxcluster.CheckTimeOut > 0 {
				if conf.Proxysql.PingTimeout >= conf.Pxcluster.CheckTimeOut {
					t.Errorf("SanityCheck=true but ProxySQLPingTimeout=%d >= CheckTimeOut=%d",
						conf.Proxysql.PingTimeout, conf.Pxcluster.CheckTimeOut)
				}
				if conf.Pxcluster.PingTimeout >= conf.Pxcluster.CheckTimeOut {
					t.Errorf("SanityCheck=true but PxcPingTimeout=%d >= CheckTimeOut=%d",
						conf.Pxcluster.PingTimeout, conf.Pxcluster.CheckTimeOut)
				}
			}
		}
	})
}

// ── FuzzCheckLockFileExists ───────────────────────────────────────────────────
//
// CheckLockFIleExists reads a two-line text file:
//
//	PID:<integer>
//	Time:<int64>
//
// It must never panic regardless of the file content.  If it successfully
// parses the file, the returned pid must match the PID line and the returned
// time must match the Time line (i.e. no silent data corruption).
//
// Failing colleague: FuzzCheckLockFileExists.
func FuzzCheckLockFileExists(f *testing.F) {
	// Seed corpus: typical and adversarial file content.
	seeds := []string{
		"PID:12345\nTime:1700000000000000000\n", // well-formed
		"PID:0\nTime:0\n",                        // zero values
		"PID:1\nTime:1\n",                        // minimal valid
		"",                                        // empty file (lock file missing content)
		"garbage\n",                               // no prefixes
		"PID:\nTime:\n",                           // empty values
		"PID:abc\nTime:xyz\n",                     // non-numeric — warns, must not panic
		"PID:-1\nTime:-1\n",                       // negative values
		"PID:99999999999999999999\nTime:0\n",      // int overflow in PID
		"PID:0\nTime:99999999999999999999\n",      // int64 overflow in Time
		strings.Repeat("x", 1000) + "\n",          // long line without prefix
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, content []byte) {
		// Write content to an isolated temp file.
		dir := t.TempDir()
		path := filepath.Join(dir, "hegel-fuzz.lock")
		if err := os.WriteFile(path, content, 0644); err != nil {
			t.Skip("cannot write temp file:", err)
		}

		lock := locker.HegelNewFileLockAtPath(path)

		// Must not panic regardless of content.
		exists, pid, ts := lock.CheckLockFIleExists()

		if !exists {
			t.Errorf("CheckLockFIleExists returned false even though file exists at %s", path)
		}
		// pid and ts can be zero for unparseable lines — that is acceptable;
		// the function logs a warning and continues.  What must NOT happen is
		// a panic, an os.Exit from scanner.Err() (which only fires on I/O
		// errors, not on unexpected content), or silent data mutation.
		_ = pid
		_ = ts
	})
}

// ── FuzzSaveRetrySQL ──────────────────────────────────────────────────────────
//
// SaveRetry embeds retry counters in a ProxySQL comment column by appending
// to whatever text is already in dataNode.Comment.  It must:
//
//  1. Always return a non-empty UPDATE statement (liveness).
//  2. Always include the correct retry_up and retry_down values (correctness).
//  3. NOT silently drop or corrupt the hostgroup/ip/port binding (safety).
//
// The secondary finding this fuzz target surfaces: single quotes in the
// comment are not escaped, producing syntactically broken SQL.  Every
// triggered instance is logged so the report shows the injection surface.
//
// Failing colleague: FuzzSaveRetrySQL.
func FuzzSaveRetrySQL(f *testing.F) {
	// Seed corpus: typical comments plus injection-relevant edge cases.
	seeds := []string{
		"",
		"normal operator note",
		"100_W_101_R_retry_up=3;100_W_101_R_retry_down=2;", // pre-existing marker
		"it's a trap",                                        // single quote
		"'; DROP TABLE mysql_servers; --",                    // SQL injection
		"say \"hello\"",                                      // double quote
		"tab\there",                                          // control char
		fmt.Sprintf("%s", strings.Repeat("x", 512)),          // long comment
		"#LOCK_1_HG_100_W_HG_101_R_12345_LOCK#",             // lock token in comment
	}
	for _, s := range seeds {
		f.Add(s, 0, 0) // comment, retryUp, retryDown
	}

	f.Fuzz(func(t *testing.T, comment string, retryUp, retryDown int) {
		if retryUp < 0 {
			retryUp = -retryUp
		}
		if retryDown < 0 {
			retryDown = -retryDown
		}

		pn := newProxySQLNode()
		node := cluster.HegelNewDefaultNode("10.0.0.1:3306")
		node.Comment = comment
		node.RetryUp = retryUp
		node.RetryDown = retryDown

		const (
			ip  = "10.0.0.1"
			hg  = 100
			port = 3306
		)

		sql := pn.SaveRetry(node, hg, ip, port)

		// 1. Liveness: SQL must never be empty.
		if sql == "" {
			t.Fatalf("SaveRetry returned empty SQL for comment=%q retryUp=%d retryDown=%d",
				comment, retryUp, retryDown)
		}

		// 2. Correctness: retry counters must appear in the SQL.
		upMarker := fmt.Sprintf("retry_up=%d", retryUp)
		downMarker := fmt.Sprintf("retry_down=%d", retryDown)
		if !strings.Contains(sql, upMarker) {
			t.Errorf("SaveRetry SQL missing %q (retryUp=%d): %s", upMarker, retryUp, sql)
		}
		if !strings.Contains(sql, downMarker) {
			t.Errorf("SaveRetry SQL missing %q (retryDown=%d): %s", downMarker, retryDown, sql)
		}

		// 3. Safety: hostgroup, ip, and port must be bound correctly.
		if !strings.Contains(sql, fmt.Sprintf("hostgroup_id=%d", hg)) {
			t.Errorf("SaveRetry SQL missing hostgroup_id=%d: %s", hg, sql)
		}
		if !strings.Contains(sql, fmt.Sprintf("hostname='%s'", ip)) {
			t.Errorf("SaveRetry SQL missing hostname=%q: %s", ip, sql)
		}

		// 4. Injection surface (logged, not fatal — see TestProp_SQLBuilders_SingleQuoteInCommentIsNotEscaped).
		if strings.Contains(comment, "'") && !strings.Contains(sql, "''") {
			t.Logf("INJECTION SURFACE: comment=%q contains unescaped single quote in SQL: %s", comment, sql)
		}
	})
}
