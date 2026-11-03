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
	"strconv"
	"testing"
	"time"

	"pxc_scheduler_handler/internal/locker"
	"pxc_scheduler_handler/internal/proxysql"

	"hegel.dev/go/hegel"
)

// ── cluster-lock mutual-exclusion properties ──────────────────────────────────
//
// The ProxySQL cluster lock (stored in the proxysql_servers comment column)
// must guarantee that exactly ONE scheduler acts at any moment.  The
// invariants below drive findLock directly — no database connection required —
// by constructing LockerImpl instances and a pre-populated node map.
//
// Lock comment format (set by findLock):
//   #LOCK_<clusterLockId>_<unixNano>_LOCK#
//
// findLock decides the winner by comparing timestamps; it returns
// (updatedNodes, true) for the winner and (nil, false) for losers.

const testLockId = "1_HG_100_W_HG_101_R"
const testLockTimeout = int64(60) // seconds

// [HIGH] When scheduler A holds a valid, non-expired cluster lock, scheduler B
// must lose the findLock election.  Violating this invariant means two
// schedulers act simultaneously, which can corrupt ProxySQL host-group state.
func TestProp_ClusterLock_SecondSchedulerDoesNotWinActiveLock(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		dnsA := "10.0.0.1:6032"
		dnsB := "10.0.0.2:6032"

		// A lock written 1 second ago is well within the 60-second timeout.
		recentTime := time.Now().UnixNano() - 1_000_000_000
		lockComment := "#LOCK_" + testLockId + "_" + strconv.FormatInt(recentTime, 10) + "_LOCK#"

		// Both nodes are visible to all schedulers via the proxysql_servers table.
		nodes := map[string]proxysql.ProxySQLNodeImpl{
			dnsA: {Dns: dnsA, Comment: lockComment}, // A currently holds the lock
			dnsB: {Dns: dnsB, Comment: ""},
		}

		lockerB := locker.HegelNewLockerForTest(testLockId, dnsB, testLockTimeout, 0)
		_, bWins := locker.HegelFindLock(lockerB, nodes)

		if bWins {
			ht.Fatal(
				"mutual exclusion violated: scheduler B won the cluster lock " +
					"while scheduler A holds a valid non-expired lock — " +
					"two schedulers would act simultaneously",
			)
		}
	})
}

// [HIGH] When a scheduler holds the lock and its timeout has expired, a second
// scheduler must be able to take over.  Stale locks must not block the cluster
// from being managed indefinitely.
func TestProp_ClusterLock_ExpiredLockAllowsTakeover(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		dnsA := "10.0.0.1:6032"
		dnsB := "10.0.0.2:6032"

		// A lock written 120 seconds ago is expired (timeout=60).
		expiredTime := time.Now().UnixNano() - 120_000_000_000
		lockComment := "#LOCK_" + testLockId + "_" + strconv.FormatInt(expiredTime, 10) + "_LOCK#"

		nodes := map[string]proxysql.ProxySQLNodeImpl{
			dnsA: {Dns: dnsA, Comment: lockComment}, // A's lock is expired
			dnsB: {Dns: dnsB, Comment: ""},
		}

		lockerB := locker.HegelNewLockerForTest(testLockId, dnsB, testLockTimeout, 0)
		_, bWins := locker.HegelFindLock(lockerB, nodes)

		if !bWins {
			ht.Fatal(
				"stale lock is not released: scheduler B could not acquire the " +
					"cluster lock even though scheduler A's lock has expired — " +
					"the cluster would be permanently unmanaged after a crash",
			)
		}
	})
}

// [HIGH] Two schedulers managing DIFFERENT PXC clusters share the same ProxySQL
// instance (and therefore the same proxysql_servers comment column).  Each
// cluster uses its own unique clusterLockId, so scheduler A's active lock must
// be invisible to scheduler B's findLock scan and must not block it from
// acquiring its own lock.
//
// If this invariant breaks, cluster B is frozen while cluster A is healthy —
// the two clusters incorrectly see each other's scheduler as a competitor.
func TestProp_ClusterLock_TwoClustersDoNotInterfere(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		const dns = "10.0.0.1:6032"
		const lockIdA = "1_HG_100_W_HG_101_R"
		const lockIdB = "2_HG_200_W_HG_201_R"

		// Scheduler A holds a fresh, non-expired lock for cluster 1.
		recentTime := time.Now().UnixNano() - 1_000_000_000
		lockA := "#LOCK_" + lockIdA + "_" + strconv.FormatInt(recentTime, 10) + "_LOCK#"

		nodes := map[string]proxysql.ProxySQLNodeImpl{
			dns: {Dns: dns, Comment: lockA},
		}

		// Scheduler B manages a DIFFERENT cluster (lockIdB).  It must win its
		// own lock immediately because lockIdA's header does not match lockIdB's
		// scan prefix ("#LOCK_2_HG_200_W_HG_201_R_").
		lockerB := locker.HegelNewLockerForTest(lockIdB, dns, testLockTimeout, 0)
		_, bWins := locker.HegelFindLock(lockerB, nodes)

		if !bWins {
			ht.Fatal(
				"cluster isolation violated: scheduler B (cluster 2) was blocked " +
					"by scheduler A's lock (cluster 1) — different clusters must use " +
					"independent lock namespaces via their clusterLockId",
			)
		}
	})
}

// [HIGH] findLock must handle any text already present in the proxysql_servers
// comment column without panicking.  Operators sometimes add free-form notes
// to that column; partial or malformed lock headers must not crash the scheduler.
func TestProp_ClusterLock_ArbitraryNodeCommentNeverCausesLockPanic(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		comment := hegel.Draw(ht, hegel.SampledFrom([]string{
			"",
			"normal operator note",
			"#LOCK_but_wrong_cluster_id_12345_LOCK#",
			"it's tricky",
			"   lots   of   spaces   ",
			"tab\there",
			"100% verified",
			"#LOCK_1_HG_100_W_HG_101_R_notanumber_LOCK#", // malformed timestamp
			"prefix #LOCK_1_HG_100_W_HG_101_R_suffix",    // missing tail
		}))

		dnsA := "10.0.0.1:6032"
		lockerA := locker.HegelNewLockerForTest(testLockId, dnsA, testLockTimeout, 0)

		nodes := map[string]proxysql.ProxySQLNodeImpl{
			dnsA: {Dns: dnsA, Comment: comment},
		}

		// Must never panic regardless of comment content.
		// We do not assert the winner because some comment variants may
		// confuse the timestamp parser; what matters is no crash.
		_, _ = locker.HegelFindLock(lockerA, nodes)
	})
}
