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

// Package locker — hegel bridge.
// Compiled only with -tags hegel.
package locker

import "pxc_scheduler_handler/internal/proxysql"

// HegelNewFileLockAtPath creates a FileLockImp that reads from / writes to
// path.  Used by fuzz tests that write arbitrary content to a temp file and
// then call CheckLockFIleExists to verify it never panics.
func HegelNewFileLockAtPath(path string) FileLockImp {
	return FileLockImp{flFullPath: path}
}

// HegelNewFileLock creates a FileLockImp with specified pid, creation time
// and timeout for use in property-based tests.
// All fields of FileLockImp are unexported, so external test packages must
// use this constructor.
func HegelNewFileLock(pid int, appCreationTime, timeout int64) FileLockImp {
	return FileLockImp{
		flPid:             pid,
		flFullPath:        "/tmp/hegel-test.lock",
		flAppCreationTime: appCreationTime,
		flTimeout:         timeout,
	}
}

// HegelEvaluateFileLockForRemoval is a thin wrapper around the exported
// EvaluateFileLockForRemoval so tests can drive it via a HegelNewFileLock
// instance without needing direct field access.
func HegelEvaluateFileLockForRemoval(lock FileLockImp, evaluate bool, localPID int, localTime int64) bool {
	return lock.EvaluateFileLockForRemoval(evaluate, localPID, localTime)
}

// ── cluster-lock test helpers ─────────────────────────────────────────────────

// HegelNewLockerForTest builds a minimal LockerImpl suitable for testing the
// findLock mutual-exclusion logic without a database connection.
// Init is intentionally NOT called — fields are set directly so tests can
// drive findLock in isolation without any network or filesystem I/O.
func HegelNewLockerForTest(clusterLockId, myDns string, lockClusterTimeout, lockRefreshTime int64) *LockerImpl {
	myServer := new(proxysql.ProxySQLNodeImpl)
	myServer.Dns = myDns
	return &LockerImpl{
		ClusterLockId:      clusterLockId,
		MyServer:           myServer,
		LockClusterTimeout: lockClusterTimeout,
		LockRefreshTime:    lockRefreshTime,
	}
}

// HegelFindLock exposes the unexported findLock method so property tests can
// verify mutual-exclusion invariants without going through CheckClusterLock
// (which requires a live ProxySQL connection).
func HegelFindLock(locker *LockerImpl, nodes map[string]proxysql.ProxySQLNodeImpl) (map[string]proxysql.ProxySQLNodeImpl, bool) {
	return locker.findLock(nodes)
}
