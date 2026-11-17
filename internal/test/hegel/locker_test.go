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

	"pxc_scheduler_handler/internal/locker"

	"hegel.dev/go/hegel"
)

// ── EvaluateFileLockForRemoval properties ─────────────────────────────────────
//
// The function signature is:
//   EvaluateFileLockForRemoval(evaluate bool, localPID int, localTime int64) bool
//
// Key behaviours to verify with property-based testing:
//   1. evaluate=false → always returns false (short-circuit, no file read).
//   2. flPid == localPID → always returns true (same process owns the lock;
//      safe to overwrite since it is a restart of the same application).
//   3. Timeout expired → returns true (stale lock from a dead process).
//   4. Timeout not expired + valid PID → returns false (another process alive).

// Property 1: evaluate=false is an unconditional no-op.
func TestProp_EvaluateFileLockForRemoval_EvaluateFalseAlwaysReturnsFalse(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		pid := hegel.Draw(ht, hegel.Integers[int](1, 99999))
		creationTime := hegel.Draw(ht, hegel.Integers[int64](1, 1_000_000))
		timeout := hegel.Draw(ht, hegel.Integers[int64](1, 3600))
		localPID := hegel.Draw(ht, hegel.Integers[int](1, 99999))
		localTime := hegel.Draw(ht, hegel.Integers[int64](0, 1_000_000))

		lock := locker.HegelNewFileLock(pid, creationTime, timeout)
		result := locker.HegelEvaluateFileLockForRemoval(lock, false, localPID, localTime)

		if result {
			ht.Fatal("EvaluateFileLockForRemoval returned true with evaluate=false (must always be false)")
		}
	})
}

// Property 2: when the lock PID matches the current process PID the lock
// belongs to this application instance — it is always safe to overwrite.
func TestProp_EvaluateFileLockForRemoval_SamePIDAlwaysTrue(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		pid := hegel.Draw(ht, hegel.Integers[int](1, 99999))
		creationTime := hegel.Draw(ht, hegel.Integers[int64](1, 1_000_000))
		timeout := hegel.Draw(ht, hegel.Integers[int64](1, 3600))
		localTime := hegel.Draw(ht, hegel.Integers[int64](0, 1_000_000))

		lock := locker.HegelNewFileLock(pid, creationTime, timeout)
		// Pass the SAME pid as localPID — the function must return true.
		result := locker.HegelEvaluateFileLockForRemoval(lock, true, pid, localTime)

		if !result {
			ht.Fatalf("EvaluateFileLockForRemoval returned false when flPid==localPID==%d (must be true)", pid)
		}
	})
}

// Property 3: when the timeout has expired the lock is stale.
// lockTime = (flAppCreationTime - FileLockTime) / 1e9 > flTimeout
// We arrange creationTime and localTime so lockTime is definitely > timeout.
//
// NOTE: the function also checks os.FindProcess; for a PID we know is dead
// (PID 1 is init — it exists but we rely on the timeout path for large PIDs
// that are guaranteed not to be running on the test host), we use a
// very large PID to hit the timeout branch. Alternatively we set a PID that
// FindProcess will fail on (negative or 0 is rejected by the OS, so we
// pick a large value that is almost certainly not a live process).
func TestProp_EvaluateFileLockForRemoval_ExpiredTimeoutReturnsTrueOrSignalError(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		// timeout in seconds (small so arithmetic is easy)
		timeout := hegel.Draw(ht, hegel.Integers[int64](1, 10))
		// Make lockTime (ns difference / 1e9) clearly > timeout.
		// lockTime = (creationTime - localTime) / 1e9
		// So we need creationTime - localTime > timeout * 1e9.
		localTime := int64(0)
		creationTime := (timeout + 1) * 1_000_000_000

		// Use a PID that is extremely unlikely to exist on the test host.
		// os.FindProcess on Linux never returns an error for any PID (it
		// succeeds even if the process doesn't exist), so the code falls
		// through to Signal(0), which will fail → triggers the timeout check.
		unusedPID := 999_999

		lock := locker.HegelNewFileLock(unusedPID, creationTime, timeout)
		result := locker.HegelEvaluateFileLockForRemoval(lock, true, unusedPID+1, localTime)

		// Either the process is genuinely not found (true) OR the timeout
		// branch fires (true). In both cases we expect true.
		// The only way result==false is if PID 999999 happens to be running
		// and the timeout hasn't expired, which the arithmetic prevents.
		if !result {
			ht.Fatalf("EvaluateFileLockForRemoval returned false for expired lock (timeout=%ds, lockTime=%ds)", timeout, (creationTime-localTime)/1_000_000_000)
		}
	})
}

// Property 4: when evaluate=false is the only input changed, the return is
// always false regardless of any other field values (boundary of property 1).
func TestProp_EvaluateFileLockForRemoval_EvaluateFalseIndependentOfAllFields(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		pid := hegel.Draw(ht, hegel.Integers[int](1, 99999))
		creationTime := hegel.Draw(ht, hegel.Integers[int64](0, 1_000_000_000_000))
		timeout := hegel.Draw(ht, hegel.Integers[int64](0, 7200))
		localPID := hegel.Draw(ht, hegel.Integers[int](1, 99999))
		localTime := hegel.Draw(ht, hegel.Integers[int64](0, 1_000_000_000_000))

		lock := locker.HegelNewFileLock(pid, creationTime, timeout)
		// Even with wildly varied parameters, evaluate=false must dominate.
		if locker.HegelEvaluateFileLockForRemoval(lock, false, localPID, localTime) {
			ht.Fatal("EvaluateFileLockForRemoval violated evaluate=false dominance")
		}
	})
}
