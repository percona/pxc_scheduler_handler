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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pxc_scheduler_handler/internal/locker"

	"hegel.dev/go/hegel"
)

// ── SetLock file-truncation properties ───────────────────────────────────────
//
// SetLock writes a two-line lock file:
//
//	PID:<int>
//	Time:<int64>
//
// It opens the file with os.O_CREATE|os.O_WRONLY — without os.O_TRUNC.
// When a subsequent SetLock call produces a shorter file, the tail bytes from
// the previous write survive on disk.  CheckLockFIleExists reads only the
// first two lines and therefore misses the corruption, but anything that reads
// or hashes the raw file bytes will see the stale data.
//
// These tests document and guard the truncation invariant.
// Failing colleague: SetLock_ShorterLockLeavesNoTail.

// [MEDIUM] Writing a new lock over a previous (longer) lock file must not leave
// trailing bytes from the earlier write.  A correct implementation opens with
// O_TRUNC so the file length always matches the newly written content exactly.
//
// Expected result: the test FAILS with the current implementation (no O_TRUNC),
// documenting the known bug.  Remove the "SetLock_ShorterLockLeavesNoTail"
// entry from the known-failures list once O_TRUNC is added.
func TestProp_SetLock_ShorterLockLeavesNoTail(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		dir, err := os.MkdirTemp("", "hegel-setlock-*")
		if err != nil {
			ht.Fatalf("MkdirTemp: %v", err)
		}
		defer os.RemoveAll(dir)

		path := filepath.Join(dir, "test.lock")

		// Write an initial lock file whose first line is longer than the one
		// SetLock will write.  The PID line starts with "PID:0\n" (6 bytes);
		// we pad the initial file with trailing garbage so a non-truncating
		// write leaves detectable stale bytes.
		//
		// Initial content: "PID:0\nTime:0\n" + 50 'X' chars + "\n"
		// SetLock content:  "PID:0\nTime:0\n"  (13 bytes, no trailing garbage)
		initial := "PID:0\nTime:0\n" + strings.Repeat("X", 50) + "\n"
		if err := os.WriteFile(path, []byte(initial), 0644); err != nil {
			ht.Fatalf("WriteFile: %v", err)
		}

		// HegelNewFileLockAtPath sets flPid=0, flAppCreationTime=0.
		// EvaluateFileLockForRemoval will read PID:0 from the file and match
		// flPid(0) == localPID(0) → returns true, so SetLock proceeds to write.
		lock := locker.HegelNewFileLockAtPath(path)
		if !lock.SetLock() {
			ht.Fatal("SetLock returned false — EvaluateFileLockForRemoval did not allow overwrite")
		}

		got, err := os.ReadFile(path)
		if err != nil {
			ht.Fatalf("ReadFile after SetLock: %v", err)
		}

		// A correct SetLock writes exactly "PID:0\nTime:0\n" and nothing else.
		want := "PID:0\nTime:0\n"
		if string(got) != want {
			ht.Fatalf(
				"SetLock left tail bytes in the lock file.\n"+
					"  want (%d bytes): %q\n"+
					"   got (%d bytes): %q\n"+
					"Fix: open the file with os.O_TRUNC in addition to os.O_CREATE|os.O_WRONLY.",
				len(want), want, len(got), string(got),
			)
		}
	})
}
