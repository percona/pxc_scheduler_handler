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

	"pxc_scheduler_handler/internal/global"

	"hegel.dev/go/hegel"
)

// ── SanityCheck idempotency properties ───────────────────────────────────────
//
// SanityCheck() mutates conf.* fields in place (auto-correcting invalid values
// such as a LockClusterTimeout below 60, a LockRefreshTime above 75% of the
// timeout, or a PingTimeout above CheckTimeOut).  After the first call the
// configuration is in a "settled" state; the second call must see the same
// state and return the same boolean result.
//
// Non-idempotency was observed in production when CheckTimeOut=1 caused
// PingTimeout to oscillate between 0 and 1000 on alternating calls because:
//   - Call 1: PingTimeout(≥1) → set to CheckTimeOut/2 = 0 (integer division).
//   - Call 2: PingTimeout==0 → zero-correction sets it to 1000; then
//     1000 >= CheckTimeOut(1) → sets it back to 0 again.
// Both calls return true, but the config never truly settles — any subsequent
// call reads a different PingTimeout value than the one the previous call
// produced.
//
// Failing colleague: SanityCheck_Idempotent (intermittent).

// [MEDIUM] SanityCheck called twice on the same Configuration must return the
// same boolean value on the second call as on the first.  The first call is
// allowed to auto-correct; the second must see the already-corrected values and
// therefore agree with the first.
func TestProp_SanityCheck_Idempotent(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		lockClusterTO := hegel.Draw(ht, hegel.Integers[int64](0, 300))
		lockRefresh := hegel.Draw(ht, hegel.Integers[int64](0, 300))
		checkTimeout := hegel.Draw(ht, hegel.Integers[int](0, 10000))
		proxySQLPingTO := hegel.Draw(ht, hegel.Integers[int](0, 10000))
		pxcPingTO := hegel.Draw(ht, hegel.Integers[int](0, 10000))
		retryUp := hegel.Draw(ht, hegel.Integers[int](0, 20))
		retryDown := hegel.Draw(ht, hegel.Integers[int](0, 20))

		conf := &global.Configuration{}
		// Fix values that cause unconditional false so we test the correction paths.
		conf.Pxcluster.MaxNumWriters = 1
		conf.Pxcluster.MaxWriters = 1
		conf.Pxcluster.SinglePrimary = true
		conf.Pxcluster.WriterIsAlsoReader = 1
		conf.Pxcluster.HgW = 100
		conf.Pxcluster.HgR = 101
		conf.Pxcluster.ConfigHgRange = 8000
		conf.Pxcluster.MaintenanceHgRange = 9000
		conf.Proxysql.LockFilePath = "/tmp"
		conf.Pxcluster.SslcertificatePath = ""

		conf.Pxcluster.CheckTimeOut = checkTimeout
		conf.Proxysql.PingTimeout = proxySQLPingTO
		conf.Pxcluster.PingTimeout = pxcPingTO
		conf.Global.LockClusterTimeout = lockClusterTO
		conf.Global.LockRefreshTime = lockRefresh
		conf.Pxcluster.RetryUp = retryUp
		conf.Pxcluster.RetryDown = retryDown

		first := conf.SanityCheck()
		second := conf.SanityCheck()

		if first != second {
			ht.Fatalf(
				"SanityCheck is not idempotent: first=%v second=%v\n"+
					"  inputs: checkTO=%d proxySQLPingTO=%d pxcPingTO=%d "+
					"lockClusterTO=%d lockRefresh=%d retryUp=%d retryDown=%d\n"+
					"  after first call: checkTO=%d proxySQLPingTO=%d pxcPingTO=%d "+
					"lockClusterTO=%d lockRefresh=%d",
				first, second,
				checkTimeout, proxySQLPingTO, pxcPingTO, lockClusterTO, lockRefresh, retryUp, retryDown,
				conf.Pxcluster.CheckTimeOut, conf.Proxysql.PingTimeout, conf.Pxcluster.PingTimeout,
				conf.Global.LockClusterTimeout, conf.Global.LockRefreshTime,
			)
		}
	})
}
