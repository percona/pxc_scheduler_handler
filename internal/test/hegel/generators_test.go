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

// Package hegel contains property-based tests for pxc_scheduler_handler.
// Build with: go test -tags hegel ./internal/test/hegel/...
package hegel

import (
	"pxc_scheduler_handler/internal/cluster"
	"pxc_scheduler_handler/internal/global"
	"pxc_scheduler_handler/internal/proxysql"

	"hegel.dev/go/hegel"
)

// ── domain-value generators ───────────────────────────────────────────────────

func wsrepStatusGen() hegel.Generator[int] {
	return hegel.SampledFrom([]int{1, 2, 3, 4, 5})
}

func wsrepClusterStatusGen() hegel.Generator[string] {
	return hegel.SampledFrom([]string{"Primary", "Non-Primary", "Disconnected", ""})
}

func pxcMaintModeGen() hegel.Generator[string] {
	return hegel.SampledFrom([]string{"DISABLED", "ENABLED", "SHUTDOWN", "DELETE", ""})
}

func proxyStatusGen() hegel.Generator[string] {
	return hegel.SampledFrom([]string{"ONLINE", "OFFLINE_SOFT", "OFFLINE_HARD"})
}

// hostgroupIdGen generates IDs that span all three logical zones:
//   production (100, 101), backup (8100, 8101), maintenance (9100, 9101).
func hostgroupIdGen() hegel.Generator[int] {
	return hegel.SampledFrom([]int{100, 101, 8100, 8101, 9100, 9101})
}

func hgTypeGen() hegel.Generator[string] {
	return hegel.SampledFrom([]string{"W", "R"})
}

// nodeGen generates a DataNodeImpl by mutating the canonical test baseline.
// Behaviorally-relevant fields are randomised; infrastructure fields (Ssl,
// maps, Dns) keep their safe defaults so no nil-pointer panics occur inside
// pure check* methods.
func nodeGen() hegel.Generator[cluster.DataNodeImpl] {
	return hegel.Composite(func(tc hegel.TestCase) cluster.DataNodeImpl {
		node := cluster.HegelNewDefaultNode("127.0.0.1:3306")
		node.WsrepStatus = hegel.Draw(tc, wsrepStatusGen())
		node.WsrepClusterStatus = hegel.Draw(tc, wsrepClusterStatusGen())
		node.PxcMaintMode = hegel.Draw(tc, pxcMaintModeGen())
		node.ProxyStatus = hegel.Draw(tc, proxyStatusGen())
		node.ReadOnly = hegel.Draw(tc, hegel.Booleans())
		node.WsrepRejectqueries = hegel.Draw(tc, hegel.Booleans())
		node.WsrepDonorrejectqueries = hegel.Draw(tc, hegel.Booleans())
		node.HostgroupId = hegel.Draw(tc, hostgroupIdGen())
		node.Weight = hegel.Draw(tc, hegel.Integers[int](1, 10000))
		node.MaxReplicationLag = hegel.Draw(tc, hegel.Integers[int](0, 1000))
		node.WsrepLocalRecvQueue = hegel.Draw(tc, hegel.Integers[int](0, 2000))
		node.RetryUp = hegel.Draw(tc, hegel.Integers[int](0, 5))
		node.RetryDown = hegel.Draw(tc, hegel.Integers[int](0, 5))
		node.NodeIsNew = hegel.Draw(tc, hegel.Booleans())
		node.ActionType = 0
		return node
	})
}

// hostgroupGen generates a Hostgroup with randomised size and type.
func hostgroupGen() hegel.Generator[cluster.Hostgroup] {
	return hegel.Composite(func(tc hegel.TestCase) cluster.Hostgroup {
		id := hegel.Draw(tc, hegel.SampledFrom([]int{100, 101}))
		size := hegel.Draw(tc, hegel.Integers[int](1, 10))
		hgType := hegel.Draw(tc, hgTypeGen())
		return cluster.HegelNewHostgroup(id, size, hgType)
	})
}

// weightGen generates a strictly-positive weight.
func weightGen() hegel.Generator[int] {
	return hegel.Integers[int](1, 10000)
}

// newTestCluster returns a freshly initialised DataClusterImpl matching the
// existing table-driven harness defaults.
// Always call this inside the hegel.Test callback so each trial gets its own
// clean instance (check* methods write into cluster.ActionNodes as a side effect).
func newTestCluster() *cluster.DataClusterImpl {
	return cluster.HegelNewDefaultCluster()
}

// ── proxysql test helper ──────────────────────────────────────────────────────

// newProxySQLNode returns a ProxySQLNodeImpl wired with the standard test
// cluster so SQL-builder methods that read Config and MySQLCluster work.
func newProxySQLNode() *proxysql.ProxySQLNodeImpl {
	cfg := &global.Configuration{}
	cfg.Pxcluster.MaintenanceHgRange = 9000
	cfg.Pxcluster.ConfigHgRange = 8000
	cfg.Pxcluster.HgW = 100
	cfg.Pxcluster.HgR = 101

	mysqlCluster := cluster.HegelNewDefaultCluster()

	return &proxysql.ProxySQLNodeImpl{
		ActionNodeList: make(map[string]cluster.DataNodeImpl),
		Hostgoups:      make(map[int]cluster.Hostgroup),
		Variables:      make(map[string]string),
		Config:         cfg,
		MySQLCluster:   mysqlCluster,
	}
}
