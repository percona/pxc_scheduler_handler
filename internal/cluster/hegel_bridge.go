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

// Package cluster — hegel bridge.
// Compiled only with -tags hegel.  Exposes private DataClusterImpl methods
// as exported wrappers so the external package internal/test/hegel can drive
// property-based tests without touching production code paths.
package cluster

// ── node-state check functions ────────────────────────────────────────────────

func (c *DataClusterImpl) HegelCheckWsrepDesync(node DataNodeImpl, hg Hostgroup) bool {
	return c.checkWsrepDesync(node, hg)
}

func (c *DataClusterImpl) HegelCheckAnyNotReadyStatus(node DataNodeImpl, hg Hostgroup) bool {
	return c.checkAnyNotReadyStatus(node, hg)
}

func (c *DataClusterImpl) HegelCheckNotPrimary(node DataNodeImpl, hg Hostgroup) bool {
	return c.checkNotPrimary(node, hg)
}

func (c *DataClusterImpl) HegelCheckRejectQueries(node DataNodeImpl, hg Hostgroup) bool {
	return c.checkRejectQueries(node, hg)
}

func (c *DataClusterImpl) HegelCheckDonorReject(node DataNodeImpl, hg Hostgroup) bool {
	return c.checkDonorReject(node, hg)
}

func (c *DataClusterImpl) HegelCheckPxcMaint(node DataNodeImpl, hg Hostgroup) bool {
	return c.checkPxcMaint(node, hg)
}

func (c *DataClusterImpl) HegelCheckReadOnly(node DataNodeImpl, hg Hostgroup) bool {
	return c.checkReadOnly(node, hg)
}

func (c *DataClusterImpl) HegelCheckBackOffline(node DataNodeImpl, hg Hostgroup) (DataNodeImpl, bool) {
	return c.checkBackOffline(node, hg)
}

func (c *DataClusterImpl) HegelCheckBackNew(node DataNodeImpl) bool {
	return c.checkBackNew(node)
}

func (c *DataClusterImpl) HegelCheckBackPrimary(node DataNodeImpl, hg Hostgroup) bool {
	return c.checkBackPrimary(node, hg)
}

func (c *DataClusterImpl) HegelCheckBackDesyncButUnderReplicaLag(node DataNodeImpl, hg Hostgroup) bool {
	return c.checkBackDesyncButUnderReplicaLag(node, hg)
}

func (c *DataClusterImpl) HegelCheckUpSaveRetry(node DataNodeImpl, hg Hostgroup) bool {
	return c.checkUpSaveRetry(node, hg)
}

func (c *DataClusterImpl) HegelCheckActiveFailover() bool {
	return c.checkActiveFailover()
}

func (c *DataClusterImpl) HegelCheckFailoverIfFound() bool {
	return c.checkFailoverIfFound()
}

func (c *DataClusterImpl) HegelCleanWriters() bool {
	return c.cleanWriters()
}

func (c *DataClusterImpl) HegelIdentifyLowerNodeToRemove(node DataNodeImpl) bool {
	return c.identifyLowerNodeToRemove(node)
}

// ── node-operation functions ──────────────────────────────────────────────────

func (c *DataClusterImpl) HegelAlignNodeValues(dst, src DataNodeImpl) DataNodeImpl {
	return c.alignNodeValues(dst, src)
}

func (c *DataClusterImpl) HegelCompareNodeDefault(nodeIn, nodeDefault DataNodeImpl) bool {
	return c.compareNodeDefault(nodeIn, nodeDefault)
}

func (c *DataClusterImpl) HegelResetNodeDefault(nodeIn, nodeDefault DataNodeImpl) DataNodeImpl {
	return c.resetNodeDefault(nodeIn, nodeDefault)
}

func (c *DataClusterImpl) HegelCalculateHgOnlineSize(m map[string]DataNodeImpl) int {
	return c.calculateHgOnlineSize(m)
}

func (c *DataClusterImpl) HegelCheckMissingForNodes(evalMap map[string]DataNodeImpl) map[string]DataNodeImpl {
	return c.checkMissingForNodes(evalMap)
}

func (c *DataClusterImpl) HegelProcessWriterIsAlsoReader(readerNodes map[string]DataNodeImpl) bool {
	return c.processWriterIsAlsoReader(readerNodes)
}

func (c *DataClusterImpl) HegelProcessUpAndDownReaders(actionNodes, readerNodes map[string]DataNodeImpl) bool {
	return c.processUpAndDownReaders(actionNodes, readerNodes)
}

// ── DataNodeImpl helpers ──────────────────────────────────────────────────────

// HegelGetRetry calls the unexported getRetry on a copy of the node and
// returns the modified copy so the caller can inspect RetryUp/RetryDown.
func HegelGetRetry(node DataNodeImpl, writeHG, readHG int) DataNodeImpl {
	node.getRetry(writeHG, readHG)
	return node
}

// ── test factories ────────────────────────────────────────────────────────────

// HegelNewDefaultCluster returns a DataClusterImpl wired the same way as the
// existing table-driven test harness (HgWriterId=100, HgReaderId=101,
// ConfigHgRange=8000, MaintenanceHgRange=9000, …).
func HegelNewDefaultCluster() *DataClusterImpl {
	cf := testClusterFactory()
	return cf.clusterNodeImplFactory()
}

// HegelNewDefaultNode returns a DataNodeImpl with baseline test values for
// the given DNS address.
func HegelNewDefaultNode(dns string) DataNodeImpl {
	cf := testClusterFactory()
	return cf.testDataNodeFactoryDns(dns)
}

// HegelNewHostgroup returns a Hostgroup configured with the supplied parameters.
func HegelNewHostgroup(id, size int, hgType string) Hostgroup {
	return getHgOpt(id, size, hgType)
}
