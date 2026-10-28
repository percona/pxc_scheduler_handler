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

package cluster

import (
	"testing"
)

// ****************  TESTS **********************************

/*
Tests for evaluate node method [start]
*/
func TestDataClusterImpl_checkWsrepDesync(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	testDataNode := clusterNode.testDataNodeFactory()
	currentHg := getHg()
	testDataNode.Hostgroups = []Hostgroup{currentHg}

	myArgs := args{testDataNode, testDataNode.Hostgroups[0]}

	tests = rulesTestCheckWsrepDesync(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			if got := cluster.checkWsrepDesync(tt.args.node, tt.args.currentHg); got != tt.want {
				t.Errorf(" %s checkWsrepDesync() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestDataClusterImpl_checkAnyNotReadyStatus(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	testDataNode := clusterNode.testDataNodeFactory()
	currentHg := getHg()
	testDataNode.Hostgroups = []Hostgroup{currentHg}

	myArgs := args{testDataNode, testDataNode.Hostgroups[0]}

	tests = rulesTestcheckAnyNotReadyStatus(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			if got := cluster.checkAnyNotReadyStatus(tt.args.node, tt.args.currentHg); got != tt.want {
				t.Errorf(" %s checkAnyNotReadyStatus() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestDataClusterImpl_checkNotPrimary(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	testDataNode := clusterNode.testDataNodeFactory()
	currentHg := getHg()
	testDataNode.Hostgroups = []Hostgroup{currentHg}

	myArgs := args{testDataNode, testDataNode.Hostgroups[0]}

	tests = rulesTestcheckNotPrimary(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			if got := cluster.checkNotPrimary(tt.args.node, tt.args.currentHg); got != tt.want {
				t.Errorf(" %s checkNotPrimary() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestDataClusterImpl_checkRejectQueries(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	testDataNode := clusterNode.testDataNodeFactory()
	currentHg := getHg()
	testDataNode.Hostgroups = []Hostgroup{currentHg}

	myArgs := args{testDataNode, testDataNode.Hostgroups[0]}

	tests = rulesTestcheckRejectQueries(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			if got := cluster.checkRejectQueries(tt.args.node, tt.args.currentHg); got != tt.want {
				t.Errorf(" %s checkRejectQueries() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestDataClusterImpl_checkDonorReject(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	testDataNode := clusterNode.testDataNodeFactory()
	currentHg := getHg()
	testDataNode.Hostgroups = []Hostgroup{currentHg}

	myArgs := args{testDataNode, testDataNode.Hostgroups[0]}

	tests = rulesTestcheckDonorReject(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.checkDonorReject(tt.args.node, tt.args.currentHg); got != tt.want {

				t.Errorf(" %s checkDonorReject() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestDataClusterImpl_checkPxcMaint(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	testDataNode := clusterNode.testDataNodeFactory()
	currentHg := getHg()
	testDataNode.Hostgroups = []Hostgroup{currentHg}

	myArgs := args{testDataNode, testDataNode.Hostgroups[0]}

	tests = rulesTestcheckPxcMaint(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.checkPxcMaint(tt.args.node, tt.args.currentHg); got != tt.want {

				t.Errorf(" %s checkPxcMaint() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestDataClusterImpl_checkReadOnly(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	testDataNode := clusterNode.testDataNodeFactory()
	currentHg := getHg()
	testDataNode.Hostgroups = []Hostgroup{currentHg}

	myArgs := args{testDataNode, testDataNode.Hostgroups[0]}

	tests = rulesTestCheckReadOnly(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.checkReadOnly(tt.args.node, tt.args.currentHg); got != tt.want {

				t.Errorf(" %s checkReadOnly() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestDataClusterImpl_checkBackOffLine(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	testDataNode := clusterNode.testDataNodeFactory()
	currentHg := getHg()
	testDataNode.Hostgroups = []Hostgroup{currentHg}

	myArgs := args{testDataNode, testDataNode.Hostgroups[0]}

	tests = rulesTestCheckBackOffLine(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if _, got := cluster.checkBackOffline(tt.args.node, tt.args.currentHg); got != tt.want {

				t.Errorf(" %s checkBackOffLine() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestDataClusterImpl_checkBackDesyncButUnderReplicaLag(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	testDataNode := clusterNode.testDataNodeFactory()
	currentHg := getHg()
	testDataNode.Hostgroups = []Hostgroup{currentHg}

	myArgs := args{testDataNode, testDataNode.Hostgroups[0]}

	tests = rulesTestCheckBackDesyncButUnderReplicaLag(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if  got := cluster.checkBackDesyncButUnderReplicaLag(tt.args.node, tt.args.currentHg); got != tt.want {

				t.Errorf(" %s checkBackDesyncButUnderReplicaLag() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}


func TestDataClusterImpl_checkBackNew(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	testDataNode := clusterNode.testDataNodeFactory()
	currentHg := getHg()
	testDataNode.Hostgroups = []Hostgroup{currentHg}

	myArgs := args{testDataNode, testDataNode.Hostgroups[0]}

	tests = rulesTestCheckBackNew(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.checkBackNew(tt.args.node); got != tt.want {

				t.Errorf(" %s checkBackNew() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestDataClusterImpl_checkBackPrimary(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	testDataNode := clusterNode.testDataNodeFactory()
	currentHg := getHg()
	testDataNode.Hostgroups = []Hostgroup{currentHg}

	myArgs := args{testDataNode, testDataNode.Hostgroups[0]}

	tests = rulesTestCheckBackPrimary(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.checkBackPrimary(tt.args.node, tt.args.currentHg); got != tt.want {

				t.Errorf(" %s checkBackPrimary() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

/*
Tests for evaluate node method [End]
*/

/*
Test Cluster method evaluateWriters [start]
*/

func TestDataClusterImpl_cleanWriters(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	dns := []string{"127.0.0.1:3306", "127.0.0.1:3307", "127.0.0.1:3308"}
	for _, myDns := range dns {
		testDataNode := clusterNode.testDataNodeFactoryDns(myDns)
		currentHg := getHg()
		testDataNode.Hostgroups = []Hostgroup{currentHg}
		clusterNode.WriterNodes[myDns] = testDataNode

	}
	myArgs := args{clusterNode.WriterNodes["127.0.0.1:3306"], clusterNode.WriterNodes["127.0.0.1:3306"].Hostgroups[0]}

	tests = rulesTestCleanWriters(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterNode.WriterNodes[tt.args.node.Dns] = tt.args.node
			cluster := clusterNode.clusterNodeImplFactory()
			cluster.WriterNodes = clusterNode.WriterNodes
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.cleanWriters(); got != tt.want {

				t.Errorf(" %s checkBackPrimary() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

//testing Primary Election
func TestDataClusterImpl_identifyPrimaryBackupNode(t *testing.T) {
	var tests = []ruleInt{}

	clusterNode := testClusterFactory()
	dns := []string{"127.0.0.1:3306", "127.0.0.1:3307", "127.0.0.1:3308"}
	for _, myDns := range dns {
		testDataNode := clusterNode.testDataNodeFactoryDns(myDns)
		currentHg := getHg()
		testDataNode.Hostgroups = []Hostgroup{currentHg}
		clusterNode.WriterNodes[myDns] = testDataNode
	}
	node := clusterNode.WriterNodes["127.0.0.1:3306"]
	node.Weight = 10000
	clusterNode.WriterNodes["127.0.0.1:3306"] = node

	myArgs := args{clusterNode.WriterNodes["127.0.0.1:3306"], clusterNode.WriterNodes["127.0.0.1:3306"].Hostgroups[0]}

	tests = rulesIdentifyPrimaryBackupNode(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterNode.WriterNodes[tt.args.node.Dns] = tt.args.node
			cluster := clusterNode.clusterNodeImplFactory()
			cluster.WriterNodes = clusterNode.WriterNodes
			cluster.FailOverNode = clusterNode.WriterNodes["127.0.0.1:3307"]
			cluster.PersistPrimarySettings = tt.testClusterNodeImp.PersistPrimarySettings

			value := cluster.identifyPrimaryBackupNode("127.0.0.1:3306")
			//t.Error( "aa ",tt.testClusterNodeImp.PersistPrimarySettings, " ", value)
			if value != tt.want {

				t.Errorf(" %s identifyPrimaryBackupNode() = %v want %v", tt.name, value, tt.want)
			}
		})
	}
}

/*
Test Cluster method evaluateWriters [end]
*/

/*
Test Cluster method evaluateReaders [start]
*/
func TestDataClusterImpl_processUpAndDownReaders(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	dns := []string{"127.0.0.1:3306", "127.0.0.1:3307", "127.0.0.1:3308"}
	for _, myDns := range dns {
		testDataNode := clusterNode.testDataNodeFactoryDns(myDns)
		currentHg := getHgOpt(101, 3, "R")
		testDataNode.HostgroupId = 101
		testDataNode.Hostgroups = []Hostgroup{currentHg}
		clusterNode.ReaderNodes[myDns] = testDataNode

	}
	myArgs := args{clusterNode.ReaderNodes["127.0.0.1:3306"], clusterNode.ReaderNodes["127.0.0.1:3306"].Hostgroups[0]}

	tests = rulesTestProcessUpAndDownReaders(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//t.Error("  AAA ", tt.args.node.ActionType)
			cluster := clusterNode.clusterNodeImplFactory()
			cluster.ActionNodes[tt.args.node.Dns] = tt.args.node
			cluster.ReaderNodes = clusterNode.ReaderNodes
			cluster.Hostgroups[tt.args.node.Hostgroups[0].Id] = tt.args.node.Hostgroups[0]
			//t.Error("  AAA ", tt.args.node.ActionType, "  ", len(cluster.ActionNodes), " ", cluster.ActionNodes[tt.args.node.Dns].ActionType, " " , cluster.Hostgroups[101].Type)

			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.processUpAndDownReaders(cluster.ActionNodes, cluster.ReaderNodes); got != tt.want {

				t.Errorf(" %s processUpAndDownReaders() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestDataClusterImpl_processWriterIsAlsoReader(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	//we need writer and readers here..
	dns := []string{"127.0.0.1:3306", "127.0.0.1:3307"}
	for _, myDns := range dns {
		testDataNode := clusterNode.testDataNodeFactoryDns(myDns)
		currentHg := getHgOpt(101, 3, "R")
		testDataNode.HostgroupId = 101
		testDataNode.Hostgroups = []Hostgroup{currentHg}
		clusterNode.ReaderNodes[myDns] = testDataNode

	}
	testDataNode := clusterNode.testDataNodeFactoryDns("127.0.0.1:3306")
	currentHg := getHgOpt(100, 1, "W")
	testDataNode.HostgroupId = 100
	testDataNode.Hostgroups = []Hostgroup{currentHg}
	clusterNode.WriterNodes[testDataNode.Dns] = testDataNode

	myArgs := args{clusterNode.ReaderNodes["127.0.0.1:3306"], clusterNode.ReaderNodes["127.0.0.1:3306"].Hostgroups[0]}

	tests = rulesTestProcessWriterIsAlsoReader(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//t.Error("  AAA ", tt.args.node.ActionType)
			//we must rebuild the readers given tests will modify the composition
			dns := []string{"127.0.0.1:3306", "127.0.0.1:3307"}
			for _, myDns := range dns {
				testDataNode := clusterNode.testDataNodeFactoryDns(myDns)
				currentHg := getHgOpt(101, 3, "R")
				testDataNode.HostgroupId = 101
				testDataNode.Hostgroups = []Hostgroup{currentHg}
				clusterNode.ReaderNodes[myDns] = testDataNode

			}
			cluster := clusterNode.clusterNodeImplFactory()
			cluster.WriterNodes[tt.args.node.Dns] = tt.args.node
			cluster.ReaderNodes = clusterNode.ReaderNodes
			if tt.testClusterNodeImp.WriterIsReader == 100 {
				tt.testClusterNodeImp.WriterIsReader = 0
				delete(cluster.ReaderNodes, "127.0.0.1:3307")
			}

			cluster.WriterIsReader = tt.testClusterNodeImp.WriterIsReader
			cluster.Hostgroups[tt.args.node.Hostgroups[0].Id] = tt.args.node.Hostgroups[0]
			//t.Error("  AAA ", cluster.WriterIsReader, "  ", len(cluster.ReaderNodes), " ")

			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.processWriterIsAlsoReader(cluster.ReaderNodes); got != tt.want {

				t.Errorf(" %s processWriterIsAlsoReader() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}


func TestDataClusterImpl_identifyLowerNodeToRemove(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	dns := []string{"127.0.0.1:3306", "127.0.0.1:3307", "127.0.0.1:3308"}

	weight := 1000
	for _, myDns := range dns {
		testDataNode := clusterNode.testDataNodeFactoryDns(myDns)
		currentHg := getHgOpt(100, 3, "W")
		testDataNode.HostgroupId = 100
		testDataNode.Hostgroups = []Hostgroup{currentHg}
		testDataNode.Weight = weight
		weight--
		clusterNode.WriterNodes[myDns] = testDataNode
	}
	myBackupWriterNode := clusterNode.testDataNodeFactoryDns("127.0.0.5:3306")
	myBackupWriterNode.HostgroupId=8100
	myBackupWriterNode.Hostgroups = []Hostgroup{getHgOpt(8100, 3, "W")}
	myBackupWriterNode.Weight = 1000

	myArgs := args{myBackupWriterNode, myBackupWriterNode.Hostgroups[0]}

	tests = rulesTestProcessIdentifyLowerNodeToRemove(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//t.Error("  AAA ", tt.args.node.ActionType)
			cluster := clusterNode.clusterNodeImplFactory()
			testNode := tt.args.node
			cluster.WriterNodes = clusterNode.WriterNodes
			cluster.Hostgroups[tt.args.node.Hostgroups[0].Id] = tt.args.node.Hostgroups[0]
			if got := cluster.identifyLowerNodeToRemove(testNode); got != tt.want {

				t.Errorf(" %s TestDataClusterImpl_identifyLowerNodeToRemove() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}



func TestDataClusterImpl_identifyLowerNodeToRemoveBecauseFailback(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	dns := []string{"127.0.0.1:3307", "127.0.0.1:3308"}

	weight := 1000
	for _, myDns := range dns {
		testDataNode := clusterNode.testDataNodeFactoryDns(myDns)
		currentHg := getHgOpt(100, 3, "W")
		testDataNode.HostgroupId = 100
		testDataNode.Hostgroups = []Hostgroup{currentHg}
		testDataNode.Weight = weight
		weight--
		clusterNode.WriterNodes[myDns] = testDataNode
	}
	myFailBackWriterNode := clusterNode.testDataNodeFactoryDns("127.0.0.1:3306")
	myFailBackWriterNode.HostgroupId=100
	myFailBackWriterNode.Hostgroups = []Hostgroup{getHgOpt(100, 3, "W")}
	myFailBackWriterNode.Weight = 1000
	myFailBackWriterNode.WsrepSegment=1

	myArgs := args{myFailBackWriterNode, myFailBackWriterNode.Hostgroups[0]}

	tests = rulesTestProcessIdentifyLowerNodeToRemoveForFailback(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			testNode := tt.args.node
			cluster.WriterNodes = clusterNode.WriterNodes
			cluster.Hostgroups[tt.args.node.Hostgroups[0].Id] = tt.args.node.Hostgroups[0]
			cluster.MainSegment =1
			cluster.ActiveFailover = 2
			if got := cluster.identifyLowerNodeToRemoveBecauseFailback(testNode); got != tt.want {

				t.Errorf(" %s TestDataClusterImpl_identifyLowerNodeToRemoveBecauseFailback() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
/*
Test Cluster method evaluateReaders [end]
*/

func TestDataClusterImpl_checkUpSaveRetry(t *testing.T) {
	var tests = []rule{}

	clusterNode := testClusterFactory()
	testDataNode := clusterNode.testDataNodeFactory()
	currentHg := getHg()
	testDataNode.Hostgroups = []Hostgroup{currentHg}

	myArgs := args{testDataNode, testDataNode.Hostgroups[0]}

	tests = rulesTestCheckUpSaveRetry(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			if got := cluster.checkUpSaveRetry(tt.args.node, tt.args.currentHg); got != tt.want {
				t.Errorf(" %s checkUpSaveRetry() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestDataClusterImpl_checkActiveFailover(t *testing.T) {
	tests := []struct {
		name           string
		activeFailover int
		want           bool
	}{
		{"ActiveFailover=0 should disable failover", 0, false},
		{"ActiveFailover=1 should enable failover", 1, true},
		{"ActiveFailover=2 multi-segment failover enabled", 2, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterNode := testClusterFactory()
			cluster := clusterNode.clusterNodeImplFactory()
			cluster.ActiveFailover = tt.activeFailover
			if got := cluster.checkActiveFailover(); got != tt.want {
				t.Errorf("checkActiveFailover() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataClusterImpl_checkFailoverIfFound(t *testing.T) {
	tests := []struct {
		name            string
		requireFailover bool
		hasWriter       bool
		failoverHg      int
		want            bool
	}{
		{"No failover required", false, true, 0, true},
		{"Failover required writers present", true, true, 0, true},
		{"Failover required no writer no candidate", true, false, 0, false},
		{"Failover required no writer candidate exists", true, false, 100, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterNode := testClusterFactory()
			cluster := clusterNode.clusterNodeImplFactory()
			cluster.RequireFailover = tt.requireFailover
			if tt.hasWriter {
				cluster.WriterNodes["127.0.0.1:3306"] = clusterNode.testDataNodeFactory()
			}
			cluster.FailOverNode = DataNodeImpl{HostgroupId: tt.failoverHg}
			if got := cluster.checkFailoverIfFound(); got != tt.want {
				t.Errorf("checkFailoverIfFound() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataNodeImpl_getRetry(t *testing.T) {
	tests := []struct {
		name     string
		comment  string
		writeHG  int
		readHG   int
		wantUp   int
		wantDown int
	}{
		{"Empty comment no retry", "", 100, 101, 0, 0},
		{"Comment with retry_up=2", "100_W_101_R_retry_up=2;", 100, 101, 2, 0},
		{"Comment with retry_down=3", "100_W_101_R_retry_down=3;", 100, 101, 0, 3},
		{"Comment with both up and down", "100_W_101_R_retry_up=1;100_W_101_R_retry_down=2;", 100, 101, 1, 2},
		{"Comment with wrong HG IDs no match", "200_W_201_R_retry_up=5;", 100, 101, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := DataNodeImpl{Comment: tt.comment}
			node.getRetry(tt.writeHG, tt.readHG)
			if node.RetryUp != tt.wantUp {
				t.Errorf("getRetry() RetryUp = %v, want %v", node.RetryUp, tt.wantUp)
			}
			if node.RetryDown != tt.wantDown {
				t.Errorf("getRetry() RetryDown = %v, want %v", node.RetryDown, tt.wantDown)
			}
		})
	}
}

func TestDataClusterImpl_alignNodeValues(t *testing.T) {
	clusterNode := testClusterFactory()
	cluster := clusterNode.clusterNodeImplFactory()

	dst := clusterNode.testDataNodeFactory()
	src := clusterNode.testDataNodeFactory()
	src.ActionType = 3001
	src.Processed = false

	result := cluster.alignNodeValues(dst, src)

	if result.ActionType != src.ActionType {
		t.Errorf("alignNodeValues() ActionType = %v, want %v", result.ActionType, src.ActionType)
	}
	if result.Processed != src.Processed {
		t.Errorf("alignNodeValues() Processed = %v, want %v", result.Processed, src.Processed)
	}
	if result.Status == nil {
		t.Error("alignNodeValues() Status should not be nil")
	}
	if result.Variables == nil {
		t.Error("alignNodeValues() Variables should not be nil")
	}
}

func TestDataClusterImpl_compareNodeDefault(t *testing.T) {
	clusterNode := testClusterFactory()
	cluster := clusterNode.clusterNodeImplFactory()
	base := clusterNode.testDataNodeFactory()

	tests := []struct {
		name   string
		nodeIn DataNodeImpl
		want   bool
	}{
		{"Identical nodes returns false no difference", base, false},
		{"Different Weight returns true", changeDataObjectAnyAttribute(base, "Weight", valueGeneric{Int64: 9999}), true},
		{"Different MaxConnection returns true", changeDataObjectAnyAttribute(base, "MaxConnection", valueGeneric{Int64: 9999}), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cluster.compareNodeDefault(tt.nodeIn, base); got != tt.want {
				t.Errorf("compareNodeDefault() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataClusterImpl_resetNodeDefault(t *testing.T) {
	clusterNode := testClusterFactory()
	cluster := clusterNode.clusterNodeImplFactory()

	nodeDefault := clusterNode.testDataNodeFactory()
	nodeDefault.Weight = 1000
	nodeDefault.Compression = 5
	nodeDefault.MaxConnection = 500
	nodeDefault.MaxReplicationLag = 100
	nodeDefault.MaxLatency = 50

	nodeIn := clusterNode.testDataNodeFactory()
	nodeIn.Weight = 1
	nodeIn.Compression = 0
	nodeIn.MaxConnection = 300
	nodeIn.MaxReplicationLag = 0
	nodeIn.MaxLatency = 100

	result := cluster.resetNodeDefault(nodeIn, nodeDefault)

	if result.Weight != nodeDefault.Weight {
		t.Errorf("resetNodeDefault() Weight = %v, want %v", result.Weight, nodeDefault.Weight)
	}
	if result.Compression != nodeDefault.Compression {
		t.Errorf("resetNodeDefault() Compression = %v, want %v", result.Compression, nodeDefault.Compression)
	}
	if result.MaxConnection != nodeDefault.MaxConnection {
		t.Errorf("resetNodeDefault() MaxConnection = %v, want %v", result.MaxConnection, nodeDefault.MaxConnection)
	}
	if result.MaxReplicationLag != nodeDefault.MaxReplicationLag {
		t.Errorf("resetNodeDefault() MaxReplicationLag = %v, want %v", result.MaxReplicationLag, nodeDefault.MaxReplicationLag)
	}
	if result.MaxLatency != nodeDefault.MaxLatency {
		t.Errorf("resetNodeDefault() MaxLatency = %v, want %v", result.MaxLatency, nodeDefault.MaxLatency)
	}
}

func TestDataClusterImpl_checkMissingForNodes(t *testing.T) {
	t.Run("Backup reader missing from eval map should be added", func(t *testing.T) {
		clusterNode := testClusterFactory()
		cluster := clusterNode.clusterNodeImplFactory()
		cluster.BackupReaders["127.0.0.1:3307"] = clusterNode.testDataNodeFactoryDns("127.0.0.1:3307")
		evalMap := map[string]DataNodeImpl{
			"127.0.0.1:3306": clusterNode.testDataNodeFactoryDns("127.0.0.1:3306"),
		}
		result := cluster.checkMissingForNodes(evalMap)
		if len(result) != 2 {
			t.Errorf("checkMissingForNodes() map size = %v, want 2", len(result))
		}
		if node, ok := result["127.0.0.1:3307"]; !ok || !node.NodeIsNew {
			t.Error("checkMissingForNodes() added node should have NodeIsNew=true")
		}
	})

	t.Run("Backup reader already in eval map should not be duplicated", func(t *testing.T) {
		clusterNode := testClusterFactory()
		cluster := clusterNode.clusterNodeImplFactory()
		cluster.BackupReaders["127.0.0.1:3306"] = clusterNode.testDataNodeFactoryDns("127.0.0.1:3306")
		evalMap := map[string]DataNodeImpl{
			"127.0.0.1:3306": clusterNode.testDataNodeFactoryDns("127.0.0.1:3306"),
		}
		result := cluster.checkMissingForNodes(evalMap)
		if len(result) != 1 {
			t.Errorf("checkMissingForNodes() map size = %v, want 1", len(result))
		}
	})

	t.Run("Backup writer missing from eval map should be added as reader", func(t *testing.T) {
		clusterNode := testClusterFactory()
		cluster := clusterNode.clusterNodeImplFactory()
		cluster.BackupWriters["127.0.0.1:3308"] = clusterNode.testDataNodeFactoryDns("127.0.0.1:3308")
		evalMap := map[string]DataNodeImpl{}
		result := cluster.checkMissingForNodes(evalMap)
		if len(result) != 1 {
			t.Errorf("checkMissingForNodes() map size = %v, want 1", len(result))
		}
		if node, ok := result["127.0.0.1:3308"]; !ok || node.HostgroupId != cluster.HgReaderId {
			t.Error("checkMissingForNodes() added writer backup should use HgReaderId")
		}
	})
}

func TestDataClusterImpl_pushNewNode(t *testing.T) {
	tests := []struct {
		name            string
		nodeInOffline   bool
		wantResult      bool
		wantActionCount int
	}{
		{"New node not in offline readers should be scheduled", false, true, 2},
		{"Node already in offline readers should be skipped", true, false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterNode := testClusterFactory()
			cluster := clusterNode.clusterNodeImplFactory()
			node := clusterNode.testDataNodeFactory()
			if tt.nodeInOffline {
				cluster.OffLineReaders[node.Dns] = node
			}
			got := cluster.pushNewNode(node)
			if got != tt.wantResult {
				t.Errorf("pushNewNode() = %v, want %v", got, tt.wantResult)
			}
			if len(cluster.ActionNodes) != tt.wantActionCount {
				t.Errorf("pushNewNode() ActionNodes count = %v, want %v", len(cluster.ActionNodes), tt.wantActionCount)
			}
		})
	}
}

func TestDataClusterImpl_cleanWriters_lastWriter(t *testing.T) {
	t.Run("Last writer offline with ActiveFailover sets RequireFailover", func(t *testing.T) {
		clusterNode := testClusterFactory()
		cluster := clusterNode.clusterNodeImplFactory()
		cluster.ActiveFailover = 1
		node := clusterNode.testDataNodeFactory()
		node.ProxyStatus = "OFFLINE_SOFT"
		cluster.WriterNodes[node.Dns] = node

		cluster.cleanWriters()

		if !cluster.RequireFailover {
			t.Error("cleanWriters() RequireFailover should be true when last writer goes offline with ActiveFailover=1")
		}
		if cluster.HasPrimary {
			t.Error("cleanWriters() HasPrimary should be false when last writer goes offline")
		}
	})

	t.Run("Last writer offline without ActiveFailover does not set RequireFailover", func(t *testing.T) {
		clusterNode := testClusterFactory()
		cluster := clusterNode.clusterNodeImplFactory()
		cluster.ActiveFailover = 0
		node := clusterNode.testDataNodeFactory()
		node.ProxyStatus = "OFFLINE_SOFT"
		cluster.WriterNodes[node.Dns] = node

		cluster.cleanWriters()

		if cluster.RequireFailover {
			t.Error("cleanWriters() RequireFailover should be false when ActiveFailover=0")
		}
		if cluster.HasPrimary {
			t.Error("cleanWriters() HasPrimary should be false when last writer goes offline")
		}
	})
}
