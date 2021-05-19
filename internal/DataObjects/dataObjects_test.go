
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

package DataObjects

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

	myArgs := args{testDataNode,testDataNode.Hostgroups[0],}

	tests = rulesTestCheckWsrepDesync(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			if got := cluster.checkWsrepDesync(tt.args.node, tt.args.currentHg); got != tt.want {
				t.Errorf(" %s checkWsrepDesync() = %v, want %v",tt.name, got, tt.want)
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

	myArgs := args{testDataNode,testDataNode.Hostgroups[0],}

	tests = rulesTestcheckAnyNotReadyStatus(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			if got := cluster.checkAnyNotReadyStatus(tt.args.node, tt.args.currentHg); got != tt.want {
				t.Errorf(" %s checkAnyNotReadyStatus() = %v, want %v",tt.name, got, tt.want)
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

	myArgs := args{testDataNode,testDataNode.Hostgroups[0],}

	tests = rulesTestcheckNotPrimary(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			if got := cluster.checkNotPrimary(tt.args.node, tt.args.currentHg); got != tt.want {
				t.Errorf(" %s checkNotPrimary() = %v, want %v",tt.name, got, tt.want)
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

	myArgs := args{testDataNode,testDataNode.Hostgroups[0],}

	tests = rulesTestcheckRejectQueries(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			if got := cluster.checkRejectQueries(tt.args.node, tt.args.currentHg); got != tt.want {
				t.Errorf(" %s checkRejectQueries() = %v, want %v",tt.name, got, tt.want)
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

	myArgs := args{testDataNode,testDataNode.Hostgroups[0],}

	tests = rulesTestcheckDonorReject(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.checkDonorReject(tt.args.node, tt.args.currentHg); got != tt.want {

				t.Errorf(" %s checkDonorReject() = %v, want %v",tt.name, got, tt.want)
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

	myArgs := args{testDataNode,testDataNode.Hostgroups[0],}

	tests = rulesTestcheckPxcMaint(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.checkPxcMaint(tt.args.node, tt.args.currentHg); got != tt.want {

				t.Errorf(" %s checkPxcMaint() = %v, want %v",tt.name, got, tt.want)
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

	myArgs := args{testDataNode,testDataNode.Hostgroups[0],}

	tests = rulesTestCheckReadOnly(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.checkReadOnly(tt.args.node, tt.args.currentHg); got != tt.want {

				t.Errorf(" %s checkReadOnly() = %v, want %v",tt.name, got, tt.want)
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

	myArgs := args{testDataNode,testDataNode.Hostgroups[0],}

	tests = rulesTestCheckBackOffLine(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if _,got := cluster.checkBackOffline(tt.args.node, tt.args.currentHg); got != tt.want {

				t.Errorf(" %s checkBackOffLine() = %v, want %v",tt.name, got, tt.want)
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

	myArgs := args{testDataNode,testDataNode.Hostgroups[0],}

	tests = rulesTestCheckBackNew(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.checkBackNew(tt.args.node); got != tt.want {

				t.Errorf(" %s checkBackNew() = %v, want %v",tt.name, got, tt.want)
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

	myArgs := args{testDataNode,testDataNode.Hostgroups[0],}

	tests = rulesTestCheckBackPrimary(myArgs, clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := clusterNode.clusterNodeImplFactory()
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.checkBackPrimary(tt.args.node,tt.args.currentHg); got != tt.want {

				t.Errorf(" %s checkBackPrimary() = %v, want %v",tt.name, got, tt.want)
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
	dns := []string{"127.0.0.1:3306", "127.0.0.1:3307","127.0.0.1:3308"}
	for _, myDns := range dns {
		testDataNode := clusterNode.testDataNodeFactoryDns(myDns)
		currentHg := getHg()
		testDataNode.Hostgroups = []Hostgroup{currentHg}
		clusterNode.WriterNodes[myDns] = testDataNode

	}
	myArgs := args{clusterNode.WriterNodes["127.0.0.1:3306"],clusterNode.WriterNodes["127.0.0.1:3306"].Hostgroups[0],}

	tests = rulesTestCleanWriters(myArgs,clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterNode.WriterNodes[tt.args.node.Dns]=tt.args.node
			cluster := clusterNode.clusterNodeImplFactory()
			cluster.WriterNodes = clusterNode.WriterNodes
			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.cleanWriters(); got != tt.want {

				t.Errorf(" %s checkBackPrimary() = %v, want %v",tt.name, got, tt.want)
			}
		})
	}
}

//testing Primary Election
func TestDataClusterImpl_identifyPrimaryBackupNode(t *testing.T) {
	var tests = []ruleInt{}

	clusterNode := testClusterFactory()
	dns := []string{"127.0.0.1:3306", "127.0.0.1:3307","127.0.0.1:3308"}
	for _, myDns := range dns {
		testDataNode := clusterNode.testDataNodeFactoryDns(myDns)
		currentHg := getHg()
		testDataNode.Hostgroups = []Hostgroup{currentHg}
		clusterNode.WriterNodes[myDns] = testDataNode
	}
	node := clusterNode.WriterNodes["127.0.0.1:3306"]
	node.Weight = 10000
	clusterNode.WriterNodes["127.0.0.1:3306"] = node


	myArgs := args{clusterNode.WriterNodes["127.0.0.1:3306"],clusterNode.WriterNodes["127.0.0.1:3306"].Hostgroups[0],}

	tests = rulesIdentifyPrimaryBackupNode(myArgs,clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterNode.WriterNodes[tt.args.node.Dns]=tt.args.node
			cluster := clusterNode.clusterNodeImplFactory()
			cluster.WriterNodes = clusterNode.WriterNodes
			cluster.FailOverNode = clusterNode.WriterNodes["127.0.0.1:3307"]
			cluster.PersistPrimarySettings = tt.testClusterNodeImp.PersistPrimarySettings

			value := cluster.identifyPrimaryBackupNode("127.0.0.1:3306")
			//t.Error( "aa ",tt.testClusterNodeImp.PersistPrimarySettings, " ", value)
			if value != tt.want {

				t.Errorf(" %s identifyPrimaryBackupNode() = %v want %v",tt.name, value, tt.want)
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
	dns := []string{"127.0.0.1:3306", "127.0.0.1:3307","127.0.0.1:3308"}
	for _, myDns := range dns {
		testDataNode := clusterNode.testDataNodeFactoryDns(myDns)
		currentHg := getHgOpt(101,3,"R")
		testDataNode.HostgroupId = 101
		testDataNode.Hostgroups = []Hostgroup{currentHg}
		clusterNode.ReaderNodes[myDns] = testDataNode

	}
	myArgs := args{clusterNode.ReaderNodes["127.0.0.1:3306"],clusterNode.ReaderNodes["127.0.0.1:3306"].Hostgroups[0],}

	tests = rulesTestProcessUpAndDownReaders(myArgs,clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//t.Error("  AAA ", tt.args.node.ActionType)
			cluster := clusterNode.clusterNodeImplFactory()
			cluster.ActionNodes[tt.args.node.Dns]=tt.args.node
			cluster.ReaderNodes = clusterNode.ReaderNodes
			cluster.Hostgroups[tt.args.node.Hostgroups[0].Id] = tt.args.node.Hostgroups[0]
			//t.Error("  AAA ", tt.args.node.ActionType, "  ", len(cluster.ActionNodes), " ", cluster.ActionNodes[tt.args.node.Dns].ActionType, " " , cluster.Hostgroups[101].Type)

			//t.Error( "aa ",tt.args.node.WsrepDonorrejectqueries, " ", tt.args.currentHg.Size)
			if got := cluster.processUpAndDownReaders(cluster.ActionNodes, cluster.ReaderNodes); got != tt.want {

				t.Errorf(" %s processUpAndDownReaders() = %v, want %v",tt.name, got, tt.want)
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
		currentHg := getHgOpt(101,3,"R")
		testDataNode.HostgroupId = 101
		testDataNode.Hostgroups = []Hostgroup{currentHg}
		clusterNode.ReaderNodes[myDns] = testDataNode

	}
	testDataNode := clusterNode.testDataNodeFactoryDns("127.0.0.1:3306")
	currentHg := getHgOpt(100,1,"W")
	testDataNode.HostgroupId = 100
	testDataNode.Hostgroups = []Hostgroup{currentHg}
	clusterNode.WriterNodes[testDataNode.Dns] = testDataNode


	myArgs := args{clusterNode.ReaderNodes["127.0.0.1:3306"],clusterNode.ReaderNodes["127.0.0.1:3306"].Hostgroups[0],}

	tests = rulesTestProcessWriterIsAlsoReader(myArgs,clusterNode)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//t.Error("  AAA ", tt.args.node.ActionType)
			//we must rebuild the readers given tests will modify the composition
			dns := []string{"127.0.0.1:3306", "127.0.0.1:3307"}
			for _, myDns := range dns {
				testDataNode := clusterNode.testDataNodeFactoryDns(myDns)
				currentHg := getHgOpt(101,3,"R")
				testDataNode.HostgroupId = 101
				testDataNode.Hostgroups = []Hostgroup{currentHg}
				clusterNode.ReaderNodes[myDns] = testDataNode

			}
			cluster := clusterNode.clusterNodeImplFactory()
			cluster.WriterNodes[tt.args.node.Dns]=tt.args.node
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

				t.Errorf(" %s processWriterIsAlsoReader() = %v, want %v",tt.name, got, tt.want)
			}
		})
	}
}


/*
Test Cluster method evaluateReaders [end]
*/