package dataobjects_test

import (
	"global"
	"testing"

	DO "dataobjects"
	domock "dataobjects/mock"
	dbmock "dbwrapper/mock"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type LockerSuite struct {
	suite.Suite
	*require.Assertions

	ctrl *gomock.Controller
}

func TestLockerSuite(t *testing.T) {
	suite.Run(t, new(LockerSuite))
}

// ran before every test
func (s *LockerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ctrl = gomock.NewController(s.T())
}

// ran after every test
func (s *LockerSuite) TearDownTest() {
	s.ctrl.Finish()
}

// some default configuration
var gConfig = global.Configuration{
	ProxySQL: global.ProxySQLConfig{
		Port: 1000,
	},
	PxcCluster: global.PxcClusterConfig{
		SinglePrimary: true,
		Host:          "defaultHost",
		MaxNumWriters: 1,
		HgW:           1,
		BckHgW:        8001,
		HgR:           2,
		BckHgR:        8002,
	},
	Global: global.SchedulerConfig{
		Debug: true,
	},
}

func (s *LockerSuite) TestInitNilConfig() {
	locker := DO.Locker{}

	clusterFactoryCalled := false
	nodeFactoryCalled := false
	res := locker.Init(nil,
		func(string) DO.ProxySQLNode {
			nodeFactoryCalled = true
			return nil
		},
		func(string, string) DO.ProxySQLCluster {
			clusterFactoryCalled = true
			return nil
		})

	assert.False(s.T(), clusterFactoryCalled)
	assert.False(s.T(), nodeFactoryCalled)
	assert.False(s.T(), res)
}

func (s *LockerSuite) TestInit() {
	locker := DO.Locker{}

	clusterFactoryCalled := false
	nodeFactoryCalled := false
	res := locker.Init(&gConfig,
		func(string) DO.ProxySQLNode {
			nodeFactoryCalled = true
			return nil
		},
		func(string, string) DO.ProxySQLCluster {
			clusterFactoryCalled = true
			return nil
		})

	assert.False(s.T(), clusterFactoryCalled)
	assert.True(s.T(), nodeFactoryCalled)
	assert.True(s.T(), res)
}

func (s *LockerSuite) Test_ClusterLock_ServerNotInitializedInitFails() {
	locker := DO.Locker{}

	// setup
	nodeFactory := func(ip string) DO.ProxySQLNode {
		mock := domock.NewMockProxySQLNode(s.ctrl)
		mock.EXPECT().IsInitialized().Return(false).Times(1)
		mock.EXPECT().Init(gomock.Not(gomock.Nil())).Return(false).Times(1)
		return mock
	}

	clusterFactoryCalled := false

	// act
	locker.Init(&gConfig, nodeFactory, func(string, string) DO.ProxySQLCluster {
		clusterFactoryCalled = true
		return nil
	})

	res := locker.ClusterLock()

	// assert
	assert.Nil(s.T(), res)
	assert.False(s.T(), clusterFactoryCalled)

}

func (s *LockerSuite) Test_ClusterLock_ServerNotInitializedInitOK() {
	locker := DO.Locker{}

	// setup
	nodeFactory := func(ip string) DO.ProxySQLNode {
		mock := domock.NewMockProxySQLNode(s.ctrl)
		mock.EXPECT().IsInitialized().Return(false).Times(2)
		mock.EXPECT().Init(gomock.Not(gomock.Nil())).Return(true).Times(1)
		return mock
	}

	clusterFactoryCalled := false

	// act
	locker.Init(&gConfig, nodeFactory, func(string, string) DO.ProxySQLCluster {
		clusterFactoryCalled = true
		return nil
	})

	res := locker.ClusterLock()

	// assert
	assert.Nil(s.T(), res)
	assert.False(s.T(), clusterFactoryCalled)
}

func (s *LockerSuite) TestLocker_ClusterLock_ZeroCluserNodes() {
	locker := DO.Locker{}

	// setup
	nodeFactory := func(ip string) DO.ProxySQLNode {
		mock := domock.NewMockProxySQLNode(s.ctrl)
		mock.EXPECT().IsInitialized().Return(true).Times(2)
		return mock
	}

	clusterFactory := func(string, string) DO.ProxySQLCluster {
		nodes := make(map[string]DO.ProxySQLNode)
		mock := domock.NewMockProxySQLCluster(s.ctrl)
		mock.EXPECT().FetchProxySQLNodes(gomock.Not(gomock.Nil())).Times(1)
		mock.EXPECT().Nodes().Return(nodes).Times(1)
		return mock
	}

	config := gConfig
	config.ProxySQL.Clustered = true

	// act
	locker.Init(&config, nodeFactory, clusterFactory)

	res := locker.ClusterLock()

	// assert
	assert.Nil(s.T(), res)
}

func (s *LockerSuite) Test_ClusterLock_CurrentNodeLockPossible() {
	locker := DO.Locker{}

	// setup
	nodeFactory := func(ip string) DO.ProxySQLNode {
		// transaction
		txMock := dbmock.NewMockDBTransaction(s.ctrl)
		txMock.EXPECT().Commit().Times(1)
		txMock.EXPECT().ExecContext(gomock.Any(), gomock.Any()).Times(1)

		// db connection mock
		connectionMock := dbmock.NewMockDBConnection(s.ctrl)
		connectionMock.EXPECT().BeginTx(gomock.Any()).Times(1).Return(txMock, nil)
		connectionMock.EXPECT().Exec(gomock.Any()).AnyTimes().Return(nil)

		mock := domock.NewMockProxySQLNode(s.ctrl)
		mock.EXPECT().IsInitialized().AnyTimes().Return(true)
		mock.EXPECT().Init(gomock.Not(gomock.Nil())).Times(0)
		mock.EXPECT().Dns().AnyTimes().Return("NodeMock_defaultDNS")
		mock.EXPECT().Connection().AnyTimes().Return(connectionMock)

		//mock.EXPECT().SetComment(gomock.Any()).AnyTimes()
		return mock
	}

	clusterFactory := func(string, string) DO.ProxySQLCluster {
		// our node
		nodes := make(map[string]DO.ProxySQLNode)
		nodeMock := domock.NewMockProxySQLNode(s.ctrl)
		nodeMock.EXPECT().Comment().AnyTimes().Return("#LOCK_1_HG_1_W_HG_2_R_")
		nodeMock.EXPECT().SetComment(gomock.Any()).AnyTimes()
		nodeMock.EXPECT().Dns().AnyTimes().Return("NodeMock_defaultDNS")
		nodeMock.EXPECT().Ip().AnyTimes().Return("192.168.0.1")
		nodeMock.EXPECT().Port().AnyTimes().Return(1234)

		nodes["NodeMock_defaultDNS"] = nodeMock

		clusterMock := domock.NewMockProxySQLCluster(s.ctrl)
		clusterMock.EXPECT().FetchProxySQLNodes(gomock.Not(gomock.Nil())).Times(1)
		clusterMock.EXPECT().Nodes().Return(nodes).AnyTimes()
		return clusterMock
	}
	config := gConfig
	config.ProxySQL.Clustered = true

	// act
	locker.Init(&config, nodeFactory, clusterFactory)

	res := locker.ClusterLock()

	// assert
	assert.NotNil(s.T(), res)
	assert.Equal(s.T(), res.Dns(), "NodeMock_defaultDNS")
}
