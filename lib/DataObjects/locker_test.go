package dataobjects_test

import (
	"database/sql"
	"testing"

	DO "../DataObjects"
	global "../Global"
)

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

type ProxySQLNodeMock struct {
	SetActionNodeListFn func(map[string]DO.DataNodePxc)
	DnsFn               func() string
	IpFn                func() string
	PortFn              func() int
	ConnectionFn        func() *sql.DB
	MySQLClusterFn      func() *DO.DataCluster
	IsInitializedFn     func() bool
	SetHoldLockFn       func(bool)
	SetLockExpiredFn    func(bool)
	LockExpiredFn       func() bool
	SetLastLockTimeFn   func(int64)
	LastLockTimeFn      func() int64
	SetCommentFn        func(string)
	CommentFn           func() string

	InitFn             func(*global.Configuration) bool
	CloseConnectionFn  func() bool
	FetchDataClusterFn func(*global.Configuration) bool
	ProcessChangesFn   func() bool
}

func (m *ProxySQLNodeMock) SetActionNodeList(v map[string]DO.DataNodePxc) {
	m.SetActionNodeListFn(v)
}
func (m *ProxySQLNodeMock) Dns() string {
	return m.DnsFn()
}
func (m *ProxySQLNodeMock) Ip() string {
	return m.IpFn()
}
func (m *ProxySQLNodeMock) Port() int {
	return m.PortFn()
}
func (m *ProxySQLNodeMock) Connection() *sql.DB {
	return m.ConnectionFn()
}
func (m *ProxySQLNodeMock) MySQLCluster() *DO.DataCluster {
	return m.MySQLClusterFn()
}
func (m *ProxySQLNodeMock) IsInitialized() bool {
	return m.IsInitializedFn()
}
func (m *ProxySQLNodeMock) SetHoldLock(v bool) {
	m.SetHoldLockFn(v)
}
func (m *ProxySQLNodeMock) SetLockExpired(v bool) {
	m.SetLockExpiredFn(v)
}
func (m *ProxySQLNodeMock) LockExpired() bool {
	return m.LockExpiredFn()
}
func (m *ProxySQLNodeMock) SetLastLockTime(v int64) {
	m.SetLastLockTimeFn(v)
}
func (m *ProxySQLNodeMock) LastLockTime() int64 {
	return m.LastLockTimeFn()
}
func (m *ProxySQLNodeMock) SetComment(v string) {
	m.SetCommentFn(v)
}
func (m *ProxySQLNodeMock) Comment() string {
	return m.CommentFn()
}
func (m *ProxySQLNodeMock) Init(v *global.Configuration) bool {
	return m.InitFn(v)
}
func (m *ProxySQLNodeMock) CloseConnection() bool {
	return m.CloseConnectionFn()
}
func (m *ProxySQLNodeMock) FetchDataCluster(v *global.Configuration) bool {
	return m.FetchDataClusterFn(v)
}
func (m *ProxySQLNodeMock) ProcessChanges() bool {
	return m.ProcessChangesFn()
}

func NewProxySQLNodeMock(v string) DO.ProxySQLNode {
	mock := ProxySQLNodeMock{
		IsInitializedFn: func() bool { return true },
		InitFn:          func(*global.Configuration) bool { return true },
		DnsFn:           func() string { return "NodeMock_defaultDNS" },
	}
	return &mock
}

type ProxySQLClusterMock struct {
	NodesFn              func() map[string]DO.ProxySQLNode
	FetchProxySQLNodesFn func(DO.ProxySQLNode)
}

func (m *ProxySQLClusterMock) Nodes() map[string]DO.ProxySQLNode {
	return m.NodesFn()
}
func (m *ProxySQLClusterMock) FetchProxySQLNodes(v DO.ProxySQLNode) {
	m.FetchProxySQLNodesFn(v)
}

func NewProxySQLClusterMock(user string, password string) DO.ProxySQLCluster {
	v := ProxySQLClusterMock{}
	return &v
}

func TestLocker_Init_NilConfig(t *testing.T) {
	locker := DO.Locker{}
	if locker.Init(nil, NewProxySQLNodeMock, NewProxySQLClusterMock) {
		t.Errorf("Expected intitialization failure")
	}
}

func TestLocker_Init(t *testing.T) {
	locker := DO.Locker{}
	if !locker.Init(&gConfig, NewProxySQLNodeMock, NewProxySQLClusterMock) {
		t.Errorf("Expected intitialization success")
	}
}

func TestLocker_ClusterLock_ServerNotInitializedInitFails(t *testing.T) {
	locker := DO.Locker{}

	// setup
	proxySQLNodeFactory := func(ip string) DO.ProxySQLNode {
		mock := ProxySQLNodeMock{
			IsInitializedFn: func() bool { return false },
			InitFn:          func(c *global.Configuration) bool { return false },
		}
		return &mock
	}

	// act
	locker.Init(&gConfig, proxySQLNodeFactory, NewProxySQLClusterMock)

	res := locker.ClusterLock()

	// assert
	if res != nil {
		t.Errorf("ClusterLock should fail")
	}
}

func TestLocker_ClusterLock_ServerNotInitializedInitOK(t *testing.T) {
	locker := DO.Locker{}

	// setup
	proxySQLNodeFactory := func(ip string) DO.ProxySQLNode {
		mock := ProxySQLNodeMock{
			IsInitializedFn: func() bool { return false },
			InitFn:          func(c *global.Configuration) bool { return true },
		}
		return &mock
	}

	// act
	locker.Init(&gConfig, proxySQLNodeFactory, NewProxySQLClusterMock)

	res := locker.ClusterLock()

	// assert
	if res != nil {
		t.Errorf("ClusterLock should fail")
	}
}

func TestLocker_ClusterLock_ZeroCluserNodes(t *testing.T) {
	locker := DO.Locker{}

	// setup
	proxySQLClusterFactory := func(string, string) DO.ProxySQLCluster {
		mock := ProxySQLClusterMock{
			FetchProxySQLNodesFn: func(DO.ProxySQLNode) {},
			NodesFn: func() map[string]DO.ProxySQLNode {
				n := make(map[string]DO.ProxySQLNode)
				return n
			},
		}
		return &mock
	}

	// act
	locker.Init(&gConfig, NewProxySQLNodeMock, proxySQLClusterFactory)

	res := locker.ClusterLock()

	// assert
	if res != nil {
		t.Errorf("ClusterLock should fail")
	}
}

/*
func TestLocker_ClusterLock_CurrentNodeLockPossible(t *testing.T) {
	locker := DO.Locker{}

	// setup
	proxySQLClusterFactory := func(string, string) DO.ProxySQLCluster {
		mock := ProxySQLClusterMock{
			FetchProxySQLNodesFn: func(DO.ProxySQLNode) {},
			NodesFn: func() map[string]DO.ProxySQLNode {
				nodes := make(map[string]DO.ProxySQLNode)
				nodeMock := ProxySQLNodeMock{
					CommentFn:         func() string { return "#LOCK_1_HG_1_W_HG_2_R_" },
					SetHoldLockFn:     func(bool) {},
					SetLastLockTimeFn: func(int64) {},
					SetCommentFn:      func(string) {},
					LastLockTimeFn:    func() int64 { return 0 },
					SetLockExpiredFn:  func(bool) {},
					LockExpiredFn:     func() bool { return false },
					DnsFn:             func() string { return "NodeMock_defaultDNS" },
				}
				nodes["NodeMock_defaultDNS"] = &nodeMock
				return nodes
			},
		}
		return &mock
	}

	// act
	locker.Init(&gConfig, NewProxySQLNodeMock, proxySQLClusterFactory)

	res := locker.ClusterLock()

	// assert
	if res != nil {
		t.Errorf("ClusterLock should fail")
	}
}

*/
