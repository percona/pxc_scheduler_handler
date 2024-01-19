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
	//"golang.org/x/text/message/pipeline"
	"net"
	global "pxc_scheduler_handler/internal/Global"
	"strconv"
	"time"
)

type lockerRule struct {
	name          string
	proxysqlNode1 ProxySQLNodeImpl
	proxysqlNode2 ProxySQLNodeImpl
	want          bool
}

type fileLockRule struct {
	name     string
	pidTest  int
	timeTest int64
	evaluate bool
	want     bool
}

// Objects declaration
type TestLockerImpl struct {
	MyServerIp             string
	MyServerPort           int
	MyServer               *ProxySQLNodeImpl
	myConfig               *global.Configuration
	FileLock               string
	FileLockPath           string
	FileLockInterval       int64
	FileLockReset          bool
	ClusterLockId          string
	ClusterLockInterval    int64
	ClusterLockReset       bool
	ClusterLastLockTime    int64
	ClusterCurrentLockTime int64
	IsClusterLocked        bool
	IsFileLocked           bool
	isLooped               bool
	LockFileTimeout        int64
	LockClusterTimeout     int64
}

func testLockerFactory() LockerImpl {
	locker := LockerImpl{
		MyServerIp:             "127.0.0.1",
		MyServerPort:           6032,
		MyServer:               new(ProxySQLNodeImpl),
		myConfig:               new(global.Configuration),
		FileLock:               "locktest.lock",
		FileLockPath:           "/tmp",
		FileLockInterval:       0,
		FileLockReset:          false,
		ClusterLockId:          "",
		ClusterLockInterval:    2000,
		ClusterLockReset:       false,
		ClusterLastLockTime:    0,
		ClusterCurrentLockTime: 0,
		IsClusterLocked:        false,
		IsFileLocked:           false,
		isLooped:               false,
		LockFileTimeout:        20,
		LockClusterTimeout:     60,
	}

	pxcCluster := testClusterFactory()
	locker.ClusterLockId = strconv.Itoa(10) +
		"_HG_" + strconv.Itoa(pxcCluster.HgWriterId) +
		"_W_HG_" + strconv.Itoa(pxcCluster.HgReaderId) +
		"_R"
	locker.FileLock = locker.ClusterLockId
	locker.MyServer.Dns = "127.0.0.1:6032"

	return locker
}

type TestFileLockImp struct {
	flPid          int
	flFullPath     string
	flTimeCreation int64
	flTimeout      int64
	flIsActive     bool
	flIsLooped     bool
}

func testFileLockFactory(active bool, looped bool) FileLockImp {
	/*
		1619616700432099000
		1619616760432224000
		60000125000
		to expire 70000125000
	*/

	flLock := FileLockImp{
		flPid:             10,
		flFullPath:        "/tmp/test",
		flAppCreationTime: 1619616760432224000,
		flIsActive:        active,
		flTimeout:         60,
		flIsLooped:        looped,
	}
	return flLock
}

func testProxySQLNodeFactory(ip string, port int, comment string) ProxySQLNodeImpl {
	node := ProxySQLNodeImpl{
		ActionNodeList:  make(map[string]DataNodeImpl),
		Dns:             "",
		Hostgoups:       make(map[int]Hostgroup),
		Ip:              ip,
		MonitorPassword: "password",
		MonitorUser:     "user",
		Password:        "password",
		Port:            6032,
		User:            "user",
		Connection:      nil,
		MySQLCluster:    nil,
		Variables:       make(map[string]string),
		IsInitialized:   false,
		Weight:          0,
		HoldLock:        false,
		IsLockExpired:   false,
		LastLockTime:    0,
		Comment:         comment,
	}
	node.Dns = net.JoinHostPort(ip, strconv.Itoa(port))
	return node

}

//*******

func rulesTestFindLock(locker LockerImpl) []lockerRule {
	/*
		1619616700432099000
		1619616760432224000
		60000125000
		to expire 70000125000
	*/
	now := time.Now().UnixNano()
	comment := "#LOCK_" + locker.ClusterLockId + "_" + strconv.FormatInt(now, 10) + "_LOCK#"
	expiredLock := "#LOCK_" + locker.ClusterLockId + "_" + strconv.FormatInt(now-70000125000, 10) + "_LOCK#"
	validLock := "#LOCK_" + locker.ClusterLockId + "_" + strconv.FormatInt(now-30000125000, 10) + "_LOCK#"

	myRules := []lockerRule{
		{"Locker base disable", testProxySQLNodeFactory("127.0.0.1", 6032, comment), testProxySQLNodeFactory("127.0.0.1", 6042, comment), true},
		{"Locker expire lock on other node", testProxySQLNodeFactory("127.0.0.1", 6032, comment), testProxySQLNodeFactory("127.0.0.1", 6042, expiredLock), true},
		{"Locker expire lock on other node no previous lock on node", testProxySQLNodeFactory("127.0.0.1", 6032, ""), testProxySQLNodeFactory("127.0.0.1", 6042, expiredLock), true},
		{"Locker lock is still good on other node", testProxySQLNodeFactory("127.0.0.1", 6032, comment), testProxySQLNodeFactory("127.0.0.1", 6042, validLock), false},
		{"Locker expire lock on my node", testProxySQLNodeFactory("127.0.0.1", 6032, expiredLock), testProxySQLNodeFactory("127.0.0.1", 6042, validLock), false},
	}
	return myRules
}

func rulesTestFileLock() []fileLockRule {
	/*
		1619616700432099000
		1619616740432099000 <-- no timeout
		1619616760434224000 <-- timeout
		60000125000
		to expire 70000125000
	*/

	myRules := []fileLockRule{
		{"File Locker not to process", 10, 1619616760432224000, false, false},
		{"File Locker same pid", 10, 1619616760432224000, true, false},
		{"File Locker pid exist and is running", 1, 1619616740432099000, true, false},
		{"File Locker pid exists is defunct NO Timeout ", 1, 1619616740432099000, true, false},
		{"File Locker pid exists is defunct WITH Timeout ", 1, 1619616600032099000, true, true},
		//{"File Locker pid does not exists", 0, 1619616761434224000, true, true},
	}
	return myRules
}
