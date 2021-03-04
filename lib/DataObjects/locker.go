package dataobjects

import (
	"bufio"
	"context"
	"fmt"
	"regexp"
	"strings"

	global "../Global"
	SQL "../Sql/Proxy"

	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

type (
	// Locker is the Object handling the Lock at cluster level and the local process for the node demanded to run the scheduler
	// Locker initialize also the ProxySQL node that will execute the actions
	Locker struct {
		myServer               ProxySQLNode
		myConfig               *global.Configuration
		fileLock               string
		fileLockPath           string
		clusterLockID          string
		clusterLastLockTime    int64
		clusterCurrentLockTime int64
		isLooped               bool
		lockFileTimeout        int64
		lockClusterTimeout     int64

		proxySQLClusterFactory func(string, string) ProxySQLCluster
	}
)

// ProxySQLNodeFactory creates instance of ProxySQLNode object
type ProxySQLNodeFactory func(ip string) ProxySQLNode
type ProxySQLClusterFactory func(user string, password string) ProxySQLCluster

// Init initializes the locker object according to provided configuration
func (locker *Locker) Init(config *global.Configuration,
	proxySQLNodeFactory ProxySQLNodeFactory, proxySQLClusterFactory ProxySQLClusterFactory) bool {
	if config == nil {
		return false
	}

	locker.myConfig = config
	locker.myServer = proxySQLNodeFactory(config.ProxySQL.Host)
	locker.fileLockPath = config.ProxySQL.LockFilePath
	locker.isLooped = config.Global.Daemonize
	locker.lockFileTimeout = config.Global.LockFileTimeout
	locker.lockClusterTimeout = config.Global.LockClusterTimeout
	locker.proxySQLClusterFactory = proxySQLClusterFactory

	// Set lock file name based on the PXC cluster ID + HGs
	locker.clusterLockID = strconv.Itoa(config.PxcCluster.ClusterID) +
		"_HG_" + strconv.Itoa(config.PxcCluster.HgW) +
		"_W_HG_" + strconv.Itoa(config.PxcCluster.HgR) +
		"_R"
	locker.fileLock = locker.clusterLockID

	log.Info("Locker initialized")
	return true
}

// ClusterLock checks if the node were we are has a lock or if can acquire one.
// If not we will return nil to indicate program must exit given either there is already another node holding the lock
// or this node is not in a good state to acquire a lock
// Outside call To get and check the active list of ProxySQL server we call ProxySQLCLuster.GetProxySQLnodes
// All the DB operations are done connecting locally to the ProxySQL node running the scheduler
func (locker *Locker) ClusterLock() ProxySQLNode {
	// TODO
	// 1 get connection
	// 2 get all we need from ProxySQL
	// 3 put the lock if we can
	global.SetPerformanceObj("Cluster lock", true, log.InfoLevel)
	defer global.SetPerformanceObj("Cluster lock", false, log.InfoLevel)

	if !locker.myServer.IsInitialized() {
		if !locker.myServer.Init(locker.myConfig) {
			return nil
		}
	}

	if locker.myServer.IsInitialized() {
		// If this is single ProxySQL node setup, lock can always be granted
		// on this node
		if locker.myConfig.ProxySQL.Clustered {
			return locker.myServer
		}

		// This is mult ProxySQL setup. Check if lock can be granted
		proxySQLCluster := locker.proxySQLClusterFactory(locker.myConfig.ProxySQL.User,
			locker.myConfig.ProxySQL.Password)

		proxySQLCluster.FetchProxySQLNodes(locker.myServer)
		if len(proxySQLCluster.Nodes()) > 0 {
			if nodes, ok := locker.findLock(proxySQLCluster.Nodes()); ok && nodes != nil {
				if locker.PushSchedulerLock(nodes) {
					return locker.myServer
				}
				return nil
			}
			log.Info(fmt.Sprintf("Cannot put a lock on the cluster for this scheduler %s another node hold the lock and acting", locker.myServer.Dns()))
			return nil
		}
	}

	return nil
}

/*
Find lock method review all the nodes existing in the ProxySQL for an active Lock
it checks only nodes that are reachable.
Checks for:
	- existing lock locally
	- lock on another node
	- lock time comparing it with lockclustertimeout parameter
*/
func (locker *Locker) findLock(nodes map[string]ProxySQLNode) (map[string]ProxySQLNode, bool) {
	lockText := ""
	winningNode := ""
	lockHeader := "#LOCK_" + locker.clusterLockID + "_"
	lockTail := "_LOCK#"
	lockHeaderLen := len(lockHeader)
	log.Debug("Using lock ID: ", lockHeader)
	// Capture the current time
	locker.clusterCurrentLockTime = time.Now().UnixNano()
	log.Debug("Locker time: ", locker.clusterCurrentLockTime)

	// let us process the nodes to identify if we have a lock, where, and if is expired
	for _, node := range nodes {
		lockText = node.Comment()

		// the node we are parsing holds a LOCK
		if strings.Contains(lockText, lockHeader) {
			node.SetHoldLock(true)

			startIdx := strings.Index(node.Comment(), lockHeader)
			endIdx := strings.Index(node.Comment(), lockTail)
			lockText := node.Comment()[startIdx+lockHeaderLen : endIdx]

			log.Debug(fmt.Sprintf("Cluster Node %s has a scheduler lock ", node.Dns()))

			// get LOCK time and assign as winning node the node that has be the more recent one
			node.SetLastLockTime(int64(global.ToInt(lockText)))

			// Also if the time is not expired I will remove the lock text from comment for my node, given if the lock on another node is active I will not do a thing.
			// But if is not I will have the comment in the node ready
			// trim some double spaces to be sure we have a clean string
			node.SetComment(node.Comment()[:startIdx] + node.Comment()[endIdx+6:])
			space := regexp.MustCompile(`\s+`)
			node.SetComment(space.ReplaceAllString(node.Comment(), " "))

			// check if we had exceed the lock time
			// convert nanoseconds to seconds
			lockTime := (locker.clusterCurrentLockTime - node.LastLockTime()) / 1e9
			if lockTime > locker.lockClusterTimeout {
				log.Debug(fmt.Sprintf("The lock on node %s has expired from %d seconds", node.Dns(), lockTime))
				node.SetLockExpired(true)
			}

			// in case of multiple locks, the node with the most recent lock time wins
			if node.LastLockTime() < locker.clusterLastLockTime && !node.LockExpired() {
				locker.clusterLastLockTime = node.LastLockTime()
				winningNode = node.Dns()
			} else if locker.clusterLastLockTime == 0 && !node.LockExpired() {
				locker.clusterLastLockTime = node.LastLockTime()
				winningNode = node.Dns()
			}
		}
	}

	// My ProxySQL node is the winner and I can push a lock on it
	// Else I exit without doing a bit
	if winningNode == "" || winningNode == locker.myServer.Dns() {
		node := nodes[locker.myServer.Dns()]
		if node.Dns() != "" {
			node.SetComment(node.Comment() + " " + lockHeader + strconv.FormatInt(locker.clusterCurrentLockTime, 10) + lockTail)
			nodes[locker.myServer.Dns()] = node
			log.Debug(fmt.Sprintf("Lock acquired by node %s Current time %d", locker.myServer.Dns(), locker.clusterCurrentLockTime))
		}

		return nodes, true
	}
	return nil, false
}

// PushSchedulerLock submits changes. As always all is executed in a transaction
// TODO SHOULD we remove the proxysql node that doesn't work ????
func (locker *Locker) PushSchedulerLock(nodes map[string]ProxySQLNode) bool {
	if len(nodes) <= 0 {
		return true
	}

	if global.Performance {
		key := "Execute SQL changes - ProxySQL cluster LOCK (" + locker.clusterLockID + ")"
		global.SetPerformanceObj(key, true, log.DebugLevel)
		defer global.SetPerformanceObj(key, false, log.DebugLevel)
	}
	// We will execute all the commands inside a transaction if any error we will roll back all
	ctx := context.Background()
	tx, err := locker.myServer.Connection().BeginTx(ctx, nil)
	if err != nil {
		log.Fatal("Error in creating transaction to push changes ", err)
	}
	for key, node := range nodes {
		if node.Dns() != "" {
			sqlAction := strings.ReplaceAll(SQL.Dml_update_comment_proxy_servers, "?", node.Comment()) + " where hostname='" + node.Ip() + "' and port= " + strconv.Itoa(node.Port())
			_, err = tx.ExecContext(ctx, sqlAction)
			if err != nil {
				tx.Rollback()
				log.Fatal("Error executing SQL: ", sqlAction, " for node: ", key, " Rollback and exit")
				log.Error(err)
				return false
			}
		}
	}
	err = tx.Commit()
	if err != nil {
		log.Fatal("Error IN COMMIT exit")
		return false

	} else {
		_, err = locker.myServer.Connection().Exec("LOAD proxysql servers to RUN ")
		if err != nil {
			log.Fatal("Cannot load new proxysql configuration to RUN ")
			return false
		} else {
			_, err = locker.myServer.Connection().Exec("save proxysql servers to disk ")
			if err != nil {
				log.Fatal("Cannot save new proxysql configuration to DISK ")
				return false
			}
		}
	}

	return true
}

// SetLockFile sets lock for local execution
func (locker *Locker) SetLockFile() bool {
	if locker.fileLock == "" {
		log.Error("Lock Filename is invalid (empty) ")
		return false
	}

	fullFile := locker.fileLockPath + string(os.PathSeparator) + locker.fileLock
	if _, err := os.Stat(fullFile); err == nil && !locker.isLooped {
		fmt.Printf("A lock file named: %s  already exists.\n If this is a refuse of a dirty execution remove it manually to have the check able to run\n", fullFile)
		return false
	}

	sampledata := []string{"PID:" + strconv.Itoa(os.Getpid()),
		"Time:" + strconv.FormatInt(time.Now().Unix(), 10),
	}

	file, err := os.OpenFile(fullFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Error(fmt.Sprintf("failed creating lock file: %s", err.Error()))
		return false
	}

	datawriter := bufio.NewWriter(file)

	for _, data := range sampledata {
		_, _ = datawriter.WriteString(data + "\n")
	}

	datawriter.Flush()
	file.Close()

	return true
}

// RemoveLockFile removes local execution lock
func (locker *Locker) RemoveLockFile() bool {
	e := os.Remove(locker.fileLockPath + string(os.PathSeparator) + locker.fileLock)
	if e != nil {
		log.Fatalf("Cannot remove lock file %s", e)
	}
	return true
}
