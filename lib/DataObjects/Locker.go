package DataObjects

import (
	"bufio"
	"fmt"
	"regexp"
	"strings"

	Global "../Global"
	//"github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"time"
)

type (
	Locker struct {
		MyServerIp          string
		MyServerPort        int
		MyServer            *ProxySQLNode
		myConfig			*Global.Configuration
		FileLock            string
		FileLockPath        string
		FileLockInterval    int64
		FileLockReset       bool
		ClusterLockId       string
		ClusterLockInterval int64
		ClusterLockReset    bool
		ClusterLastLockTime	int64
		ClusterCurrentLockTime int64
		IsClusterLocked     bool
		IsFileLocked        bool
		isLooped		    bool
		LockFileTimeout int64
		LockClusterTimeout int64
	}
)

//Initialize the locker
//TODO initialize
func (locker *Locker) Init(config *Global.Configuration) bool{
	locker.myConfig = config
	locker.MyServerIp = config.Proxysql.Host
	locker.MyServerPort = config.Proxysql.Port
	var MyServer = new(ProxySQLNode)
	locker.MyServer = MyServer
	locker.MyServer.Ip = locker.MyServerIp
	locker.FileLockPath = config.Proxysql.LockFilePath
	locker.isLooped = config.Global.Development
	locker.LockFileTimeout = config.Global.LockFileTimeout
	locker.LockClusterTimeout = config.Global.LockClusterTimeout

	// Set lock file name based on the PXC cluster ID + HGs
	locker.ClusterLockId = strconv.Itoa(config.Pxcluster.ClusterId) +
		"_HG_" + strconv.Itoa(config.Pxcluster.HgW) +
		"_W_HG_" + strconv.Itoa(config.Pxcluster.HgR) +
		"_R"
	locker.FileLock = locker.ClusterLockId

	log.Info("Locker initialized")
	return true
}
//TODO fill the method
func (locker *Locker) CheckFileLock() *ProxySQLNode{

	log.Info("")

	return locker.MyServer
}
// we will check if the node were we are has a lock or if can acquire one.
// If not we will return nil to indicate program must exit given either there is already another node holding the lock
// or this node is not in a good state to acquire a lock
func (locker *Locker) CheckClusterLock( ) *ProxySQLNode{
	//TODO
	// 1 get connection
	// 2 get all we need from ProxySQL
	// 3 put the lock if we can
	proxySQLCluster := new(ProxySQLCluster)
	if !locker.MyServer.IsInitialized {
		if !locker.MyServer.Init(locker.myConfig){
			return nil
		}
	}
	if locker.MyServer.IsInitialized {
		proxySQLCluster.User = locker.myConfig.Proxysql.User
		proxySQLCluster.Password = locker.myConfig.Proxysql.Password
		//myMap := new(map[string]ProxySQLNode)
		//log.Info(myMap)
		proxySQLCluster.Nodes = make(map[string]ProxySQLNode)
		if proxySQLCluster.GetProxySQLnodes(locker.MyServer) && len(proxySQLCluster.Nodes) > 0{
			if nodes, ok := locker.findLock(proxySQLCluster.Nodes); ok && nodes != nil {
				if locker.PushSchedulerLock(nodes){
					return locker.MyServer
				}else{
					return nil
				}
			}else{
				log.Info(fmt.Sprintf("Cannot put a lock on the cluster for this scheduler %s another node hold the lock and acting",locker.MyServer.Dns))
				return nil
			}
		}
	}

	log.Info("")
	return locker.MyServer
}

func (locker *Locker) findLock(nodes map[string]ProxySQLNode)  (map[string]ProxySQLNode,bool){
	lockText := ""
	//isMyNodeLocking :=false
	//isLockTimeoutExceeded := false
	winningNode :=  ""
	lockHeader := "#LOCK_" + locker.ClusterLockId +"_"
	log.Debug("Using lock ID: ", lockHeader)
	lockHeaderLen := len(lockHeader)
	locker.ClusterCurrentLockTime = time.Now().UnixNano()
	log.Debug("Locker time: ", locker.ClusterCurrentLockTime)

	//let us process the nodes to identify if we have a lock, where and if is expired
	for _, node := range nodes {
		lockText = node.Comment


		// the node we are parsing hold a LOCK
		if strings.Contains(lockText, lockHeader) {
			node.HoldLock = true

			startIdx := strings.Index(node.Comment, lockHeader)
			endIdx := strings.Index(node.Comment, "_LOCK#")
			lockText := strings.ReplaceAll(node.Comment[startIdx + lockHeaderLen : endIdx], "#LOCK_", "")

			log.Debug(fmt.Sprintf("Cluster Node %s has a scheduler lock ", node.Dns))

			//get LOCK time and assign as winning node the node that has be the more recent one
			node.LastLockTime = int64(Global.ToInt(lockText))

			//Also if the time is not expired I will remove the lock text from comment for my node, given if the lock on another node is active I will not do a thing. But if is not I will have the node ready
			//trim some double spaces to be sure we have a clean string
			node.Comment = node.Comment[:startIdx] + node.Comment[endIdx+6:]
			space := regexp.MustCompile(`\s+`)
			node.Comment = space.ReplaceAllString(node.Comment, " ")

			//check if we had exceed the lock time
			//convert nanoseconds to seconds
			lockTime := (locker.ClusterCurrentLockTime - node.LastLockTime) / 1000000000
			if lockTime > locker.LockClusterTimeout {
				log.Debug(fmt.Sprintf("The lock on node %s has expired from %d seconds", node.Dns, lockTime))
				node.IsLockExpired = true
			}

			if node.LastLockTime < locker.ClusterLastLockTime && !node.IsLockExpired {
				locker.ClusterLastLockTime = node.LastLockTime
				winningNode = node.Dns
			} else if locker.ClusterLastLockTime == 0 && !node.IsLockExpired {
				locker.ClusterLastLockTime = node.LastLockTime
				winningNode = node.Dns
			}


			// if my node is the winner I will ad it back at the end of the comment
			//nodes[node.Dns] = node
		}
		nodes[node.Dns] = node
	}
	if winningNode == "" || winningNode == locker.MyServer.Dns{
		return  nodes, true
	}
	return nil,false

}

func (locker *Locker) PushSchedulerLock(nodes map[string]ProxySQLNode) bool {


	return true
}

func (locker *Locker)SetLockFile() bool {
	if locker.FileLock ==""{
		log.Error("Lock Filename is invalid (empty) ")
		return false
	}
	fullFile := locker.FileLockPath + string(os.PathSeparator) + locker.FileLock
	if _, err := os.Stat(fullFile); err == nil && !locker.isLooped{
		fmt.Printf("A lock file named: %s  already exists.\n If this is a refuse of a dirty execution remove it manually to have the check able to run\n", fullFile)
		return false
	} else {
		sampledata := []string{"PID:" + strconv.Itoa(os.Getpid()),
			"Time:" + strconv.FormatInt(time.Now().Unix(), 10),
		}

		file, err := os.OpenFile(fullFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

		if err != nil {
			fmt.Printf("failed creating lock file: %s", err)
		}

		datawriter := bufio.NewWriter(file)

		for _, data := range sampledata {
			_, _ = datawriter.WriteString(data + "\n")
		}

		datawriter.Flush()
		file.Close()
	}

	return true
}

func (locker *Locker)RemoveLockFile() bool {
	e := os.Remove("/tmp/" + locker.FileLock)
	if e != nil {
		log.Fatalf("Cannot remove lock file %s", e)
	}
	return true
}
