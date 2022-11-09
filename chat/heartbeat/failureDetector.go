package heartbeat

import (
	"github.com/robfig/cron/v3"
	"log"
	"mp/chat/node_info"
	"sync"
	"time"
)

type FailureDetector struct {
	HeartbeatMap sync.Map
	TimeoutMills int64
	UserMap      *sync.Map
}

func newFailureDetector(timeoutMills int64) *FailureDetector {
	failureDetector := FailureDetector{TimeoutMills: timeoutMills}
	return &failureDetector
}

func (failureDetector *FailureDetector) ScheduledCheck() {
	crontab := cron.New(cron.WithSeconds())
	task := func() {
		//fmt.Println("Start scheduled detector !")
		failureDetector.heartbeatCheck()
	}
	spec := "*/5 * * * * ?"
	crontab.AddFunc(spec, task)
	crontab.Start()
	select {}
}

func (failureDetector *FailureDetector) HeartbeatReceived(id int32, host string, port int32, freeSpace int64, numberOfRequests int64) {
	currentTime := time.Now()
	nodeInfo := node_info.NodeInfo{LastTime: currentTime, Host: host, Port: port, FreeSpace: freeSpace, NumberOfRequests: numberOfRequests}
	failureDetector.markUp(id, nodeInfo)
}

func (failureDetector *FailureDetector) heartbeatCheck() {
	currentTime := time.Now()
	failureDetector.HeartbeatMap.Range(func(k, v interface{}) bool {
		nodeId := k.(int32)
		curNodeInfo := v.(node_info.NodeInfo)
		lastTime := curNodeInfo.LastTime
		timeInterval := currentTime.Sub(lastTime).Milliseconds()
		if timeInterval >= failureDetector.TimeoutMills {
			failureDetector.markDown(nodeId)
		}
		return true
	})
}

func (failureDetector *FailureDetector) markDown(nodeId int32) {
	log.Println("&&&&&&&&&&& find the failure node !!!!!:", failureDetector.HeartbeatMap)
	failureDetector.HeartbeatMap.Delete(nodeId)
	failureDetector.UserMap.Delete(nodeId)
}

func (failureDetector *FailureDetector) markUp(nodeId int32, nodeInfo node_info.NodeInfo) {
	//log.Println("***** mark up :", nodeId)
	failureDetector.HeartbeatMap.Store(nodeId, nodeInfo)
	//log.Println("!!!!!!!!! after mark up:")
	failureDetector.UserMap.Store(nodeId, &node_info.Node{Id: nodeId, Host: nodeInfo.Host, Port: nodeInfo.Port})
	i := 0
	failureDetector.HeartbeatMap.Range(func(key, value interface{}) bool {
		i++
		return true
	})
	//log.Println("!!!!!!!!! map size: ", i)
}
