package node_info

import "time"

type NodeInfo struct {
	LastTime         time.Time
	Host             string
	Port             int32
	FreeSpace        int64
	NumberOfRequests int64
}

// NewNodeInfo creates a NodeInfo object.
func NewNodeInfo(curTime time.Time, host string, port int32, freeSpace int64, numberOfRequest int64) *NodeInfo {
	nodeInfo := NodeInfo{LastTime: curTime, Host: host, Port: port, FreeSpace: freeSpace, NumberOfRequests: numberOfRequest}

	return &nodeInfo
}
