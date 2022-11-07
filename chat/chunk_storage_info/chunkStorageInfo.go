package chunk_storage_info

import (
	"fmt"
	"mp/chat/node_info"
	"sync"
)

type ChunkStorageInfo struct {
	ChunkName string
	NodeList  []node_info.NodeInfo // a list of node
	mu        sync.Mutex
}

func newChunkStorageInfo(name string) *ChunkStorageInfo {
	chunk := ChunkStorageInfo{ChunkName: name}
	return &chunk
}

// AddStoreNode Add a new storage node to the node list.
func (c *ChunkStorageInfo) AddStoreNode(node *node_info.NodeInfo) {
	c.mu.Lock()
	c.NodeList = append(c.NodeList, *node)
	c.mu.Unlock()
}

// GetName returns chunk name.
func (c *ChunkStorageInfo) GetName() string {
	return c.ChunkName
}

// Print prints node list.
func (c *ChunkStorageInfo) Print() {
	for _, node := range c.NodeList {
		fmt.Printf("host: %s, port: %d\n", node.Host, node.Port)
	}
}
