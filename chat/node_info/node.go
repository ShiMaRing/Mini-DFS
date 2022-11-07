package node_info

// Node information
type Node struct {
	Id   int32
	Host string
	Port int32
}

// NewNode Creates a new node.
func NewNode(id int32, host string, port int32) *Node {
	node := Node{Id: id, Host: host, Port: port}
	return &node
}
