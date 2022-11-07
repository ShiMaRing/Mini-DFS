package fileTree

import "sync"

type TreeNode struct {
	path        string
	fileName    string
	isDirectory bool
	childNodes  sync.Map
}

// NewTreeNode creates a new tree node.
func NewTreeNode(path string, fileName string, isDirectory bool) *TreeNode {
	treeNode := TreeNode{path: path, fileName: fileName, isDirectory: isDirectory, childNodes: sync.Map{}}
	return &treeNode
}

func (t *TreeNode) GetPath() string {
	return t.path
}

func (t *TreeNode) GetFileName() string {
	return t.fileName
}

func (t *TreeNode) IsDirectory() bool {
	return t.isDirectory
}

func (t *TreeNode) GetChildNodes() sync.Map {
	return t.childNodes
}

func (t *TreeNode) UpdateChildNodes(treeNode *TreeNode) {
	t.childNodes.Store(treeNode, true)
}
