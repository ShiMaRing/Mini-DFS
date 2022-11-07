package fileTree

import (
	"fmt"
	"log"
	"strings"
	"sync"
)

type FileTree struct {
	Root     *TreeNode
	RootPath string
	mutex    *sync.Mutex
}

func NewFileTree(rootPath string) *FileTree {
	fileTree := FileTree{RootPath: rootPath, mutex: &sync.Mutex{}}
	temp := NewTreeNode(rootPath, "", true)
	fileTree.Root = temp
	return &fileTree
}

// AppendFile appends a file node in the tree.
func (fileTree *FileTree) AppendFile(filePath string, fileName string) {
	fileTree.mutex.Lock()
	log.Printf("path: %s, name: %s\n", filePath, fileName)
	leftPaths := fileTree.removeRootPath(filePath)
	fileTree.appendFileHelper(fileTree.Root, 0, leftPaths, fileName)
	fileTree.mutex.Unlock()
}

// Remove root path.
func (fileTree *FileTree) removeRootPath(filePath string) []string {
	curPath := strings.Split(filePath, fileTree.RootPath)
	if len(curPath) == 1 {
		log.Fatalln("Input path is not correct")
	}
	curLeftPath := curPath[1]
	leftPaths := strings.Split(curLeftPath, "/")
	return leftPaths
}

// Helper function for appending a file into the tree.
func (fileTree *FileTree) appendFileHelper(parentNode *TreeNode, index int, leftPaths []string, fileName string) {
	if index == len(leftPaths) || leftPaths[index] == "" {
		treeNode := NewTreeNode(parentNode.GetPath(), fileName, false)
		parentNode.UpdateChildNodes(treeNode)
		return
	} else {
		curNode := NewTreeNode(parentNode.GetPath(), leftPaths[index], true)
		parentNode.UpdateChildNodes(curNode)
		fileTree.appendFileHelper(curNode, index+1, leftPaths, fileName)
	}
}

// DeleteFile deletes a file in the tree.
func (fileTree *FileTree) DeleteFile(filePath string, fileName string) {
	fileTree.mutex.Lock()
	leftPaths := fileTree.removeRootPath(filePath)
	var curList []*TreeNode
	parentList := curList
	fileTree.deleteFileHelper(nil, fileTree.Root, 0, leftPaths, fileName, parentList)
	deleteRedundantDir(parentList)
	fileTree.mutex.Unlock()
}

// Delete redundant directory.
func deleteRedundantDir(curList []*TreeNode) {
	curLen := len(curList)
	for i := curLen - 1; i >= 0; i-- {
		curNode := curList[i]
		count := 0

		nodes := curNode.GetChildNodes()
		nodes.Range(func(key, value interface{}) bool {
			node, _ := key.(*TreeNode)
			if node != nil {
				count += 1
			}
			return true
		})
		if count == 0 && i >= 1 {
			curParent := curList[i-1]
			childNodes := curParent.GetChildNodes()
			childNodes.Delete(curNode)
		}
	}
}

// Delete a file in the tree.
func (fileTree *FileTree) deleteFileHelper(parentNode *TreeNode, curNode *TreeNode, index int,
	leftPaths []string, fileName string, parentList []*TreeNode) {
	if index == len(leftPaths) || leftPaths[index] == "" {
		if parentNode == nil {
			parentNode = curNode
		}
		nodes := curNode.GetChildNodes()
		nodes.Range(func(key, value interface{}) bool {
			childNode, _ := key.(*TreeNode)
			if childNode.GetFileName() == fileName {
				nodes.Delete(childNode)
				log.Println("successfully delete file: ", fileName)
				return true
			}
			return true
		})
		log.Println("file not exist")
		return
	} else {
		curDirName := leftPaths[index]
		if parentNode == nil {
			parentNode = curNode
		}
		nodes := curNode.GetChildNodes()
		nodes.Range(func(key, value interface{}) bool {
			childNode, _ := key.(*TreeNode)
			if childNode.GetFileName() == curDirName {
				parentList = append(parentList, parentNode)
				fileTree.deleteFileHelper(curNode, childNode, index+1, leftPaths, fileName, parentList)
				return true
			}
			return true
		})
	}
}

// FindNode file the node with the given directory name.
func (fileTree *FileTree) FindNode(root *TreeNode, dirName string) *TreeNode {
	if root.GetFileName() == dirName {
		return root
	} else {
		var childNode *TreeNode
		nodes := root.GetChildNodes()
		nodes.Range(func(key, value interface{}) bool {
			node, _ := key.(*TreeNode)
			if node.GetFileName() == dirName || childNode.IsDirectory() {
				childNode = node
				return true
			} else {
				childNode = fileTree.FindNode(node, dirName)
				return true
			}
		})
	}
	return nil
}

// Print returns string representation of the tree.
func (fileTree *FileTree) Print(root *TreeNode, space string) string {
	var s strings.Builder
	if !root.IsDirectory() {
		s.WriteString(space + "*" + root.GetFileName() + "\n")
	} else {
		nodes := root.GetChildNodes()
		nodes.Range(func(key, value interface{}) bool {
			node, _ := key.(*TreeNode)
			if node != nil {
				s.WriteString(space + root.GetFileName())
				s.WriteString(fileTree.Print(node, space+"-"))
			}
			return true
		})
	}
	return s.String()
}

// Print the tree.
func (fileTree *FileTree) printTree(root *TreeNode, space string) {
	if !root.IsDirectory() {
		fmt.Println(space + "*" + root.GetFileName())
	} else {
		nodes := root.GetChildNodes()
		nodes.Range(func(key, value interface{}) bool {
			node, _ := key.(*TreeNode)
			if node != nil {
				fmt.Print(space + root.GetFileName())
				fileTree.printTree(node, space+"-")
			}
			return true
		})
	}
}
