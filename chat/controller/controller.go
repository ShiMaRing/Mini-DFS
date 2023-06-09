package main

import (
	"bufio"
	fileTree2 "mp/chat/controller/fileTree"
	"mp/chat/controller/writeToFile"
	"mp/chat/heartbeat"
	"mp/chat/messages"
	"mp/chat/node_info"
	"sync/atomic"

	"encoding/json"
	"fmt"
	"github.com/emirpasic/gods/lists/arraylist"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type Controller struct {
	userMap         sync.Map // storage node map
	fileStorageMap  sync.Map // map of map, key is file name, map{key: chunkIdx, lst of node}
	fileStatusMap   sync.Map
	failureDetector heartbeat.FailureDetector
	fileTree        *fileTree2.FileTree
	writer          *writeToFile.JsonWriter

	jobId   uint32
	taskMap map[uint32]*Task //jobId
}

type Task struct {
	isFinish bool       //true when finish
	jobId    uint32     //auto increment
	mu       sync.Mutex //avoid race
	fileName string     //filename
	dirName  string     //dirname

	result      []*ReduceResult
	mapNodes    map[string]*messages.StoreNode //key: nodeId-chunkIdx
	reduceNodes []*messages.StoreNode          //all reduce node
	clientId    int32
}

// ReduceResult reply to user to info reduce result place
type ReduceResult struct {
	dirName  string
	fileName string
	jobId    uint32
	node     *messages.StoreNode
}

const path = "./bigdata/lliu78/storage"
const storagePath = "./bigdata/lliu78/storage/storage.json"
const address = "localhost:22000"

func NewController() *Controller {
	c := Controller{fileTree: fileTree2.NewFileTree(path + "/"), taskMap: make(map[uint32]*Task)}
	if checkFileExist() {
		c.readFile()
	} else {
		c.writer = writeToFile.NewJsonWriter(storagePath)
	}
	return &c
}

// Check if the storage file exist or not.
func checkFileExist() bool {
	_, error := os.Stat(storagePath)
	// check if error is "file not exists"
	if !os.IsNotExist(error) {
		// file exist
		log.Println("file exists")
		return true
	} else {
		return false
	}
}

// Read from the file and store info to the map.
func (c *Controller) readFile() {
	readFile, err := os.Open(storagePath)
	if err != nil {
		log.Println(err)
	}
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	var fileStorage writeToFile.FileStorage
	for fileScanner.Scan() {
		text := fileScanner.Text()
		err := json.Unmarshal([]byte(text), &fileStorage)
		if err != nil {
			log.Println(err)
		}
		fileName := fileStorage.FileName
		filePath := fileStorage.FilePath
		chunkId := fileStorage.ChunkId
		nodeList := fileStorage.NodeList
		wholeName := filePath + "/" + fileName
		_, ok := c.fileStorageMap.Load(wholeName)
		if !ok {
			c.fileStorageMap.Store(wholeName, &sync.Map{})
			// update file tree
			c.fileTree.AppendFile(path+"/"+filePath, fileName)
			// update map
			c.fileStatusMap.Store(wholeName, true)
		}
		lst := &messages.ChunkNodeInfo{}
		f, _ := c.fileStorageMap.Load(wholeName)
		fm, _ := f.(*sync.Map)
		for i := 0; i < len(nodeList); i++ {
			node := nodeList[i]
			lst.NodeList = append(lst.NodeList, &messages.StoreNode{Id: node.Id, Ip: node.Host, Port: node.Port})
		}
		fm.Store(chunkId, lst)
		log.Printf("read from file chunkId[%d], %s\n", chunkId, lst)
	}
	log.Println("finish reading index file, ready to listen for incoming requests")
	readFile.Close()
}

// A helper function random choose three alive nodes.
func (c *Controller) getThreeNodes() *messages.ChunkNodeInfo {
	candidates := arraylist.New()
	c.userMap.Range(func(key, value interface{}) bool {
		node := key.(int32)
		candidates.Add(node)
		return true
	})
	lst := &messages.ChunkNodeInfo{}
	lmt := candidates.Size()
	p := rand.Perm(lmt)
	for _, r := range p[:3] {
		nodeIdx, _ := candidates.Get(r)
		i := nodeIdx.(int32)
		s, _ := c.userMap.Load(i)
		sn := s.(*node_info.Node)
		log.Println(sn)
		lst.NodeList = append(lst.NodeList, &messages.StoreNode{Id: sn.Id, Ip: sn.Host, Port: sn.Port})
	}
	return lst
}

// Start listening.
func (c *Controller) Start() {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	c.failureDetector = heartbeat.FailureDetector{TimeoutMills: 3000, HeartbeatMap: sync.Map{}, UserMap: &c.userMap}
	go c.failureDetector.ScheduledCheck()
	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			// concurrent, goroutines
			go c.handleClient(msgHandler)
		}
	}
}

// Mark the file status as completed.
func (c *Controller) markFileCompleted(fileName string) {
	c.fileStatusMap.Store(fileName, true)
}

// Return the file status.
func (c *Controller) checkFileStatus(fileName string) bool {
	completed, ok := c.fileStatusMap.Load(fileName)
	if ok {
		status := completed.(bool)
		return status
	}
	return false
}

func (c *Controller) handleGetMsg(id int32, fileName string, dirName string, msgHandler *messages.MessageHandler) {

	// generate a response msg, get list of node of the given file name
	if !c.checkFileStatus(dirName + "/" + fileName) {
		// send file not exist msg
		resp := &messages.Wrapper{
			Msg: &messages.Wrapper_FileNotExistMessage{
				FileNotExistMessage: &messages.FileNotExist{FileName: dirName + "/" + fileName, ErrorMsg: "file not exist"}},
		}
		msgHandler.Send(resp)
		return
	}
	getResp := c.getFileMap(dirName, fileName)
	resp := &messages.Wrapper{
		Msg: &messages.Wrapper_GetResponseMessage{
			GetResponseMessage: getResp,
		},
	}
	msgHandler.Send(resp)
	log.Println("response msg", resp)
	log.Printf("send a list of node to client %d of file %s:\n", id, fileName)
}

// Handle delete request.
func (c *Controller) handleDeleteMsg(fileName string, filePath string, msgHandler *messages.MessageHandler) {
	wholeName := filePath + "/" + fileName
	chunkMap, ok := c.fileStorageMap.Load(wholeName)
	if !ok {
		// file not exist
		resp := &messages.Wrapper{
			Msg: &messages.Wrapper_FileNotExistMessage{
				FileNotExistMessage: &messages.FileNotExist{FileName: fileName, ErrorMsg: "file not exist"}},
		}
		msgHandler.Send(resp)
		return
	}

	m := make(map[int32]*arraylist.List) // key is nodeId, value is a list of chunkId
	// Get chunk list
	cm := chunkMap.(*sync.Map)
	cm.Range(func(key, value interface{}) bool { // key is chunkId, value is ChunkNodeInfo
		chunkId := key.(int32)
		v := value.(*messages.ChunkNodeInfo)
		for i := 0; i < len(v.GetNodeList()); i++ {
			node := v.GetNodeList()[i]
			if val, exist := m[node.GetId()]; exist {
				val.Add(chunkId)
			} else {
				list := arraylist.New()
				list.Add(chunkId)
				m[node.GetId()] = list
			}
		}
		return true
	})
	for nodeId, chunks := range m {
		msg := &messages.DeleteReplicas{FileName: fileName, DirName: filePath}
		for j := 0; j < chunks.Size(); j++ {
			chunkId, _ := chunks.Get(j)
			id := chunkId.(int32)
			msg.ChunkToDeleteList = append(msg.ChunkToDeleteList, id)
		}
		resp := &messages.Wrapper{
			Msg: &messages.Wrapper_DeleteReplicasMessage{
				DeleteReplicasMessage: msg,
			},
		}
		log.Printf("sends deletion msg to node {%d}\n", nodeId)
		// connect to storage Node
		node, _ := c.userMap.Load(nodeId)
		n := node.(*node_info.Node)
		conn, err := net.Dial("tcp", n.Host+":"+strconv.Itoa(int(n.Port)))
		if err != nil {
			log.Fatalln(err.Error())
			return
		}
		newMsgHandler := messages.NewMessageHandler(conn)
		newMsgHandler.Send(resp)
		msgHandler.Close()
	}
	// delete from storage map
	c.fileStorageMap.Delete(wholeName)
	c.fileStatusMap.Delete(wholeName)
	c.fileTree.DeleteFile(path+filePath, fileName)
	c.writer.Delete(fileName, filePath)
}

// Handle getMap requests.
func (c *Controller) getFileMap(dirName string, fileName string) *messages.GetResponse {
	msg := messages.NodeMap{}
	msg.NodeMap = make(map[int32]*messages.ChunkNodeInfo)
	chunkMap, ok := c.fileStorageMap.Load(dirName + "/" + fileName)
	if ok {
		cm := chunkMap.(*sync.Map)
		cm.Range(func(key, value interface{}) bool {
			chunkId := key.(int32)
			v := value.(*messages.ChunkNodeInfo)
			msg.NodeMap[chunkId] = v
			return true
		})
	}
	getResp := messages.GetResponse{DirName: dirName}
	getResp.GetResponse = make(map[string]*messages.NodeMap)
	getResp.GetResponse[fileName] = &msg
	return &getResp
}

// A helper function to write file tree to the file.
func (c *Controller) writeToFile(filePath string, fileName string, chunkId int32, nodeList *messages.ChunkNodeInfo) {
	fs := writeToFile.NewFileStorage(filePath, fileName, chunkId, nodeList)
	c.writer.Write(fs)
}

// Handle Ls requests.
func (c *Controller) handleLsRequest(dirName string, msgHandler *messages.MessageHandler) {
	if len(dirName) == 0 {
		// root
		s := c.fileTree.Print(c.fileTree.Root, "")
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_LsResponseMessage{
				LsResponseMessage: &messages.LsResponse{
					Response: s,
				},
			},
		}
		msgHandler.Send(wrapper)
	} else {
		treeNode := c.fileTree.FindNode(c.fileTree.Root, dirName)
		s := c.fileTree.Print(treeNode, "")
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_LsResponseMessage{
				LsResponseMessage: &messages.LsResponse{
					Response: s,
				},
			},
		}
		msgHandler.Send(wrapper)
	}
}

func (c *Controller) sendMapTask(dirName string, fileName string, job string, jobContent []byte,
	mapNodes map[*messages.StoreNode][]int32, jobId uint32, reduceNodes []*messages.StoreNode) {
	for node := range mapNodes {
		chunkNums := mapNodes[node]
		for i := range chunkNums {
			chunkNum := chunkNums[i]
			conn, err := net.Dial("tcp", node.Ip+":"+strconv.Itoa(int(node.Port)))
			if err != nil {
				log.Fatalln(err.Error())
				return
			}
			newMsgHandler := messages.NewMessageHandler(conn)
			wrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_SendMapTask{
					SendMapTask: &messages.SendMapTask{
						FileName:   fileName,
						DirName:    dirName,
						ChunkIdx:   chunkNum,
						JobContent: jobContent,
						JobName:    job,
						JobId:      jobId,
						NodeList:   reduceNodes, //in same order
					},
				},
			}
			newMsgHandler.Send(wrapper)
			newMsgHandler.Close()
		}
	}
}

// Handle incoming requests.
func (c *Controller) handleClient(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()
	for {
		wrapper, _ := msgHandler.Receive()
		//fmt.Println(wrapper)
		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_HeartbeatMessage:
			id := msg.HeartbeatMessage.GetId()
			host := msg.HeartbeatMessage.GetHost()
			port := msg.HeartbeatMessage.GetPort()
			freeSpace := msg.HeartbeatMessage.GetFreeSpace()
			numberOfRequests := msg.HeartbeatMessage.GetNumberOfRequests()
			c.failureDetector.HeartbeatReceived(id, host, port, freeSpace, numberOfRequests)
		case *messages.Wrapper_GetMapRequestMessage:
			fmt.Println(wrapper)
			//convert heartbeat map to map that can be sent
			convertedMap := c.convertMap()
			mapMsg := messages.SendMapResponse{DataMap: convertedMap}
			mapWrapper := &messages.Wrapper{
				Msg: &messages.Wrapper_SendMapResponseMessage{
					SendMapResponseMessage: &mapMsg,
				},
			}
			err := msgHandler.Send(mapWrapper)
			if err != nil {
				log.Fatalln("Something went wrong")
			}
		case *messages.Wrapper_PutRequest:
			fmt.Println(wrapper)
			log.Println(msg.PutRequest)
			fileName := msg.PutRequest.GetFileName()
			dirName := msg.PutRequest.GetDirName()
			wholeName := dirName + "/" + fileName
			numOfChunks := msg.PutRequest.GetNumOfChunk()
			_, exist := c.fileStorageMap.Load(wholeName)
			if exist {
				log.Println("file already exists!")
				resp := &messages.Wrapper{
					Msg: &messages.Wrapper_FileExistMessage{FileExistMessage: &messages.FileExist{
						Filename: fileName,
					}},
				}
				msgHandler.Send(resp)
				break
			}
			c.fileTree.AppendFile(path+"/"+dirName, fileName)
			c.fileStatusMap.Store(wholeName, true)
			// update storage map
			c.fileStorageMap.Store(wholeName, &sync.Map{})
			f, _ := c.fileStorageMap.Load(wholeName)
			fm, _ := f.(*sync.Map)
			putMessage := messages.PutResponse{DirName: dirName}
			putMessage.ChunkMap = make(map[string]*messages.ChunkNodeInfo)
			i := int32(0)
			for i < numOfChunks {
				// random pick 3 node
				lst := c.getThreeNodes()
				log.Printf("lst size is: %d\n", len(lst.NodeList))
				fm.Store(i, lst)
				chunkName := fmt.Sprintf("%s%d", fileName+"/chunk", i)
				log.Println("chunk name : " + chunkName)
				putMessage.ChunkMap[chunkName] = lst
				// write to file
				go c.writeToFile(dirName, fileName, i, lst)
				i++
			}
			resp := &messages.Wrapper{
				Msg: &messages.Wrapper_PutResponse{PutResponse: &putMessage},
			}
			log.Println("send put msg: ", resp)
			err := msgHandler.Send(resp)
			if err != nil {
				log.Fatalln("Something went wrong")
			}
		case *messages.Wrapper_SubmitJobMessage:
			dirName := msg.SubmitJobMessage.GetDirName()
			fileName := msg.SubmitJobMessage.GetFileName()
			log.Printf("get a job , name :%s  filename:%s \n", msg.SubmitJobMessage.GetJobName(), fileName)
			wholeName := dirName + "/" + fileName
			_, exist := c.fileStorageMap.Load(wholeName)
			//if file does not exist
			if !exist {
				log.Println("file does not exists!")
				resp := &messages.Wrapper{
					Msg: &messages.Wrapper_FileNotExistMessage{
						FileNotExistMessage: &messages.FileNotExist{
							FileName: fileName,
							ErrorMsg: "file does not exits",
						},
					},
				}
				msgHandler.Send(resp)
				break
			}
			//get file info
			f, _ := c.fileStorageMap.Load(wholeName)
			fm, _ := f.(*sync.Map)
			//fm store every chunk address ,pick different nodes
			mapNodeSet := make(map[*messages.StoreNode][]int32)
			fm.Range(func(key, value any) bool {
				chunkIdx := key.(int32)
				info := value.(*messages.ChunkNodeInfo)
				i := rand.Intn(len(info.NodeList)) //rand choose node to process map func
				mapNodeSet[info.NodeList[i]] = append(mapNodeSet[info.NodeList[i]], chunkIdx)
				log.Printf("node %d map chunk %d", info.NodeList[i].Id, chunkIdx)
				return true
			})
			//pick some reduce
			//preferentially generated in the map node
			//load balance
			reduceNum := int(msg.SubmitJobMessage.GetReduceNum())
			reduceNodeSet := make(map[*messages.StoreNode]struct{})
			if reduceNum <= len(mapNodeSet) {
				var i = 0
				for node := range mapNodeSet {
					if i == reduceNum {
						break
					}
					reduceNodeSet[node] = struct{}{}
					i++
				}
			} else {
				for node := range mapNodeSet {
					reduceNodeSet[node] = struct{}{}
				}
				//find rest userMap
				var rest = reduceNum - len(mapNodeSet)
				c.userMap.Range(func(key, value any) bool {
					if rest == 0 {
						return false
					}
					sn := value.(*node_info.Node)
					for node := range reduceNodeSet {
						if node.Id == sn.Id {
							return true
						}
					}
					node := &messages.StoreNode{Id: sn.Id, Ip: sn.Host, Port: sn.Port}
					//add to reduce count
					reduceNodeSet[node] = struct{}{}
					rest--
					return true
				})
			}
			for node := range reduceNodeSet {
				log.Printf("node %d is chosed to reduce ", node.Id)
			}
			jobContent := msg.SubmitJobMessage.GetJobContent() //generate job name and store the node info
			jobName := msg.SubmitJobMessage.GetJobName()
			clientId := msg.SubmitJobMessage.GetClientId() //client can get result via id and jobName
			atomic.AddUint32(&c.jobId, 1)
			task := &Task{jobId: c.jobId}
			task.fileName = fileName
			task.dirName = dirName
			task.mapNodes = make(map[string]*messages.StoreNode)
			task.clientId = clientId
			for node, chunkIdx := range mapNodeSet {
				for i := range chunkIdx {
					key := strconv.Itoa(int(node.Id)) + "-" + strconv.Itoa(int(chunkIdx[i]))
					task.mapNodes[key] = node
				}
			}
			for node := range reduceNodeSet {
				task.reduceNodes = append(task.reduceNodes, node)
			}
			c.taskMap[c.jobId] = task //add task
			//send message to all map node
			go c.sendJobInfoToClient(msgHandler)
			go c.sendReduceTask(jobName, jobContent, c.jobId, task.reduceNodes, fileName, dirName)
			go c.sendMapTask(dirName, fileName, jobName, jobContent, mapNodeSet, c.jobId, task.reduceNodes)

		case *messages.Wrapper_GetRequestMessage:
			fmt.Println(wrapper)
			// handleGetRequest
			getRequest := msg.GetRequestMessage
			id := getRequest.GetId()
			fileName := getRequest.GetFileName()
			dirName := getRequest.GetDirName()
			c.handleGetMsg(id, fileName, dirName, msgHandler)
		case *messages.Wrapper_AckReplicaChunkMessage:
			fmt.Println(wrapper)
			ackMsg := msg.AckReplicaChunkMessage
			replicaSuccess := ackMsg.GetReplicaSuccess()
			fileName := ackMsg.GetFileName()
			// If chunk is lost, mark file is lost
			if !replicaSuccess {
				c.fileStatusMap.Store(fileName, false)
			}
		case *messages.Wrapper_DeleteRequestMessage:
			fmt.Println(wrapper)
			log.Println("deletion: ", msg.DeleteRequestMessage)
			deleteMsg := msg.DeleteRequestMessage
			fileName := deleteMsg.GetFileName()
			filePath := deleteMsg.GetFilePath()
			c.handleDeleteMsg(fileName, filePath, msgHandler)
		case *messages.Wrapper_LsRequestMessage:
			fmt.Println(wrapper)
			dirName := msg.LsRequestMessage.GetDirName()
			c.handleLsRequest(dirName, msgHandler)
		case *messages.Wrapper_JobInfo:
			//client want to look for job status
			id := msg.JobInfo.GetJobId()
			c.handleJobInfoRequest(msgHandler, id)
		case *messages.Wrapper_NotifyMapFinish:
			//notify finish map
			jobId := msg.NotifyMapFinish.GetJobId()
			chunkId := msg.NotifyMapFinish.GetChunkId()
			nodeId := msg.NotifyMapFinish.GetNodeId()
			//search map for jobId
			task := c.taskMap[jobId]
			task.mu.Lock()
			key := strconv.Itoa(int(nodeId)) + "-" + strconv.Itoa(int(chunkId))
			delete(task.mapNodes, key)
			task.mu.Unlock()
			log.Printf("node %d finish map\n", nodeId)
			if len(task.mapNodes) == 0 {
				//provide jobId and notify reduce to start job
				startMsg := &messages.StartReduce{JobId: jobId}
				for i := range task.reduceNodes {
					c.startReduce(startMsg, task.reduceNodes[i])
				}
			}
		case *messages.Wrapper_NotifyReduceFinish:
			jobId := msg.NotifyReduceFinish.GetJobId()
			dirName := msg.NotifyReduceFinish.GetDirName()
			fileName := msg.NotifyReduceFinish.GetFileName()
			nodeId := msg.NotifyReduceFinish.GetNodeId()
			//search map for jobId
			task := c.taskMap[jobId]
			task.mu.Lock()
			//findNode
			var node *messages.StoreNode
			for i := range task.reduceNodes {
				if task.reduceNodes[i].Id == nodeId {
					node = task.reduceNodes[i]
					break
				}
			}
			task.result = append(task.result, &ReduceResult{
				dirName:  dirName,
				fileName: fileName,
				jobId:    jobId,
				node:     node,
			})
			if len(task.result) == len(task.reduceNodes) {
				task.isFinish = true
			}
			task.mu.Unlock()
			log.Printf("node %d finish reduce\n", nodeId)
		case nil:
			/*log.Println("Received an empty message, terminating client")*/
			return
		default:
			log.Printf("Unexpected message type: %T", msg)
		}
	}
}

// A helper function converts the heartbeat msg to a getMap response.
func (c *Controller) startReduce(msg *messages.StartReduce, node *messages.StoreNode) {
	conn, err := net.Dial("tcp", node.Ip+":"+strconv.Itoa(int(node.Port)))
	if err != nil {
		log.Println("send start reduce to reduce node fail: " + err.Error())
		return
	}
	msgHandler := messages.NewMessageHandler(conn)
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_StartReduce{
			StartReduce: msg,
		},
	}
	msgHandler.Send(wrapper)
	msgHandler.Close()
}

// A helper function converts the heartbeat msg to a getMap response.
func (c *Controller) convertMap() map[int32]*messages.HeartBeat {
	m := make(map[int32]*messages.HeartBeat)
	c.failureDetector.HeartbeatMap.Range(func(k, v interface{}) bool {
		nodeId := k.(int32)
		curNodeInfo := v.(node_info.NodeInfo)
		heartMsg := messages.HeartBeat{Id: nodeId, Host: curNodeInfo.Host, Port: curNodeInfo.Port,
			FreeSpace: curNodeInfo.FreeSpace, NumberOfRequests: curNodeInfo.NumberOfRequests}
		m[nodeId] = &heartMsg
		return true
	})
	return m
}

//send message to all reduceNode
func (c *Controller) sendReduceTask(jobName string, jobContent []byte, jobId uint32,
	reduceNodes []*messages.StoreNode, fileName string, dirName string) {
	for _, node := range reduceNodes {
		conn, err := net.Dial("tcp", node.Ip+":"+strconv.Itoa(int(node.Port)))
		if err != nil {
			log.Fatalln(err.Error())
			return
		}
		newMsgHandler := messages.NewMessageHandler(conn)
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_SendReduceTask{
				SendReduceTask: &messages.SendReduceTask{
					FileName:   fileName,
					DirName:    dirName,
					JobContent: jobContent,
					JobName:    jobName,
					JobId:      jobId,
				},
			},
		}
		newMsgHandler.Send(wrapper)
	}
}

func (c *Controller) sendJobInfoToClient(handler *messages.MessageHandler) {
	jobInfo := &messages.JobInfo{
		JobId: c.jobId,
	}
	info := &messages.Wrapper_JobInfo{JobInfo: jobInfo}
	wrapper := &messages.Wrapper{Msg: info}
	handler.Send(wrapper)
}

func (c *Controller) handleJobInfoRequest(msgHandler *messages.MessageHandler, id uint32) {
	if task, ok := c.taskMap[id]; !ok {
		msgHandler.Send(&messages.Wrapper{
			Msg: &messages.Wrapper_NotifyClientResult{
				NotifyClientResult: &messages.NotifyClientResult{
					Status:        "wrong",
					JobResultList: nil},
			}})
	} else {
		if !task.isFinish {
			msgHandler.Send(&messages.Wrapper{
				Msg: &messages.Wrapper_NotifyClientResult{
					NotifyClientResult: &messages.NotifyClientResult{
						Status:        "running",
						JobResultList: nil},
				}})
		} else {
			results := make([]*messages.JobResult, 0)
			for i := range task.result {
				res := task.result[i]
				results = append(results, &messages.JobResult{
					FileName: res.fileName,
					DirName:  res.dirName,
					Node:     res.node,
				})
			}
			msgHandler.Send(&messages.Wrapper{
				Msg: &messages.Wrapper_NotifyClientResult{
					NotifyClientResult: &messages.NotifyClientResult{
						Status:        "finish",
						JobResultList: results},
				}})
		}
	}
}

func main() {
	newController := NewController()
	newController.Start()
	time.Sleep(2 * time.Second)
}
