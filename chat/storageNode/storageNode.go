package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mp/chat/messages"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type StorageNode struct {
	Id               int32
	Host             string
	Port             int32
	controllerHost   string
	controllerPort   int32
	FreeSpace        int64
	NumberOfRequests int64
	SavePath         string
	NeighborMap      sync.Map

	ReduceTask map[uint32]*ReduceTask
}

// ReduceTask MapResult save the data come from map nodes
type ReduceTask struct {
	mu          sync.Mutex
	jobName     string
	jobContent  []byte
	fileName    string
	dirName     string
	jobFilePath string
	basePath    string
	data        map[int32][]*messages.Result //chunkId - chunk Data
	//    dirname
	//               |- reduce-id             //basePath
	//                          |--            jobName
	//                                         reduceResult
}

// A helper function convert integer to string.
func convertIntToString(value int32) string {
	return strconv.Itoa(int(value))
}

// A helper function compute the address of given host and port.
func computeAddress(host string, port int32) string {
	addr := host + ":" + convertIntToString(port)
	return addr
}

// NewStorageNode creates a new storage node.
func NewStorageNode(id int32, host string, port int32, controllerHost string, controllerPort int32, savePath string,
	freeSpace int64, numberOfRequests int64) *StorageNode {
	// create file path
	if err := os.MkdirAll(savePath, os.ModePerm); err != nil {
		log.Fatalln(err)
	}
	s := StorageNode{Id: id, Host: host, Port: port, controllerHost: controllerHost, controllerPort: controllerPort,
		SavePath: savePath, FreeSpace: freeSpace * 1024, NumberOfRequests: numberOfRequests, ReduceTask: make(map[uint32]*ReduceTask),
	}
	// start to listen the incoming requests
	go s.startListener()
	go s.StartHeartBeat()
	return &s
}

// Start listening for incoming requests.
func (storageNode *StorageNode) startListener() {
	address := computeAddress(storageNode.Host, storageNode.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	for {
		if conn, err := listener.Accept(); err == nil {
			msgHandler := messages.NewMessageHandler(conn)
			// concurrent, goroutines
			go storageNode.receiveMsg(msgHandler)
		}
	}
}

// StartHeartBeat sends heartbeat message to the controller.
func (storageNode *StorageNode) StartHeartBeat() {
	ticker := time.NewTicker(3000 * time.Millisecond)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				log.Println("done!")
				return
			case _ = <-ticker.C:
				address := computeAddress(storageNode.controllerHost, storageNode.controllerPort)
				conn, err := net.Dial("tcp", address) // connect to controller
				if err != nil {
					log.Println(err.Error())
					break
				}
				msgHandler := messages.NewMessageHandler(conn)
				heartMsg := messages.HeartBeat{Id: storageNode.Id, Host: storageNode.Host, Port: storageNode.Port,
					FreeSpace: storageNode.FreeSpace, NumberOfRequests: storageNode.NumberOfRequests}
				wrapper := &messages.Wrapper{
					Msg: &messages.Wrapper_HeartbeatMessage{
						HeartbeatMessage: &heartMsg,
					},
				}
				msgHandler.Send(wrapper)
				msgHandler.Close()
				log.Println("send heart beat")
			}
		}
	}()
}

// Listening for incoming requests.
func (storageNode *StorageNode) receiveMsg(msgHandler *messages.MessageHandler) {
	quit := false
	for !quit {
		wrapper, _ := msgHandler.Receive()
		switch msg := wrapper.Msg.(type) {

		case *messages.Wrapper_ReplicaChunkMessage:
			//atomic update the number of requests
			atomic.AddInt64(&storageNode.NumberOfRequests, 1)
			index := msg.ReplicaChunkMessage.GetIndex()
			nodeList := msg.ReplicaChunkMessage.GetNodeLists()
			chunkContents := msg.ReplicaChunkMessage.GetChunk()
			fileName := msg.ReplicaChunkMessage.GetFileName()
			chunkIndex := msg.ReplicaChunkMessage.GetCheckIndex()
			dirname := msg.ReplicaChunkMessage.GetDirName()
			storageNode.processReplicaChunk(index, nodeList, chunkContents, fileName, chunkIndex, dirname)
		case *messages.Wrapper_SendMapTask:
			//get map task
			log.Printf("get map task, id: %d  name:%s \n", msg.SendMapTask.JobId, msg.SendMapTask.JobName)
			atomic.AddInt64(&storageNode.NumberOfRequests, 1)
			fileName := msg.SendMapTask.GetFileName()
			jobName := msg.SendMapTask.GetJobName()
			jobContent := msg.SendMapTask.GetJobContent()
			dirName := msg.SendMapTask.GetDirName()
			jobId := msg.SendMapTask.GetJobId()
			idx := msg.SendMapTask.GetChunkIdx()
			reduceNode := msg.SendMapTask.GetNodeList()
			storageNode.processMapTask(fileName, jobName, jobContent, dirName, jobId, idx, reduceNode)
		case *messages.Wrapper_SendReduceTask:
			//get map task
			log.Printf("get reduce task, id: %d  name:%s\n", msg.SendReduceTask.JobId, msg.SendReduceTask.JobName)
			atomic.AddInt64(&storageNode.NumberOfRequests, 1)
			fileName := msg.SendReduceTask.GetFileName()
			jobName := msg.SendReduceTask.GetJobName()
			jobContent := msg.SendReduceTask.GetJobContent()
			dirName := msg.SendReduceTask.GetDirName()
			jobId := msg.SendReduceTask.GetJobId()
			storageNode.processReduceTask(fileName, jobName, jobContent, dirName, jobId)
		case *messages.Wrapper_GetReplicaMessage:
			log.Println("get replica msg:", msg.GetReplicaMessage)
			//atomic update the number of requests
			atomic.AddInt64(&storageNode.NumberOfRequests, 1)
			fileName := msg.GetReplicaMessage.FileName
			chunkIndex := msg.GetReplicaMessage.ChunkIndex
			dirName := msg.GetReplicaMessage.DirName
			storageNode.processGetReplica(dirName, fileName, chunkIndex, msgHandler)
		case *messages.Wrapper_DeleteReplicasMessage:
			atomic.AddInt64(&storageNode.NumberOfRequests, 1)
			fileName := msg.DeleteReplicasMessage.FileName
			dirName := msg.DeleteReplicasMessage.GetDirName()
			chunkIndexList := msg.DeleteReplicasMessage.ChunkToDeleteList
			storageNode.processDeleteReplica(fileName, dirName, chunkIndexList)
		case *messages.Wrapper_SendMapResult:
			//receive map result
			jobId := msg.SendMapResult.GetJobId()
			idx := msg.SendMapResult.GetChunkIdx()
			data := msg.SendMapResult.GetData()
			storageNode.processMapResult(idx, jobId, data)
		case *messages.Wrapper_StartReduce:
			//receive map result
			jobId := msg.StartReduce.GetJobId()
			storageNode.doReduce(jobId)
		case nil:
			/*log.Println("Received an empty message, close connection")*/
		default:
			log.Printf("Unexpected message type: %T", msg)
		}
	}
}

// Process replica chunk message.
func (storageNode *StorageNode) processReplicaChunk(index int32, nodes []*messages.StoreNode, chunk []byte,
	fileName string, chunkIndex int32, dirName string) {
	//connect to the controller
	address := computeAddress(storageNode.controllerHost, storageNode.controllerPort)
	conn, err := net.Dial("tcp", address)
	msgHandler := messages.NewMessageHandler(conn)
	defer conn.Close()

	//write file to disk
	storageNode.writeHelper(fileName, "-chunk-", chunkIndex, chunk, dirName)
	//update the left freeSpace
	atomic.AddInt64(&storageNode.FreeSpace, -int64(len(chunk)))
	//send ack to the controller
	ackMsg := messages.AckReplicaChunk{ReplicaSuccess: true, Id: storageNode.Id, FileName: fileName, ChunkIndex: index}
	ackWrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_AckReplicaChunkMessage{AckReplicaChunkMessage: &ackMsg},
	}
	err = msgHandler.Send(ackWrapper)
	if err != nil {
		return
	}

	//send the next one if it is not the next one
	log.Printf("current index: %d, len: %d\n", index, len(nodes))
	if int(index+1) < len(nodes) {
		nextChunkMsg := messages.ReplicaChunk{Index: index + 1, NodeLists: nodes, Chunk: chunk, FileName: fileName,
			CheckIndex: chunkIndex, DirName: dirName}
		nextReplica := &messages.Wrapper{
			Msg: &messages.Wrapper_ReplicaChunkMessage{ReplicaChunkMessage: &nextChunkMsg},
		}
		nextNode := nodes[index+1]
		nextId := nextNode.GetId()
		curMsgHandler, ok := storageNode.NeighborMap.Load(nextId)

		if ok {
			fmt.Println("connection exists")
			newMsgHandler := curMsgHandler.(*messages.MessageHandler)
			err := newMsgHandler.Send(nextReplica)
			if err != nil {
				return
			}
		} else {
			//if connection not exist
			host := nextNode.GetIp()
			port := nextNode.GetPort()
			addr := computeAddress(host, port)
			conn, err := net.Dial("tcp", addr) // connect to next node
			if err != nil {
				log.Fatalln(err.Error())
				return
			}
			newMsgHandler := messages.NewMessageHandler(conn)
			log.Println("successfully store new connection:", port, " to the connection map:")
			storageNode.NeighborMap.Store(nextId, newMsgHandler)
			err = newMsgHandler.Send(nextReplica)
			if err != nil {
				return
			}
		}
	}
}

// A helper function to write chunk into file.
func (storageNode *StorageNode) writeHelper(fileName string, postFix string, index int32, chunk []byte, dirName string) {
	curFileName := fileName + postFix + strconv.Itoa(int(index))
	var filePath string
	if len(dirName) == 0 {
		filePath = storageNode.SavePath + "/" + curFileName
	} else {
		filePath = storageNode.SavePath + "/" + dirName
		_, err := os.Stat(filePath)
		if os.IsNotExist(err) {
			log.Println("dir not exist")
			if err := os.MkdirAll(filePath, os.ModePerm); err != nil {
				log.Println("make dir ", filePath)
				log.Fatalln(err)
			}
		}
		filePath = filePath + "/" + curFileName
	}
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Println("opening file fails", err)
	}
	defer file.Close()
	write := bufio.NewWriter(file)
	_, err = write.Write(chunk)
	if err != nil {
		return
	}
	//write to the file
	err = write.Flush()
	if err != nil {
		log.Println("Error with flush! ")
		return
	}
	log.Println("Finish writing! ")
}

// Process get replica message.
func (storageNode *StorageNode) processGetReplica(dirName string, fileName string, chunkIndex int32,
	msgHandler *messages.MessageHandler) {
	curFileName := fileName + "-chunk-" + strconv.Itoa(int(chunkIndex))
	filePath := storageNode.SavePath
	if len(dirName) == 0 {
		filePath = filePath + "/" + curFileName
	} else {
		filePath = filePath + "/" + dirName + "/" + curFileName
	}
	f, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	chunks := make([]byte, 0)
	buf := make([]byte, 1024)
	for {
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if 0 == n {
			break
		}
		chunks = append(chunks, buf[:n]...)
	}
	// log.Println(string(chunks))
	returnChunkMsg := messages.ReturnReplica{Chunk: chunks, Id: chunkIndex}
	returnChunkWrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ReturnChunkMessage{ReturnChunkMessage: &returnChunkMsg},
	}
	err = msgHandler.Send(returnChunkWrapper)
	if err != nil {
		return
	}
}

// Process delete replica message.
func (storageNode *StorageNode) processDeleteReplica(fileName string, dirName string, chunkToDeleteList []int32) {
	// using for loop
	for i := 0; i < len(chunkToDeleteList); i++ {
		index := chunkToDeleteList[i]
		curFileName := fileName + "-chunk-" + strconv.Itoa(int(index))
		filePath := storageNode.SavePath
		if len(dirName) == 0 {
			filePath = filePath + "/" + curFileName
		} else {
			filePath = filePath + "/" + dirName + "/" + curFileName
		}
		fi, err := os.Stat(filePath)
		if err != nil {
			return
		}
		// get the size
		size := fi.Size()
		log.Printf("The file is %d bytes long\n", size)
		// Removing file from the directory
		// Using Remove() function
		e := os.Remove(filePath)
		if e != nil {
			log.Fatal(e)
		}
		//update the left space
		atomic.AddInt64(&storageNode.FreeSpace, size)
	}
}

//processMapTask create map output
func (storageNode *StorageNode) processMapTask(fileName string, jobName string, jobContent []byte, dirName string,
	jobId uint32, idx int32, reduceNode []*messages.StoreNode) {

	curFileName := fileName + "-chunk-" + strconv.Itoa(int(idx))
	//chunk file path
	filePath := storageNode.SavePath
	if len(dirName) == 0 {
		filePath = filePath + "/" + curFileName
	} else {
		filePath = filePath + "/" + dirName + "/" + curFileName
	}
	//base path
	basePath := storageNode.SavePath + "/" + dirName + "/" + "map-" + strconv.Itoa(int(jobId))
	os.MkdirAll(basePath, os.ModePerm)
	//create temp job
	jobFilePath := basePath + "/" + jobName
	file, err := os.OpenFile(jobFilePath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Fatalf("create job file fail %v \n", err)
	}
	_, err = file.Write(jobContent)
	file.Close() //close file

	if err != nil {
		log.Fatalf("write job content fail %v \n", err)
	}
	outputFilePath := basePath + "/" + fileName + "-map" //get output filePath
	absFilePath, _ := filepath.Abs(filePath)
	absOutPutPath, _ := filepath.Abs(outputFilePath)
	command := exec.Command(jobFilePath, "map", absFilePath, absOutPutPath)
	err = command.Run()
	//do map func
	if err != nil {
		log.Fatalf("do job %d fail with %v", jobId, err)
	}
	//finish map task
	//read data,shuffle data,and send to reducer
	//reducer will accept and store it unless master send message to process
	f, _ := os.OpenFile(outputFilePath, os.O_RDONLY, os.ModePerm)
	scanner := bufio.NewScanner(f)
	//the data will be send
	m := make(map[*messages.StoreNode][]*messages.Result)
	for i := 0; i < len(reduceNode); i++ {
		m[reduceNode[i]] = make([]*messages.Result, 0)
	}
	//shuffle data
	for scanner.Scan() {
		bytes := scanner.Bytes()
		r := &messages.Result{}
		_ = json.Unmarshal(bytes, r)
		u := string(r.Key)[0]
		node := reduceNode[int(u)%len(reduceNode)]
		m[node] = append(m[node], r)
	}
	f.Close()
	//make message and send to reduce node
	for node := range m {
		resultData, err := json.Marshal(m[node])
		if err != nil {
			log.Printf("marshal data to json fail %v\n", err)
		}
		result := &messages.SendMapResult{
			FileName: fileName,
			DirName:  dirName,
			ChunkIdx: idx,
			JobId:    jobId,
			Data:     resultData,
		}
		SendMapResult(node, result)
	}

	//delete temp file in local
	err = os.RemoveAll(basePath)
	if err != nil {
		log.Println("remove temp dir fail: ", err)
	}
	// notify controller
	address := computeAddress(storageNode.controllerHost, storageNode.controllerPort)
	conn, err := net.Dial("tcp", address) // connect to controller
	if err != nil {
		log.Println("notify controller fail " + err.Error())
	}
	msgHandler := messages.NewMessageHandler(conn)
	defer msgHandler.Close()
	notify := &messages.NotifyMapFinish{
		JobId:   jobId,
		ChunkId: idx,
		NodeId:  storageNode.Id,
	}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_NotifyMapFinish{NotifyMapFinish: notify},
	}
	msgHandler.Send(wrapper)
	log.Printf("node %d finish map process", storageNode.Id)
}

func SendMapResult(node *messages.StoreNode, result *messages.SendMapResult) {
	conn, err := net.Dial("tcp", node.Ip+":"+strconv.Itoa(int(node.Port)))
	if err != nil {
		log.Println("send result to reduce node fail: " + err.Error())
		return
	}
	msgHandler := messages.NewMessageHandler(conn)
	defer msgHandler.Close()
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_SendMapResult{
			SendMapResult: result,
		},
	}
	msgHandler.Send(wrapper)
}

func (storageNode *StorageNode) processReduceTask(fileName, jobName string, jobContent []byte, dirName string, jobId uint32) {
	if _, ok := storageNode.ReduceTask[jobId]; !ok {
		storageNode.ReduceTask[jobId] = &ReduceTask{
			mu:   sync.Mutex{},
			data: make(map[int32][]*messages.Result),
		}
	}
	//may be map send data before
	storageNode.ReduceTask[jobId].jobName = jobName
	storageNode.ReduceTask[jobId].jobContent = jobContent
	storageNode.ReduceTask[jobId].fileName = fileName
	storageNode.ReduceTask[jobId].dirName = dirName
	//make work dir
	basePath := storageNode.SavePath + "/" + dirName + "/" + "reduce-" + strconv.Itoa(int(jobId))
	os.MkdirAll(basePath, os.ModePerm)
	jobFilePath := basePath + "/" + jobName
	storageNode.ReduceTask[jobId].jobFilePath = jobFilePath
	storageNode.ReduceTask[jobId].basePath = basePath
	file, err := os.OpenFile(jobFilePath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Fatalf("create job file fail %v \n", err)
	}
	_, err = file.Write(jobContent)
	file.Close() //close file
	if err != nil {
		log.Fatalf("write job content fail %v \n", err)
	}
}

//fileName, dirName, idx, dirName, jobId, data
func (storageNode *StorageNode) processMapResult(idx int32, jobId uint32, data []byte) {
	if _, ok := storageNode.ReduceTask[jobId]; !ok {
		storageNode.ReduceTask[jobId] = &ReduceTask{
			mu:   sync.Mutex{},
			data: make(map[int32][]*messages.Result),
		}
	}
	task := storageNode.ReduceTask[jobId]
	task.mu.Lock()
	defer task.mu.Unlock()
	mapResult := make([]*messages.Result, 0)
	err := json.Unmarshal(data, &mapResult)
	if err != nil {
		log.Println("convert data to result list fail ", err)
	}
	//append not set , avoid same node do dup reduce
	task.data[idx] = append(task.data[idx], mapResult...) //the data of chunk to be reduced
}

func (storageNode *StorageNode) doReduce(jobId uint32) {
	//do reduce
	task := storageNode.ReduceTask[jobId]
	//sort all data
	sortedMap := make(map[string][][]byte)
	for _, resultList := range task.data {
		for i := range resultList {
			result := resultList[i]
			keyString := string(result.Key)
			if _, ok := sortedMap[keyString]; !ok {
				sortedMap[keyString] = make([][]byte, 0)
			}
			sortedMap[keyString] = append(sortedMap[keyString], result.Value)
		}
	}
	//make input data format
	inputFileName := task.basePath + "/reduce-tmp"
	outPutFileName := task.basePath + "/" + task.fileName + "-result"

	inputFileName, _ = filepath.Abs(inputFileName)
	outPutFileName, _ = filepath.Abs(outPutFileName)

	inputFile, err := os.OpenFile(inputFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Println("create tmp file to reduce fail:", err)
		return
	}
	writer := bufio.NewWriter(inputFile)
	for s := range sortedMap {
		inputData := &messages.InputData{
			Key:   []byte(s),
			Value: sortedMap[s],
		}
		data, _ := json.Marshal(inputData)
		writer.WriteString(string(data) + "\n")
	}
	writer.Flush()
	inputFile.Close()
	command := exec.Command(task.jobFilePath, "reduce", inputFileName, outPutFileName)
	command.Run()
	//finish reduce ,notify controller,dirName is dirname/reduce-id/result
	os.Remove(inputFileName)

	//notify master
	notify := &messages.NotifyReduceFinish{
		JobId:    jobId,
		NodeId:   storageNode.Id,
		DirName:  task.dirName + "/reduce-" + strconv.Itoa(int(jobId)),
		FileName: task.fileName + "-result",
	}
	address := computeAddress(storageNode.controllerHost, storageNode.controllerPort)
	conn, err := net.Dial("tcp", address)
	msgHandler := messages.NewMessageHandler(conn)
	defer msgHandler.Close()
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_NotifyReduceFinish{NotifyReduceFinish: notify},
	}
	msgHandler.Send(wrapper)
	log.Printf("node %d finish reduce process", storageNode.Id)
}

func main() {
	// id, host, port, saved path
	i, _ := strconv.ParseInt(os.Args[1], 10, 32)
	id := int32(i)
	host := os.Args[2]
	p, _ := strconv.ParseInt(os.Args[3], 10, 32)
	port := int32(p)
	path := os.Args[4]

	NewStorageNode(id, host, port, "localhost", 22000, path, 1024*1024, 0)
	time.Sleep(1000 * time.Second)
}
