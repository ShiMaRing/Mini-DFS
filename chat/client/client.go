package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/emirpasic/gods/lists/arraylist"
	"io/ioutil"
	"log"
	"math"
	"mp/chat/messages"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

const controllerAddress = "localhost:22000"
const path = "./bigdata/lliu78/storage/"

type Client struct {
	id       int32
	host     string
	port     int32
	chunkMap sync.Map
}

type chunkSlice struct {
	id    int32
	chunk []byte
}

func newClient(id int32, host string, port int32) *Client {
	client := Client{id: id, host: host, port: port}
	return &client
}

// sends chunks to storage nodes.
func (client *Client) sendGetChunkMsg(dirName string, fileName string, idx int32, nodeList *messages.ChunkNodeInfo,
	channel chan *chunkSlice, wg *sync.WaitGroup) {
	for i := 0; i < 3; i++ {
		node := nodeList.GetNodeList()[i]
		// connect to storage node
		success := client.getChunkFromNode(node, dirName, fileName, idx, channel)
		if success {
			break
		}
	}
	defer wg.Done()
}

// Retrieve a chunk from storage nodes.
func (client *Client) getChunkFromNode(node *messages.StoreNode, dirName string, fileName string, idx int32,
	channel chan *chunkSlice) bool {
	// connect to storage node
	conn, err := net.Dial("tcp", node.GetIp()+":"+strconv.Itoa(int(node.GetPort())))
	if err != nil {
		log.Println(err.Error())
		return false
	}
	msgHandler := messages.NewMessageHandler(conn)

	getReplicaMessage := messages.GetReplica{FileName: fileName, ChunkIndex: idx,
		ClientHost: client.host, ClientPort: client.port, DirName: dirName}
	getReplicaWrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_GetReplicaMessage{GetReplicaMessage: &getReplicaMessage},
	}
	log.Println("replica msg", getReplicaWrapper)
	go msgHandler.Send(getReplicaWrapper)

	wrapper, _ := msgHandler.Receive()
	switch msg := wrapper.Msg.(type) {
	case *messages.Wrapper_ReturnChunkMessage:
		chunk := msg.ReturnChunkMessage.GetChunk()
		cs := chunkSlice{id: idx, chunk: chunk}
		channel <- &cs
	}
	return true
}

// Combine received chunks to a file.
func (client *Client) combineChunks(chunkMap map[int32][]byte, fileName string) {
	_ = os.MkdirAll(path+strconv.Itoa(int(client.id))+"/", 0700)
	out, err := os.Create(path + strconv.Itoa(int(client.id)) + "/" + fileName)
	defer out.Close()
	if err != nil {
		log.Fatalf("Error while creating file: %v", err)
		return
	}

	buf := bytes.NewBuffer(nil)
	for i := 0; i < len(chunkMap); i++ {
		buf.Write(chunkMap[int32(i)])
	}

	l, err := buf.WriteTo(out)
	if err != nil {
		log.Fatalf("Error while creating file: %v", err)
		return
	}
	log.Printf("wrote to file: %v, len: %v", fileName, l)
	return
}

// Incoming requests
func (client *Client) receiveMsg(msgHandler *messages.MessageHandler) {
	quit := false
	for !quit {
		wrapper, _ := msgHandler.Receive()
		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_PutResponse:
			chunkNodeMap := msg.PutResponse.GetChunkMap()
			dirName := msg.PutResponse.GetDirName()
			fmt.Println(msg.PutResponse)
			for wholeChunkName, nodeList := range chunkNodeMap {
				chunkNum := string(wholeChunkName[len(wholeChunkName)-1])
				split := strings.Split(wholeChunkName, "/")
				fileName := split[len(split)-2]
				idx, err := strconv.Atoi(chunkNum)
				if err != nil {
					log.Fatalln(err)
				}
				log.Printf("idx: %d, chunkNum: %s\n", idx, chunkNum)
				// get chunk
				bytes, _ := client.chunkMap.Load(fileName)
				chunk, e := bytes.(*arraylist.List).Get(idx)
				if e {
					if b, ok := chunk.([]byte); ok {
						client.sendReplication(fileName, dirName, int32(idx), &b, nodeList.GetNodeList())
					}
				}
			}
		case *messages.Wrapper_GetResponseMessage:
			fileChunkMap := msg.GetResponseMessage.GetGetResponse()
			dirName := msg.GetResponseMessage.GetDirName()
			wg := &sync.WaitGroup{}
			// channel used to save all chunks
			channel := make(chan *chunkSlice)
			// Each chunk has written into channel
			done := make(chan bool, 1)

			dict := make(map[int32][]byte)
			go func() {
				for slice := range channel {
					dict[slice.id] = slice.chunk
				}
				// send signal to main thread that all chunks has been written into the channel.
				done <- true
			}()
			var fileName string
			for file, nodeMap := range fileChunkMap {
				fileName = file
				log.Println("file is: ", fileName)
				for idx, nodeList := range nodeMap.GetNodeMap() {
					wg.Add(1)
					go client.sendGetChunkMsg(dirName, fileName, idx, nodeList, channel, wg)
				}
				// wait for all goroutines to complete.
				wg.Wait()
				close(channel)
				<-done
				close(done)
			}
			client.combineChunks(dict, fileName)
		case *messages.Wrapper_LsResponseMessage:
			fmt.Println(msg.LsResponseMessage.GetResponse())
		case *messages.Wrapper_SendMapResponseMessage:
			curMap := msg.SendMapResponseMessage.DataMap
			for key, element := range curMap {
				fmt.Println("Id of storage node:", key, "=>", "Node_info:", element)
			}
		case *messages.Wrapper_FileNotExistMessage:
			fmt.Printf("file [%s] not exist\n", msg.FileNotExistMessage.FileName)
		case *messages.Wrapper_FileExistMessage:
			fmt.Printf("file [%s] already exists, please update a new file\n", msg.FileExistMessage.GetFilename())
		case nil:
			log.Println("Received an empty message, terminating client")
			quit = true
		default:
			log.Printf("Unexpected message type: %T", msg)
		}
	}

}

// send chunks to a list of storage nodes
func (client *Client) sendReplication(fileName string, dirName string, chunkIdx int32, bytes *[]byte,
	nodeList []*messages.StoreNode) {
	go sendChunkToNode(int32(0), nodeList, bytes, fileName, dirName, chunkIdx)
}

// send chunk to a node
func sendChunkToNode(i int32, nodeList []*messages.StoreNode, bytes *[]byte, fileName string,
	dirName string, chunkIdx int32) {
	host := nodeList[i].GetIp()
	port := nodeList[i].GetPort()
	// connect to the storage node
	conn, err := net.Dial("tcp", host+":"+strconv.FormatInt(int64(port), 10))
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	msgHandler := messages.NewMessageHandler(conn)
	//defer conn.Close()
	// Send bytes to a storageNode
	msg := &messages.ReplicaChunk{Index: i, NodeLists: nodeList, Chunk: *bytes, FileName: fileName,
		CheckIndex: chunkIdx, DirName: dirName}
	repWrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_ReplicaChunkMessage{ReplicaChunkMessage: msg},
	}
	log.Println("list of node is: ", nodeList)
	msgHandler.Send(repWrapper)
	log.Printf("send a chunk {%d} to host: %s, port %d\n",
		chunkIdx, host, port)
}

// split the file into chunks, each chunk contains 1 MB data
func splitFile(path string) *arraylist.List {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalln(err)
		return nil
	}
	defer file.Close()
	chunkList := arraylist.New()
	fileInfo, _ := file.Stat()
	var fileSize = fileInfo.Size()
	var fileChunk = 1 * (1 << 20) // 1MB
	if fileSize >= 1024*(1<<20) {
		fileChunk = 120 * (1 << 20) // 120MB
	} else if fileSize >= 25*(1<<20) {
		fileChunk = 5 * (1 << 20) // 5MB
	}
	// calculate total number of parts the file will be chunked into
	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))
	fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)
	for i := uint64(0); i < totalPartsNum; i++ {
		partSize := int(math.Min(float64(fileChunk), float64(fileSize-int64(i*uint64(fileChunk)))))
		partBuffer := make([]byte, partSize)
		file.Read(partBuffer)
		chunkList.Add(partBuffer)
	}
	return chunkList
}

// Ls command.
func (client *Client) handleLsCommand(message string) {
	splits := strings.Split(message, " ")
	dir := ""
	if len(splits) == 2 {
		dir = splits[1]
	}
	regMsg := messages.LsRequest{ClientId: client.id, DirName: dir}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_LsRequestMessage{LsRequestMessage: &regMsg},
	}
	client.sendMsgToController(wrapper)
}

// Get requests.
func (client *Client) handleGetRequest(message string) {
	split := strings.Split(message, " ")
	fileName := ""
	dirName := ""
	if len(split) == 3 {
		fileName = split[1]
		dirName = split[2]
	} else if len(split) == 2 {
		fileName = split[1]
	}
	log.Println("filename: ", fileName)
	regMsg := messages.GetRequest{FileName: fileName, DirName: dirName, Id: client.id}
	log.Println(regMsg)
	regWrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_GetRequestMessage{GetRequestMessage: &regMsg},
	}
	client.sendMsgToController(regWrapper)
	log.Println("send a get request to controller: ", regMsg)
}

// Get map requests.
func (client *Client) handleGetMapRequest() {
	// Send bytes to a storageNode
	msg := messages.GetMapRequest{Id: client.id}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_GetMapRequestMessage{
			GetMapRequestMessage: &msg,
		},
	}
	client.sendMsgToController(wrapper)
}

// Delete requests.
func (client *Client) handleDeleteRequest(fn string, fp string) {
	// Send bytes to a storageNode
	msg := messages.DeleteRequest{FileName: fn, FilePath: fp, ClientId: client.id}
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_DeleteRequestMessage{
			DeleteRequestMessage: &msg,
		},
	}
	client.sendMsgToController(wrapper)
}

// Handle put requests.
func (client *Client) handlePutRequest(dirName string, filePath string) {
	putMessage := messages.PutRequest{ClientId: client.id, DirName: dirName}
	split := strings.Split(filePath, "/")
	fileName := split[len(split)-1]
	putMessage.FileName = fileName
	chunkList := splitFile(filePath)
	putMessage.NumOfChunk = int32(chunkList.Size())
	log.Printf("file name is: %s, dir is: %s", fileName, dirName)
	client.chunkMap.Store(fileName, chunkList)
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_PutRequest{
			PutRequest: &putMessage,
		},
	}
	client.sendMsgToController(wrapper)
}

func (client *Client) sendMsgToController(wrapper *messages.Wrapper) {
	// connect to controller
	conn, err := net.Dial("tcp", controllerAddress)
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	msgHandler := messages.NewMessageHandler(conn)
	defer conn.Close()
	msgHandler.Send(wrapper)
	client.receiveMsg(msgHandler)
}

//upload job to controller
//format: submit jobPath reduceCount  filename  dirName
//example: submit ./wordCount 2   hello.txt  data
func (client *Client) handleSubmitJob(message string) {
	params := strings.Split(message, " ")
	if len(params) != 5 {
		log.Fatalln("wrong params num")
	}
	jobPath := params[1]
	jobPathSplit := strings.Split(jobPath, "/")
	jobName := jobPathSplit[len(jobPathSplit)-1] //getJob Name
	f, err := os.Open(jobPath)
	if err != nil {
		log.Fatalln(err)
	}
	defer f.Close()
	reduceCount, _ := strconv.ParseInt(params[2], 10, 32)
	jobContent, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalln("load job content fail", err)
	}
	subMitRequest := &messages.SubmitJobRequest{
		ClientId:   client.id,
		FileName:   params[4],
		ReduceNum:  int32(reduceCount),
		DirName:    params[3],
		JobContent: jobContent,
		JobName:    jobName,
	}
	log.Printf("send submitJob,fileName is %s ,dirName is %s", params[4], params[3])
	wrapper := &messages.Wrapper{
		Msg: &messages.Wrapper_SubmitJobMessage{
			SubmitJobMessage: subMitRequest,
		},
	}
	client.sendMsgToController(wrapper)
}

func main() {
	res, err := strconv.ParseInt(os.Args[1], 10, 32)
	if err != nil {
		panic(err)
	}

	id := int32(res)
	host := os.Args[2]
	p, _ := strconv.ParseInt(os.Args[3], 10, 32)
	port := int32(p)
	client := newClient(id, host, int32(port))
	log.Println("Hello, ", client)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		result := scanner.Scan() // Reads up to a \n newline character
		if result == false {
			break
		}
		message := scanner.Text()
		if len(message) != 0 {
			// put message
			if strings.HasPrefix(message, "put") {
				splits := strings.Split(message, " ")
				var filePath string
				var dirName = ""
				if len(splits) == 3 {
					filePath = splits[1]
					dirName = splits[2]
				} else if len(splits) == 2 {
					filePath = splits[1]
				}
				go client.handlePutRequest(dirName, filePath)
			} else if message == "ls" {
				// handle 'ls' command
				log.Println("received a ls command")
				go client.handleLsCommand(message)
			} else if strings.HasPrefix(message, "getMap") {
				go client.handleGetMapRequest()
			} else if strings.HasPrefix(message, "get") {
				go client.handleGetRequest(message)
			} else if strings.HasPrefix(message, "delete") {
				var fileName string
				var filePath = ""
				splits := strings.Split(message, " ")
				if len(splits) == 3 {
					fileName = splits[1]
					filePath = splits[2]
				} else if len(splits) == 2 {
					fileName = splits[1]
				}
				go client.handleDeleteRequest(fileName, filePath)
			} else if strings.HasPrefix(message, "submit") {
				go client.handleSubmitJob(message)
			} else {
				fmt.Println("Unknown command!")
			}
		}
	}
}
