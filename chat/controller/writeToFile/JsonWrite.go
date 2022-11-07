package writeToFile

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"mp/chat/messages"
	"mp/chat/node_info"
	"os"
	"sync"
)

type JsonWriter struct {
	mutex    *sync.Mutex
	fileName string
}

func NewJsonWriter(fileName string) *JsonWriter {

	return &JsonWriter{mutex: &sync.Mutex{}, fileName: fileName}
}

// Write storage node to file.
func (w *JsonWriter) Write(storage *FileStorage) {
	content, _ := json.Marshal(storage)
	w.mutex.Lock()
	defer w.mutex.Unlock()
	f, err := os.OpenFile(w.fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Println("cannot open file", w.fileName)
	}
	f.Write(content)
	f.WriteString("\n")
	f.Close()
}

type FileStorage struct {
	FilePath string
	FileName string
	ChunkId  int32
	NodeList []node_info.Node
}

// NewFileStorage creates a file storage with providing information.
func NewFileStorage(filePath string, fileName string, ChunkId int32, nodeList *messages.ChunkNodeInfo) *FileStorage {
	fs := FileStorage{FilePath: filePath, FileName: fileName, ChunkId: ChunkId}
	fs.NodeList = []node_info.Node{}
	for i := 0; i < len(nodeList.GetNodeList()); i++ {
		n := nodeList.GetNodeList()[i]
		node := node_info.NewNode(n.GetId(), n.GetIp(), n.GetPort())
		fs.NodeList = append(fs.NodeList, *node)
	}
	return &fs
}
func (w *JsonWriter) Delete(fileName string, filePath string) {
	w.mutex.Lock()
	file, err := os.Open(w.fileName)
	if err != nil {
		log.Println("file err")
	}
	buf := bufio.NewReader(file)
	var result = ""
	for {
		a, _, err := buf.ReadLine()
		if err == io.EOF {
			log.Println("reading completes")
			break
		}
		log.Println("line: ", string(a))
		if len(a) == 0 {
			break
		}
		var fileStorage FileStorage
		err = json.Unmarshal(a, &fileStorage)
		if err != nil {
			log.Println(err)
		}
		if fileStorage.FileName != fileName {
			result += string(a) + "\n"
		} else if fileStorage.FilePath != filePath {
			result += string(a) + "\n"
		}
	}
	// empty the file and re-write data into the file
	fw, err := os.OpenFile(w.fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Println("Failed to open file, please check the file exists")
		return
	}
	defer fw.Close()

	writer := bufio.NewWriter(fw)
	writer.WriteString(result)
	writer.Flush()
	defer w.mutex.Unlock()
	log.Printf("delete file [%s/%s]\n", filePath, fileName)
}
