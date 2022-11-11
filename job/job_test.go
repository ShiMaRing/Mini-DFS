package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"testing"
)

func TestJob(t *testing.T) {
	f, _ := os.Open("test-out")
	all, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalln(err)
	}
	result := &Result{}
	err = json.Unmarshal(all, result)
	fmt.Println(string(result.Key))
	fmt.Println(string(result.Value))

}

func TestCountJob(t *testing.T) {
	f, _ := os.Open("test-out")
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		data := scanner.Bytes()
		result := &Result{}
		err := json.Unmarshal(data, result)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println(string(result.Key), string(result.Value))
	}

}

func TestWrite(t *testing.T) {
	outputFile, err := os.OpenFile("test.txt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Fatalln("create out put file fail:", err)
	}
	defer outputFile.Close()
	writer := bufio.NewWriter(outputFile)
	var sum = 1003
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.LittleEndian, int32(sum))

	writer.Write([]byte("hello"))
	writer.WriteString(" ")
	writer.Write([]byte("world"))
	writer.WriteString("\n")
	writer.Write([]byte("hello2"))
	writer.WriteString(" ")
	writer.Write([]byte("world2"))
	writer.WriteString("\n")
	fmt.Println(bytesBuffer.Bytes())
	writer.Write(bytesBuffer.Bytes())
	writer.Flush()
}

// split the file into chunks, each chunk contains 1 MB data,if type is text ,will split with line
func TestSplitFile(t *testing.T) {
	file, err := os.Open("test.txt")
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	fileInfo, _ := file.Stat()
	var fileSize = fileInfo.Size()
	var fileChunk = 4
	// calculate total number of parts the file will be chunked into
	totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))
	var offset int64
	//split in line
	for i := uint64(0); i < totalPartsNum; i++ {
		file.Seek(offset, 0)
		reader := bufio.NewReader(file)
		var cumulativeSize int64
		part := make([]byte, 0)
		for {
			if cumulativeSize > int64(fileChunk) {
				break
			}
			b, err := reader.ReadBytes('\n')
			if err == io.EOF {
				part = append(part, b...)
				fmt.Println(string(part))
				return
			}
			part = append(part, b...)
			if err != nil {
				panic(err)
			}
			cumulativeSize += int64(len(b))
		}
		fmt.Println(string(part))
		offset += cumulativeSize
	}
}
