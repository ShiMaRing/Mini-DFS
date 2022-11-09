package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestJob(t *testing.T) {
	f, _ := os.Open("test.txt")
	all, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalln(err)
	}
	result := &Result{}
	err = json.Unmarshal(all, result)
	fmt.Println(string(result.Key))
	fmt.Println(string(result.Value))

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
