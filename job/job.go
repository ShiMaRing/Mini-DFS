package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
)

// Result outPut Result
type Result struct {
	Key   []byte
	Value []byte
}

// InputData reduce input data
type InputData struct {
	Key   []byte
	Value [][]byte
}

// Map user should realize
func Map(line int, data []byte) *Result {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.LittleEndian, int32(1))
	return &Result{
		Key:   data,
		Value: bytesBuffer.Bytes(),
	}
}

// Reduce user should realize
func Reduce(inputKey []byte, data [][]byte) *Result {
	var sum int32
	for i := range data {
		bytesBuffer := bytes.NewBuffer(data[i])
		var u int32
		_ = binary.Read(bytesBuffer, binary.LittleEndian, &u)
		sum += u
	}
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.LittleEndian, sum)
	return &Result{
		Key:   inputKey,
		Value: bytesBuffer.Bytes(),
	}
}

func main() {
	op := os.Args[1]
	filePath := os.Args[2]
	outPutPath := os.Args[3]
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatalln("open file fail: ", filePath)
	}
	results := make([]*Result, 0)
	switch op {
	case "map":
		scanner := bufio.NewScanner(f)
		line := 1
		for scanner.Scan() {
			data := scanner.Bytes()
			line++
			result := Map(line, data)
			results = append(results, result)
		}
	case "reduce":
		//read line by line
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			data := scanner.Bytes()
			inputData := &InputData{}
			err := json.Unmarshal(data, inputData)
			if err != nil {
				log.Fatalln("json convert data fail : ", err)
			}
			result := Reduce(inputData.Key, inputData.Value)
			results = append(results, result)
		}
	}
	//write results to output file
	outputFile, err := os.OpenFile(outPutPath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Fatalln("create out put file fail:", err)
	}
	defer outputFile.Close()
	writer := bufio.NewWriter(outputFile)
	for i := range results {
		result := results[i]
		resultJson, _ := json.Marshal(result)
		writer.WriteString(string(resultJson) + "\n")
	}
	writer.Flush()
}
