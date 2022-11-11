package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"strconv"
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
	itoa := strconv.Itoa(1)
	return &Result{
		Key:   data,
		Value: []byte(itoa),
	}
}

// Reduce user should realize
func Reduce(inputKey []byte, data [][]byte) *Result {
	var sum int
	for i := range data {
		s := string(data[i])
		atoi, _ := strconv.Atoi(s)
		sum += atoi
	}
	itoa := strconv.Itoa(sum)
	return &Result{
		Key:   inputKey,
		Value: []byte(itoa),
	}
}

//you only need to write code to read file and send data to map func
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
		//read file and do map,you should deal with it yourself
		scanner := bufio.NewScanner(f)
		line := 1
		for scanner.Scan() {
			data := scanner.Bytes()
			if len(data) == 0 {
				continue
			}
			line++
			result := Map(line, data)
			results = append(results, result)
		}

	case "reduce":
		//read line by line
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			data := scanner.Bytes()
			if len(data) == 0 {
				continue
			}
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
	outputFile, err := os.OpenFile(outPutPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Fatalln("create out put file fail:", err)
	}
	defer outputFile.Close()
	writer := bufio.NewWriter(outputFile)
	if op == "map" {
		for i := range results {
			result := results[i]
			resultJson, _ := json.Marshal(result)
			writer.WriteString(string(resultJson) + "\n")
		}
	} else {
		for i := range results {
			key := results[i].Key
			value := results[i].Value
			writer.Write(key)
			writer.WriteString(" ")
			writer.Write(value)
			writer.WriteString("\n")
		}
	}
	writer.Flush()
}
