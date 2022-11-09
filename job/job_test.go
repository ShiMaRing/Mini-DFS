package main

import (
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
