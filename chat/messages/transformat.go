package messages

// Result map and reduce outPut data
type Result struct {
	Key   []byte
	Value []byte
}

// InputData reduce input data
type InputData struct {
	Key   []byte
	Value [][]byte
}
