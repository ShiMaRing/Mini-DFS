package main

// FileInfo Temporary files generated
type FileInfo struct {
	name    string
	size    int64
	content []byte
}

func NewFileInfo(name string, size int64, content []byte) *FileInfo {
	return &FileInfo{name: name, size: size, content: content}
}
