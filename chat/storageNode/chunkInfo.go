package main

type ChunkInfo struct {
	name     string
	size     int64
	checksum string
	content  []byte
}

// NewChunkInfo creates a new ChunkInfo object.
func NewChunkInfo(name string, size int64, checksum string, content []byte) *ChunkInfo {
	chunk := ChunkInfo{name: name, size: size, checksum: checksum, content: content}
	return &chunk
}

// GetName returns the file name of the chunk.
func (chunkInfo *ChunkInfo) GetName() string {
	return chunkInfo.name
}

// GetSize returns the size of the chunk.
func (chunkInfo *ChunkInfo) GetSize() int64 {
	return chunkInfo.size
}

// GetCheckSum returns the checksum of the chunk.
func (chunkInfo *ChunkInfo) GetCheckSum() string {
	return chunkInfo.checksum
}

// GetContent returns the content of the chunk.
func (chunkInfo *ChunkInfo) GetContent() []byte {
	return chunkInfo.content
}
