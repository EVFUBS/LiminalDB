package serializer

import (
	"LiminalDb/internal/database/indexing"
	"io"
	"os"
)

func (b BinarySerializer) ReadIndexFromFile(filename string) (*indexing.Index, error) {
	file, err := os.Open(filename)
	if err != nil {
		return &indexing.Index{}, err
	}
	defer file.Close()

	indexBytes, err := io.ReadAll(file)
	if err != nil {
		return &indexing.Index{}, err
	}

	return indexing.DeserializeIndex(indexBytes)
}

