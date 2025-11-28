package serializer

import (
	db "LiminalDb/internal/database"
	"LiminalDb/internal/database/indexing"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
)

// table file structure:
// Header:
// 1. Magic Version
// 2. Version
// 3. MetadataLength
// Metadata:
// 1. table name length
// 2. table name
// 3. column count
// 4. columns
// 5. row count
// 6. data offset (where the actual data starts)
// Data:
// Serialized rows

type Serializer interface {
	SerializeHeader(header db.FileHeader) ([]byte, error)
	DeserializeHeader(data []byte) (db.FileHeader, error)
	SerializeMetadata(metadata db.TableMetadata) ([]byte, error)
	DeserializeMetadata(data []byte) (db.TableMetadata, error)
	SerializeRow(data []any, columns []db.Column) ([]byte, error)
	DeserializeRow(data []byte, columns []db.Column) ([]any, error)
}

type BinarySerializer struct{}

func NewBinarySerializer() *BinarySerializer {
	return &BinarySerializer{}
}

func (b BinarySerializer) writeData(buf *bytes.Buffer, data any) error {
	return binary.Write(buf, binary.LittleEndian, data)
}

func (b BinarySerializer) readData(buf *bytes.Reader, data any) error {
	return binary.Read(buf, binary.LittleEndian, data)
}

func (b BinarySerializer) writeString(buf *bytes.Buffer, s string) error {
	strBytes := []byte(s)
	if err := b.writeData(buf, uint16(len(strBytes))); err != nil {
		return err
	}
	return b.writeData(buf, strBytes)
}

func (b BinarySerializer) readString(buf *bytes.Reader) (string, error) {
	var length uint16
	if err := b.readData(buf, &length); err != nil {
		return "", err
	}

	strBytes := make([]byte, length)
	if _, err := buf.Read(strBytes); err != nil {
		return "", err
	}

	return string(strBytes), nil
}

func (b BinarySerializer) SerializeInt64Array(data []int64) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, int64(len(data))); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (b BinarySerializer) DeserializeInt64Array(r io.Reader) ([]int64, error) {
	var length int64
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}
	data := make([]int64, length)
	if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func (b BinarySerializer) ReadFromFile(filename string) (*db.Table, *indexing.Index, error) {
	if strings.Contains(filename, ".idx") {
		index, err := b.ReadIndexFromFile(filename)
		if index == nil {
			return nil, nil, errors.New("invalid index file")
		}

		if err != nil {
			return nil, nil, err
		}

		return nil, index, nil
	}

	table, err := b.ReadTableFromPath(filename)
	if err != nil {
		return nil, nil, err
	}

	return table, nil, nil
}
