package db

import (
	"errors"
	"os"
	"sync"
)

// Table metadata structure

type DatabaseFile struct {
	file     *os.File
	metadata TableMetadata
	mutex    sync.RWMutex
}

type Table struct {
	Header   FileHeader
	Metadata TableMetadata
	Data     [][]interface{}
}

type FileHeader struct {
	Magic          uint32 // Magic number to identify our file type
	Version        uint16 // File format version
	MetadataLength uint32 // Length of the metadata section
}

type TableMetadata struct {
	Name        string
	ColumnCount int64
	Columns     []Column
	RowCount    int64
	DataOffset  uint32 // Where actual data begins in the file
}

func (m *TableMetadata) ValidateMetadata() error {
	if m.Name == "" {
		return errors.New("table name cannot be empty")
	}

	if m.ColumnCount == 0 {
		return errors.New("table must have at least one column")
	}

	if m.Columns != nil && m.ColumnCount != int64(len(m.Columns)) {
		return errors.New("column count does not match number of columns")
	}

	for i, col := range m.Columns {
		if col.Name == "" {
			return errors.New("column name cannot be empty")
		}

		if col.DataType == TypeString && col.Length == 0 {
			return errors.New("string column length cannot be zero")
		}

		for j := i + 1; int64(j) < m.ColumnCount; j++ {
			if col.Name == m.Columns[j].Name {
				return errors.New("duplicate column name")
			}
		}
	}

	return nil
}

// Column definition
type Column struct {
	Name       string
	DataType   ColumnType
	Length     uint16 // For variable-length types like strings
	IsNullable bool
}

// Custom data types enum
type ColumnType int8

const (
	TypeInteger64 ColumnType = iota
	TypeFloat64
	TypeString
	TypeBoolean
	TypeTimestamp
)
