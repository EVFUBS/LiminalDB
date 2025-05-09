package database

import (
	"errors"
	"os"
	"sync"
)

const (
	TypeInteger64 ColumnType = iota
	TypeFloat64
	TypeString
	TypeBoolean
	TypeTimestamp
)

const MagicNumber uint32 = 0x4D444247
const CurrentVersion uint16 = 1

const (
	DatabaseDir   = "db"
	FileExtension = ".bin"
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
	Magic          uint32 // Magic number to identify file type
	Version        uint16 // File format version
	MetadataLength uint32 // Length of the metadata section
}

type TableMetadata struct {
	Name        string
	ColumnCount int64
	Columns     []Column
	RowCount    int64
	DataOffset  uint32
	ForeignKeys []ForeignKeyConstraint
	Indexes     []IndexMetadata
}

type Column struct {
	Name         string
	DataType     ColumnType
	Length       uint16 // For variable-length types like strings
	IsNullable   bool
	IsPrimaryKey bool
}

type ForeignKeyConstraint struct {
	ReferencedTable   string
	ReferencedColumns []ForeignKeyReference
}

type ForeignKeyReference struct {
	ColumnName           string
	ReferencedColumnName string
}

type IndexMetadata struct {
	Name      string
	Columns   []string
	IsUnique  bool
	IsPrimary bool
}

type ColumnType int8

type QueryResult struct {
	Columns []Column
	Rows    [][]interface{}
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

	hasPrimaryKey := false
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

		if col.IsPrimaryKey {
			hasPrimaryKey = true
			if col.IsNullable {
				return errors.New("primary key column cannot be nullable")
			}
		}
	}

	for _, fk := range m.ForeignKeys {
		for _, ref := range fk.ReferencedColumns {
			if ref.ColumnName == "" || ref.ReferencedColumnName == "" {
				return errors.New("foreign key column name cannot be empty")
			}
		}
	}

	if !hasPrimaryKey {
		return errors.New("table must have at least one primary key")
	}

	return nil
}

func (c ColumnType) String() string {
	switch c {
	case TypeInteger64:
		return "INT"
	case TypeFloat64:
		return "FLOAT"
	case TypeString:
		return "STRING"
	case TypeBoolean:
		return "BOOL"
	case TypeTimestamp:
		return "TIMESTAMP"
	default:
		return "UNKNOWN"
	}
}
