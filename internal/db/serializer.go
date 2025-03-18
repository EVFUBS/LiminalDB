package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"strings"
	"time"
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

func getTableFilePath(filename string) string {
	return DatabaseDir + "/" + filename + FileExtension
}

type Serializer interface {
	SerializeHeader(header FileHeader) ([]byte, error)
	DeserializeHeader(data []byte) (FileHeader, error)
	SerializeMetadata(metadata TableMetadata) ([]byte, error)
	DeserializeMetadata(data []byte) (TableMetadata, error)
	SerializeRow(data []interface{}, columns []Column) ([]byte, error)
	DeserializeRow(data []byte, columns []Column) ([]interface{}, error)
}

type BinarySerializer struct {
}

func (b BinarySerializer) SerializeHeader(header FileHeader) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, header.Magic); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, header.Version); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.LittleEndian, header.MetadataLength); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (b BinarySerializer) DeserializeHeader(buf *bytes.Reader) (FileHeader, error) {
	var header FileHeader

	if err := binary.Read(buf, binary.LittleEndian, &header.Magic); err != nil {
		return header, err
	}

	if header.Magic != MagicNumber {
		return FileHeader{}, errors.New("invalid magic number")
	}

	if err := binary.Read(buf, binary.LittleEndian, &header.Version); err != nil {
		return header, err
	}

	if err := binary.Read(buf, binary.LittleEndian, &header.MetadataLength); err != nil {
		return header, err
	}

	return header, nil
}

func (b BinarySerializer) SerializeMetadata(metadata TableMetadata) ([]byte, uint32, error) {
	buf := new(bytes.Buffer)

	nameBytes := []byte(metadata.Name)
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(nameBytes))); err != nil {
		return nil, 0, err
	}

	if err := binary.Write(buf, binary.LittleEndian, nameBytes); err != nil {
		return nil, 0, err
	}

	if err := binary.Write(buf, binary.LittleEndian, metadata.ColumnCount); err != nil {
		return nil, 0, err
	}

	for _, col := range metadata.Columns {
		colNameBytes := []byte(col.Name)
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(colNameBytes))); err != nil {
			return nil, 0, err
		}

		if err := binary.Write(buf, binary.LittleEndian, colNameBytes); err != nil {
			return nil, 0, err
		}

		if err := binary.Write(buf, binary.LittleEndian, col.DataType); err != nil {
			return nil, 0, err
		}

		if err := binary.Write(buf, binary.LittleEndian, col.Length); err != nil {
			return nil, 0, err
		}

		if err := binary.Write(buf, binary.LittleEndian, col.IsNullable); err != nil {
			return nil, 0, err
		}

		if err := binary.Write(buf, binary.LittleEndian, col.IsPrimaryKey); err != nil {
			return nil, 0, err
		}
	}

	if err := binary.Write(buf, binary.LittleEndian, metadata.RowCount); err != nil {
		return nil, 0, err
	}
	if err := binary.Write(buf, binary.LittleEndian, metadata.DataOffset); err != nil {
		return nil, 0, err
	}

	return buf.Bytes(), uint32(buf.Len()), nil
}

func (b BinarySerializer) DeserializeMetadata(buf *bytes.Reader) (TableMetadata, error) {
	var metadata TableMetadata

	var nameLength uint16
	if err := binary.Read(buf, binary.LittleEndian, &nameLength); err != nil {
		return TableMetadata{}, err
	}

	nameBytes := make([]byte, nameLength)
	if _, err := buf.Read(nameBytes); err != nil {
		return TableMetadata{}, err
	}
	metadata.Name = string(nameBytes)

	if err := binary.Read(buf, binary.LittleEndian, &metadata.ColumnCount); err != nil {
		return TableMetadata{}, err
	}

	metadata.Columns = make([]Column, metadata.ColumnCount)
	for i := range metadata.Columns {
		var colNameLength uint16
		if err := binary.Read(buf, binary.LittleEndian, &colNameLength); err != nil {
			return TableMetadata{}, err
		}

		colNameBytes := make([]byte, colNameLength)
		if _, err := buf.Read(colNameBytes); err != nil {
			return TableMetadata{}, err
		}
		metadata.Columns[i].Name = string(colNameBytes)

		if err := binary.Read(buf, binary.LittleEndian, &metadata.Columns[i].DataType); err != nil {
			return TableMetadata{}, err
		}

		if err := binary.Read(buf, binary.LittleEndian, &metadata.Columns[i].Length); err != nil {
			return TableMetadata{}, err
		}

		if err := binary.Read(buf, binary.LittleEndian, &metadata.Columns[i].IsNullable); err != nil {
			return TableMetadata{}, err
		}

		if err := binary.Read(buf, binary.LittleEndian, &metadata.Columns[i].IsPrimaryKey); err != nil {
			return TableMetadata{}, err
		}
	}

	if err := binary.Read(buf, binary.LittleEndian, &metadata.RowCount); err != nil {
		return TableMetadata{}, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.DataOffset); err != nil {
		return TableMetadata{}, err
	}

	return metadata, nil
}

func (b BinarySerializer) SerializeRow(data []interface{}, columns []Column) ([]byte, error) {
	buf := new(bytes.Buffer)

	for i, val := range data {
		col := columns[i]

		switch v := val.(type) {
		case int64:
			if col.DataType != TypeInteger64 {
				return nil, errors.New("data type mismatch for column " + col.Name)
			}
			if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
				return nil, err
			}
		case float64:
			if col.DataType != TypeFloat64 {
				return nil, errors.New("data type mismatch for column " + col.Name)
			}
			if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
				return nil, err
			}
		case string:
			if col.DataType != TypeString {
				return nil, errors.New("data type mismatch for column " + col.Name)
			}
			strBytes := []byte(v)
			if uint16(len(strBytes)) > col.Length {
				return nil, errors.New("string too long for column " + col.Name)
			}
			if err := binary.Write(buf, binary.LittleEndian, uint16(len(strBytes))); err != nil {
				return nil, err
			}
			if _, err := buf.Write(strBytes); err != nil {
				return nil, err
			}
		case bool:
			if col.DataType != TypeBoolean {
				return nil, errors.New("data type mismatch for column " + col.Name)
			}
			var boolByte byte
			if v {
				boolByte = 1
			} else {
				boolByte = 0
			}
			if err := binary.Write(buf, binary.LittleEndian, boolByte); err != nil {
				return nil, err
			}
		case time.Time:
			if col.DataType != TypeTimestamp {
				return nil, errors.New("data type mismatch for column " + col.Name)
			}
			if err := binary.Write(buf, binary.LittleEndian, v.Unix()); err != nil {
			}
		default:
			return nil, errors.New("unsupported data type for column " + col.Name)
		}
	}

	return buf.Bytes(), nil
}

func (b BinarySerializer) DeserializeRow(buf *bytes.Reader, columns []Column) ([]interface{}, error) {
	var row []interface{}

	for _, col := range columns {
		switch col.DataType {
		case TypeInteger64:
			var val int64
			if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
				return nil, err
			}
			row = append(row, val)
		case TypeFloat64:
			var val float64
			if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
				return nil, err
			}
			row = append(row, val)
		case TypeString:
			var strLen uint16
			if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
				return nil, err
			}

			var strBytes []byte

			if strLen > 0 {
				strBytes = make([]byte, strLen)
				if _, err := buf.Read(strBytes); err != nil {
					return nil, err
				}
			}
			row = append(row, string(strBytes))
		case TypeBoolean:
			var boolByte byte
			if err := binary.Read(buf, binary.LittleEndian, &boolByte); err != nil {
				return nil, err
			}
			row = append(row, boolByte == 1)
		case TypeTimestamp:
			var unixTime int64
			if err := binary.Read(buf, binary.LittleEndian, &unixTime); err != nil {
				return nil, err
			}
			row = append(row, time.Unix(unixTime, 0))
		default:
			panic("unhandled default case")
		}
	}

	return row, nil
}

func (b BinarySerializer) SerializeTable(table Table) ([]byte, error) {
	buf := new(bytes.Buffer)

	_, metadataLength, err := b.SerializeMetadata(table.Metadata)
	if err != nil {
		return nil, err
	}

	table.Header.MetadataLength = metadataLength

	headerBytes, err := b.SerializeHeader(table.Header)
	if err != nil {
		return nil, err
	}

	dataOffset := uint32(len(headerBytes)) + metadataLength
	table.Metadata.DataOffset = dataOffset
	table.Metadata.RowCount = int64(len(table.Data))
	table.Metadata.ColumnCount = int64(len(table.Metadata.Columns))

	metadataBytes, _, err := b.SerializeMetadata(table.Metadata)
	if err != nil {
		return nil, err
	}

	dataBytes := make([][]byte, len(table.Data))
	for i, row := range table.Data {
		dataBytes[i], err = b.SerializeRow(row, table.Metadata.Columns)
		if err != nil {
			return nil, err
		}
	}

	buf.Write(headerBytes)
	buf.Write(metadataBytes)
	for _, row := range dataBytes {
		buf.Write(row)
	}

	return buf.Bytes(), nil
}

func (b BinarySerializer) DeserializeTable(data []byte) (Table, error) {
	buf := bytes.NewReader(data)

	header, err := b.DeserializeHeader(buf)
	if err != nil {
		return Table{}, err
	}

	metadata, err := b.DeserializeMetadata(buf)
	if err != nil {
		return Table{}, err
	}

	rows := make([][]interface{}, metadata.RowCount)
	for i := range rows {
		rows[i], err = b.DeserializeRow(buf, metadata.Columns)
		if err != nil {
			return Table{}, err
		}
	}

	return Table{Header: header, Metadata: metadata, Data: rows}, nil
}

func (b BinarySerializer) WriteTableToFile(table Table, filename string) error {
	serialisedTable, err := b.SerializeTable(table)
	if err != nil {
		return err
	}

	if _, err := os.Stat(DatabaseDir); os.IsNotExist(err) {
		err = os.Mkdir(DatabaseDir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	filename = getTableFilePath(filename)

	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(serialisedTable)
	if err != nil {
		return err
	}

	return nil
}

func (b BinarySerializer) ReadTableFromFile(filename string) (Table, error) {
	filename = getTableFilePath(filename)

	file, err := os.Open(filename)
	if err != nil {
		return Table{}, err
	}
	defer file.Close()

	serialisedTable, err := io.ReadAll(file)
	if err != nil {
		return Table{}, err
	}

	return b.DeserializeTable(serialisedTable)
}

func (b BinarySerializer) ListTables() ([]string, error) {
	files, err := os.ReadDir(DatabaseDir)
	if err != nil {
		return nil, err
	}

	tables := make([]string, 0)
	for _, file := range files {
		if !file.IsDir() {
			tables = append(tables, file.Name())
		}
	}

	for i, tableName := range tables {
		if strings.HasSuffix(tableName, ".bin") {
			tables[i] = strings.TrimSuffix(tableName, ".bin")
		}
	}

	return tables, nil
}
