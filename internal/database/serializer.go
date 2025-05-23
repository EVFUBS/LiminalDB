package database

import (
	"LiminalDb/internal/database/indexing"
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

func GetTableFilePath(filename string) string {
	return TableDir + "/" + filename + FileExtension
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

// writeData is a helper method to write data to a buffer using binary.Write
func (b BinarySerializer) writeData(buf *bytes.Buffer, data interface{}) error {
	return binary.Write(buf, binary.LittleEndian, data)
}

// readData is a helper method to read data from a reader using binary.Read
func (b BinarySerializer) readData(buf *bytes.Reader, data interface{}) error {
	return binary.Read(buf, binary.LittleEndian, data)
}

// writeString writes a string with its length prefix to a buffer
func (b BinarySerializer) writeString(buf *bytes.Buffer, s string) error {
	strBytes := []byte(s)
	if err := b.writeData(buf, uint16(len(strBytes))); err != nil {
		return err
	}
	return b.writeData(buf, strBytes)
}

// readString reads a length-prefixed string from a reader
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

func (b BinarySerializer) SerializeHeader(header FileHeader) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := b.writeData(buf, header.Magic); err != nil {
		return nil, err
	}

	if err := b.writeData(buf, header.Version); err != nil {
		return nil, err
	}

	if err := b.writeData(buf, header.MetadataLength); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (b BinarySerializer) DeserializeHeader(buf *bytes.Reader) (FileHeader, error) {
	var header FileHeader

	if err := b.readData(buf, &header.Magic); err != nil {
		return header, err
	}

	if header.Magic != MagicNumber {
		return FileHeader{}, errors.New("invalid magic number")
	}

	if err := b.readData(buf, &header.Version); err != nil {
		return header, err
	}

	if err := b.readData(buf, &header.MetadataLength); err != nil {
		return header, err
	}

	return header, nil
}

func (b BinarySerializer) SerializeMetadata(metadata TableMetadata) ([]byte, uint32, error) {
	buf := new(bytes.Buffer)

	// Write table name
	if err := b.writeString(buf, metadata.Name); err != nil {
		return nil, 0, err
	}

	// Write column count
	if err := b.writeData(buf, metadata.ColumnCount); err != nil {
		return nil, 0, err
	}

	// Write columns
	for _, col := range metadata.Columns {
		// Write column name
		if err := b.writeString(buf, col.Name); err != nil {
			return nil, 0, err
		}

		// Write column properties
		if err := b.writeData(buf, col.DataType); err != nil {
			return nil, 0, err
		}

		if err := b.writeData(buf, col.Length); err != nil {
			return nil, 0, err
		}

		if err := b.writeData(buf, col.IsNullable); err != nil {
			return nil, 0, err
		}

		if err := b.writeData(buf, col.IsPrimaryKey); err != nil {
			return nil, 0, err
		}
	}

	// Write row count and data offset
	if err := b.writeData(buf, metadata.RowCount); err != nil {
		return nil, 0, err
	}

	if err := b.writeData(buf, metadata.DataOffset); err != nil {
		return nil, 0, err
	}

	// Serialize foreign keys (placeholder for future implementation)
	if err := b.writeData(buf, int64(len(metadata.ForeignKeys))); err != nil {
		return nil, 0, err
	}
	// We're not actually serializing foreign keys yet, just writing the count

	// Serialize indexes
	indexCount := int64(0)
	if metadata.Indexes != nil {
		indexCount = int64(len(metadata.Indexes))
	}

	if err := b.writeData(buf, indexCount); err != nil {
		return nil, 0, err
	}

	// Write indexes
	for _, idx := range metadata.Indexes {
		// Write index name
		if err := b.writeString(buf, idx.Name); err != nil {
			return nil, 0, err
		}

		// Write columns count and names
		if err := b.writeData(buf, int64(len(idx.Columns))); err != nil {
			return nil, 0, err
		}

		for _, col := range idx.Columns {
			if err := b.writeString(buf, col); err != nil {
				return nil, 0, err
			}
		}

		// Write flags
		if err := b.writeData(buf, idx.IsUnique); err != nil {
			return nil, 0, err
		}

		if err := b.writeData(buf, idx.IsPrimary); err != nil {
			return nil, 0, err
		}
	}

	return buf.Bytes(), uint32(buf.Len()), nil
}

func (b BinarySerializer) DeserializeMetadata(buf *bytes.Reader) (TableMetadata, error) {
	var metadata TableMetadata

	// Read table name
	tableName, err := b.readString(buf)
	if err != nil {
		return TableMetadata{}, err
	}
	metadata.Name = tableName

	// Read column count
	if err := b.readData(buf, &metadata.ColumnCount); err != nil {
		return TableMetadata{}, err
	}

	// Read columns
	metadata.Columns = make([]Column, metadata.ColumnCount)
	for i := range metadata.Columns {
		// Read column name
		colName, err := b.readString(buf)
		if err != nil {
			return TableMetadata{}, err
		}
		metadata.Columns[i].Name = colName

		// Read column properties
		if err := b.readData(buf, &metadata.Columns[i].DataType); err != nil {
			return TableMetadata{}, err
		}

		if err := b.readData(buf, &metadata.Columns[i].Length); err != nil {
			return TableMetadata{}, err
		}

		if err := b.readData(buf, &metadata.Columns[i].IsNullable); err != nil {
			return TableMetadata{}, err
		}

		if err := b.readData(buf, &metadata.Columns[i].IsPrimaryKey); err != nil {
			return TableMetadata{}, err
		}
	}

	// Read row count and data offset
	if err := b.readData(buf, &metadata.RowCount); err != nil {
		return TableMetadata{}, err
	}

	if err := b.readData(buf, &metadata.DataOffset); err != nil {
		return TableMetadata{}, err
	}

	// Deserialize foreign keys (placeholder for future implementation)
	var foreignKeyCount int64
	if err := b.readData(buf, &foreignKeyCount); err != nil {
		// If we can't read foreign key count, it might be an old file format
		// Just return what we have so far
		return metadata, nil
	}
	// We're not actually deserializing foreign keys yet, just reading the count

	// Deserialize indexes
	var indexCount int64
	if err := b.readData(buf, &indexCount); err != nil {
		// If we can't read index count, it might be an old file format
		// Just return what we have so far
		return metadata, nil
	}

	// Read indexes
	metadata.Indexes = make([]IndexMetadata, indexCount)
	for i := range metadata.Indexes {
		// Read index name
		idxName, err := b.readString(buf)
		if err != nil {
			return TableMetadata{}, err
		}
		metadata.Indexes[i].Name = idxName

		// Read columns count and names
		var columnCount int64
		if err := b.readData(buf, &columnCount); err != nil {
			return TableMetadata{}, err
		}

		metadata.Indexes[i].Columns = make([]string, columnCount)
		for j := range metadata.Indexes[i].Columns {
			colName, err := b.readString(buf)
			if err != nil {
				return TableMetadata{}, err
			}
			metadata.Indexes[i].Columns[j] = colName
		}

		// Read flags
		if err := b.readData(buf, &metadata.Indexes[i].IsUnique); err != nil {
			return TableMetadata{}, err
		}

		if err := b.readData(buf, &metadata.Indexes[i].IsPrimary); err != nil {
			return TableMetadata{}, err
		}
	}

	return metadata, nil
}

// serializeValue serializes a single value based on its column definition
func (b BinarySerializer) serializeValue(buf *bytes.Buffer, val interface{}, col Column) error {
	switch v := val.(type) {
	case int64:
		if col.DataType != TypeInteger64 {
			return errors.New("data type mismatch for column " + col.Name)
		}
		return b.writeData(buf, v)

	case float64:
		if col.DataType != TypeFloat64 {
			return errors.New("data type mismatch for column " + col.Name)
		}
		return b.writeData(buf, v)

	case string:
		if col.DataType != TypeString {
			return errors.New("data type mismatch for column " + col.Name)
		}
		strBytes := []byte(v)
		if uint16(len(strBytes)) > col.Length {
			return errors.New("string too long for column " + col.Name)
		}
		if err := b.writeData(buf, uint16(len(strBytes))); err != nil {
			return err
		}
		_, err := buf.Write(strBytes)
		return err

	case bool:
		if col.DataType != TypeBoolean {
			return errors.New("data type mismatch for column " + col.Name)
		}
		var boolByte byte
		if v {
			boolByte = 1
		} else {
			boolByte = 0
		}
		return b.writeData(buf, boolByte)

	case time.Time:
		if col.DataType != TypeTimestamp {
			return errors.New("data type mismatch for column " + col.Name)
		}
		return b.writeData(buf, v.Unix())

	default:
		return errors.New("unsupported data type for column " + col.Name)
	}
}

func (b BinarySerializer) SerializeRow(data []interface{}, columns []Column) ([]byte, error) {
	buf := new(bytes.Buffer)

	for i, val := range data {
		col := columns[i]
		if err := b.serializeValue(buf, val, col); err != nil {
			return nil, err
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

func (b BinarySerializer) SerializeTable(table *Table) ([]byte, error) {
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

func (b BinarySerializer) DeserializeTable(data []byte) (*Table, error) {
	buf := bytes.NewReader(data)

	header, err := b.DeserializeHeader(buf)
	if err != nil {
		return nil, err
	}

	metadata, err := b.DeserializeMetadata(buf)
	if err != nil {
		return nil, err
	}

	rows := make([][]interface{}, metadata.RowCount)
	for i := range rows {
		rows[i], err = b.DeserializeRow(buf, metadata.Columns)
		if err != nil {
			return nil, err
		}
	}

	return &Table{Header: header, Metadata: metadata, Data: rows}, nil
}

func (b BinarySerializer) WriteTableToFile(table *Table, filename string) error {
	serialisedTable, err := b.SerializeTable(table)
	if err != nil {
		return err
	}

	if _, err := os.Stat(TableDir); os.IsNotExist(err) {
		err = os.MkdirAll(TableDir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	filename = GetTableFilePath(filename)

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

func (b BinarySerializer) ReadFromFile(filename string) (*Table, *indexing.Index, error) {
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

	table, err := b.ReadTableFromFile(filename)
	if err != nil {
		return nil, nil, err
	}

	return table, nil, nil
}

func (b BinarySerializer) ReadTableFromFile(filename string) (*Table, error) {
	filename = GetTableFilePath(filename)

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	serialisedTable, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return b.DeserializeTable(serialisedTable)
}

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

func (b BinarySerializer) ListTables() ([]string, error) {
	files, err := os.ReadDir(TableDir)
	if err != nil {
		return nil, err
	}

	fileNames := make([]string, 0)
	for _, file := range files {
		if !file.IsDir() {
			fileNames = append(fileNames, file.Name())
		}
	}

	tableNames := make([]string, 0)
	for _, tableName := range fileNames {
		if strings.HasSuffix(tableName, ".bin") {
			name := strings.TrimSuffix(tableName, ".bin")
			tableNames = append(tableNames, name)
		}
	}

	return tableNames, nil
}
