package db

import (
	"bytes"
	"encoding/binary"
	"errors"
)

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

	if err := binary.Write(buf, binary.LittleEndian, header); err != nil {
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

func (b BinarySerializer) DeserializeHeader(data []byte) (FileHeader, error) {
	var header FileHeader
	buf := bytes.NewReader(data)

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

func (b BinarySerializer) SerializeMetadata(metadata TableMetadata) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Write table name length and table name
	nameBytes := []byte(metadata.Name)
	if err := binary.Write(buf, binary.LittleEndian, uint16(len(nameBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(nameBytes); err != nil {
		return nil, err
	}

	// Write column count
	if err := binary.Write(buf, binary.LittleEndian, metadata.ColumnCount); err != nil {
		return nil, err
	}

	// Write columns
	for _, col := range metadata.Columns {
		// Write column name length and name
		colNameBytes := []byte(col.Name)
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(colNameBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(colNameBytes); err != nil {
			return nil, err
		}

		// Write column type
		if err := binary.Write(buf, binary.LittleEndian, col.DataType); err != nil {
			return nil, err
		}

		// Write length for variable-length types
		if err := binary.Write(buf, binary.LittleEndian, col.Length); err != nil {
			return nil, err
		}

		// Write nullable flag
		if err := binary.Write(buf, binary.LittleEndian, col.IsNullable); err != nil {
			return nil, err
		}
	}

	// Write row count and data offset
	if err := binary.Write(buf, binary.LittleEndian, metadata.RowCount); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, metadata.DataOffset); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (b BinarySerializer) DeserializeMetadata(data []byte) (TableMetadata, error) {
	var metadata TableMetadata
	buf := bytes.NewReader(data)

	// Read table name length
	var nameLength uint16
	if err := binary.Read(buf, binary.LittleEndian, &nameLength); err != nil {
		return TableMetadata{}, err
	}

	// Read table name
	nameBytes := make([]byte, nameLength)
	if _, err := buf.Read(nameBytes); err != nil {
		return TableMetadata{}, err
	}
	metadata.Name = string(nameBytes)

	// Read column count
	if err := binary.Read(buf, binary.LittleEndian, &metadata.ColumnCount); err != nil {
		return TableMetadata{}, err
	}

	// Read columns
	metadata.Columns = make([]Column, metadata.ColumnCount)
	for i := range metadata.Columns {
		// Read column name length
		var colNameLength uint16
		if err := binary.Read(buf, binary.LittleEndian, &colNameLength); err != nil {
			return TableMetadata{}, err
		}

		// Read column name
		colNameBytes := make([]byte, colNameLength)
		if _, err := buf.Read(colNameBytes); err != nil {
			return TableMetadata{}, err
		}
		metadata.Columns[i].Name = string(colNameBytes)

		// Read column type
		if err := binary.Read(buf, binary.LittleEndian, &metadata.Columns[i].DataType); err != nil {
			return TableMetadata{}, err
		}

		// Read length
		if err := binary.Read(buf, binary.LittleEndian, &metadata.Columns[i].Length); err != nil {
			return TableMetadata{}, err
		}

		// Read nullable flag
		if err := binary.Read(buf, binary.LittleEndian, &metadata.Columns[i].IsNullable); err != nil {
			return TableMetadata{}, err
		}
	}

	// Read row count and data offset
	if err := binary.Read(buf, binary.LittleEndian, &metadata.RowCount); err != nil {
		return TableMetadata{}, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &metadata.DataOffset); err != nil {
		return TableMetadata{}, err
	}

	return metadata, nil
}

func (b BinarySerializer) SerializeRow(data []interface{}, columns []Column) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (b BinarySerializer) DeserializeRow(data []byte, columns []Column) ([]interface{}, error) {
	//TODO implement me
	panic("implement me")
}
