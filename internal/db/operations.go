package db

import (
	"fmt"
	"os"
	"strings"
)

const MagicNumber uint32 = 0x4D444247
const CurrentVersion uint16 = 1

type Operations interface {
	CreateTable(metadata TableMetadata) error
	DropTable(tableName string) error
	ReadMetadata(filename string) (TableMetadata, error)
	WriteRows(tableName string, data [][]interface{}) error
	ReadRows(tableName string, fields []string, filter func([]interface{}, []Column) (bool, error)) (*QueryResult, error)
	DeleteRows(tableName string, filter func([]interface{}, []Column) (bool, error)) (int64, error)
}

type OperationsImpl struct {
	serializer BinarySerializer
}

func (o *OperationsImpl) CreateTable(metadata TableMetadata) error {

	table := Table{
		Header: FileHeader{
			Magic:   MagicNumber,
			Version: CurrentVersion,
		},
		Metadata: metadata,
		Data:     [][]interface{}{},
	}

	err := o.serializer.WriteTableToFile(table, metadata.Name)
	if err != nil {
		return err
	}

	return nil
}

func (o *OperationsImpl) DropTable(tableName string) error {
	err := os.Remove(getTableFilePath(tableName))
	if err != nil {
		return err
	}
	return nil
}

func (o *OperationsImpl) ReadMetadata(filename string) (TableMetadata, error) {
	table, err := o.serializer.ReadTableFromFile(filename)
	if err != nil {
		return TableMetadata{}, err
	}
	return table.Metadata, nil
}

func (o *OperationsImpl) WriteRows(tableName string, data [][]interface{}) error {
	table, err := o.serializer.ReadTableFromFile(tableName)
	if err != nil {
		return err
	}

	table.Data = append(table.Data, data...)
	return o.serializer.WriteTableToFile(table, tableName)
}

func (o *OperationsImpl) ReadRows(tableName string, fields []string, filter func([]interface{}, []Column) (bool, error)) (*QueryResult, error) {
	table, err := o.serializer.ReadTableFromFile(tableName)
	if err != nil {
		return &QueryResult{}, err
	}

	result := &QueryResult{
		Columns: table.Metadata.Columns,
	}

	for _, row := range table.Data {
		if filter != nil {
			matches, err := filter(row, table.Metadata.Columns)
			if err != nil {
				return nil, err
			}
			if !matches {
				continue
			}
		}

		var selectedRow []interface{}
		if len(fields) == 0 || (len(fields) == 1 && fields[0] == "*") {
			selectedRow = make([]interface{}, len(row))
			copy(selectedRow, row)
		} else {
			selectedRow = []interface{}{}
			for _, field := range fields {
				found := false
				for index, col := range table.Metadata.Columns {
					if strings.EqualFold(col.Name, field) {
						selectedRow = append(selectedRow, row[index])
						found = true
						break
					}
				}
				if !found {
					return nil, fmt.Errorf("column not found: %s", field)
				}
			}
		}

		result.Rows = append(result.Rows, selectedRow)
	}

	return result, nil
}

func (o *OperationsImpl) DeleteRows(tableName string, filter func([]interface{}, []Column) (bool, error)) (int64, error) {
	table, err := o.serializer.ReadTableFromFile(tableName)
	if err != nil {
		return 0, err
	}

	originalLength := len(table.Data)
	newData := make([][]interface{}, 0, originalLength)

	for _, row := range table.Data {
		shouldDelete := false
		if filter != nil {
			matches, err := filter(row, table.Metadata.Columns)
			if err != nil {
				return 0, err
			}
			shouldDelete = matches
		}

		if !shouldDelete {
			newData = append(newData, row)
		}
	}

	deletedCount := int64(originalLength - len(newData))
	if deletedCount > 0 {
		table.Data = newData
		err = o.serializer.WriteTableToFile(table, tableName)
		if err != nil {
			return 0, fmt.Errorf("failed to write updated table: %w", err)
		}
	}

	return deletedCount, nil
}
