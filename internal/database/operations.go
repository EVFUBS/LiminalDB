package database

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

	for _, newRow := range data {
		for _, existingRow := range table.Data {
			for i, col := range table.Metadata.Columns {
				if col.IsPrimaryKey {
					if existingRow[i] == newRow[i] {
						return fmt.Errorf("primary key violation: duplicate value for column %s", col.Name)
					}
				}
			}
		}

		err := o.writeForeignKeyCheck(table, newRow)
		if err != nil {
			return err
		}
	}

	table.Data = append(table.Data, data...)
	return o.serializer.WriteTableToFile(table, tableName)
}

// TODO: can already see this implementation being crap for performance so circle back to this
func (o *OperationsImpl) writeForeignKeyCheck(table Table, newRow []interface{}) error {
	for _, foreignKey := range table.Metadata.ForeignKeys {

		refTable, err := o.serializer.ReadTableFromFile(foreignKey.ReferencedTable)
		if err != nil {
			return fmt.Errorf("failed to read referenced table %s: %w", foreignKey.ReferencedTable, err)
		}

		for _, referencedColumns := range foreignKey.ReferencedColumns {

			referencedColumnExists := false
			for _, refCol := range refTable.Metadata.Columns {
				if refCol.Name == referencedColumns.ReferencedColumnName {
					referencedColumnExists = true
					break
				}
			}

			if !referencedColumnExists {
				return fmt.Errorf("referenced column %s not found in referenced table %s", referencedColumns, foreignKey.ReferencedTable)
			}

			sourceColumnIndex, err := o.GetColumnIndex(table, referencedColumns.ColumnName)
			if err != nil {
				return err
			}

			if sourceColumnIndex == -1 {
				return fmt.Errorf("column %s not found in table %s", referencedColumns.ColumnName, foreignKey.ReferencedTable)
			}

			refColumnIndex, err := o.GetColumnIndex(refTable, referencedColumns.ReferencedColumnName)
			if err != nil {
				return err
			}

			if refColumnIndex == -1 {
				return fmt.Errorf("referenced column %s not found in table %s",
					referencedColumns.ReferencedColumnName, foreignKey.ReferencedTable)
			}

			valueFound := false
			for _, refRow := range refTable.Data {
				if newRow[sourceColumnIndex] == refRow[refColumnIndex] {
					valueFound = true
					break
				}
			}

			if !valueFound {
				return fmt.Errorf("foreign key violation: value %v in column %s not found in referenced table %s column %s",
					newRow[sourceColumnIndex], referencedColumns.ColumnName,
					foreignKey.ReferencedTable, referencedColumns.ReferencedColumnName)
			}
		}
	}

	return nil
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
		selectedRow, err := o.selectRowFields(row, fields, table, filter)

		if err != nil {
			return nil, err
		}

		if selectedRow == nil {
			continue
		}

		result.Rows = append(result.Rows, selectedRow)
	}

	return result, nil
}

func (o *OperationsImpl) selectRowFields(row []interface{}, fields []string, table Table, filter func([]interface{}, []Column) (bool, error)) ([]interface{}, error) {
	if filter != nil {
		matches, err := filter(row, table.Metadata.Columns)
		if err != nil {
			return nil, err
		}
		if !matches {
			return nil, nil
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

	return selectedRow, nil
}

func (o *OperationsImpl) DeleteRows(tableName string, filter func([]interface{}, []Column) (bool, error)) (int64, error) {
	table, err := o.serializer.ReadTableFromFile(tableName)
	if err != nil {
		return 0, err
	}

	rowsToDelete, err := o.DetermineRowsToDelete(table, filter)
	if err != nil {
		return 0, err
	}

	err = o.DeleteRowForeignKeyCheck(table, rowsToDelete)
	if err != nil {
		return 0, err
	}

	originalLength := len(table.Data)
	newData := make([][]interface{}, 0, originalLength)

	for i, row := range table.Data {
		if !rowsToDelete[i] {
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

func (o *OperationsImpl) DetermineRowsToDelete(table Table, filter func([]interface{}, []Column) (bool, error)) ([]bool, error) {
	rowsToDelete := make([]bool, len(table.Data))
	for i, row := range table.Data {
		if filter != nil {
			matches, err := filter(row, table.Metadata.Columns)
			if err != nil {
				return nil, err
			}
			rowsToDelete[i] = matches
		}
	}
	return rowsToDelete, nil
}

func (o *OperationsImpl) DeleteRowForeignKeyCheck(table Table, rowsToDelete []bool) error {
	tables, err := o.serializer.ListTables()
	if err != nil {
		return fmt.Errorf("failed to list tables for foreign key check: %w", err)
	}

	for _, otherTableName := range tables {
		if otherTableName == table.Metadata.Name {
			continue
		}

		otherTable, err := o.serializer.ReadTableFromFile(otherTableName)
		if err != nil {
			return fmt.Errorf("failed to read table %s for foreign key check: %w", otherTableName, err)
		}

		for _, fk := range otherTable.Metadata.ForeignKeys {
			if fk.ReferencedTable == table.Metadata.Name {
				for i, row := range table.Data {
					if !rowsToDelete[i] {
						continue
					}

					for _, ref := range fk.ReferencedColumns {
						referencedColIndex, err := o.GetColumnIndex(table, ref.ReferencedColumnName)
						if err != nil {
							return fmt.Errorf("failed to find referenced column: %w", err)
						}

						valueToDelete := row[referencedColIndex]

						for _, otherRow := range otherTable.Data {
							otherColIndex, err := o.GetColumnIndex(otherTable, ref.ColumnName)
							if err != nil {
								return fmt.Errorf("failed to find referencing column: %w", err)
							}

							if otherRow[otherColIndex] == valueToDelete {
								return fmt.Errorf("foreign key constraint violation: cannot delete row from %s because it is referenced in table %s",
									table.Metadata.Name, otherTableName)
							}
						}
					}
				}
			}
		}
	}

	return nil
}

func (o *OperationsImpl) GetColumnIndex(table Table, columnName string) (int, error) {
	for idx, col := range table.Metadata.Columns {
		if col.Name == columnName {
			return idx, nil
		}
	}
	return -1, fmt.Errorf("column %s not found in table %s", columnName, table.Metadata.Name)
}
