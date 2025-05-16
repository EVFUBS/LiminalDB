package operations

import (
	"LiminalDb/internal/database"
)

func (o *OperationsImpl) UpdateRows(tableName string, data map[string]any, filter Filter) error {
	table, err := o.Serializer.ReadTableFromFile(tableName)
	if err != nil {
		return err
	}

	rows, err := rowsToUpdate(table, filter)
	if err != nil {
		return err
	}

	updatedRows, err := o.updateRows(table, rows, data)
	if err != nil {
		return err
	}

	err = o.UpdateTableWithRows(table, updatedRows)
	if err != nil {
		return err
	}

	// TODO: Update indexes after the index rework

	return nil
}

func rowsToUpdate(table *database.Table, filter Filter) ([][]any, error) {
	var filterRows [][]any
	for _, row := range table.Data {
		matches, err := filter(row, table.Metadata.Columns)
		if err != nil {
			return nil, err
		}
		if matches {
			filterRows = append(filterRows, row)
		}
	}

	return filterRows, nil
}

func (o *OperationsImpl) updateRows(table *database.Table, rows [][]any, data map[string]any) ([][]any, error) {
	for _, row := range rows {
		for colName, colValue := range data {
			colIndex, err := o.GetColumnIndex(table, colName)
			if err != nil {
				return nil, err
			}

			row[colIndex] = colValue
		}
	}

	return rows, nil
}

func (o *OperationsImpl) UpdateTableWithRows(table *database.Table, rows [][]any) error {
	primaryKeyIndex, err := o.GetPrimaryKeyIndexFromMetadata(table)
	if err != nil {
		return err
	}

	for _, tableRow := range table.Data {
		for i, row := range rows {
			if tableRow[primaryKeyIndex] == row[primaryKeyIndex] {
				table.Data[i] = row
				break
			}
		}
	}

	err = o.Serializer.WriteTableToFile(table, table.Metadata.Name)
	if err != nil {
		return err
	}

	return nil
}
