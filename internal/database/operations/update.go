package operations

import (
	"LiminalDb/internal/database"
	"fmt"
)

func (o *OperationsImpl) UpdateRows(op *Operation) *Result {
	table, err := o.Serializer.ReadTableFromPath(o.getWorkingTablePath(op, op.TableName))
	if err != nil {
		return &Result{Err: err}
	}
	if table.File != nil {
		defer table.File.Close()
	}

	if err := o.LoadAllRows(table); err != nil {
		return &Result{Err: err}
	}

	rows, err := rowsToUpdate(table, op.Filter)
	if err != nil {
		return &Result{Err: err}
	}

	updatedRows, err := o.updateRows(table, rows, op.Data.Update)
	if err != nil {
		return &Result{Err: err}
	}

	err = o.UpdateTableWithRows(table, updatedRows, op)
	if err != nil {
		return &Result{Err: err}
	}

	// TODO: Update indexes after the index rework

	return &Result{Message: fmt.Sprintf("Successfully updated %d rows in %s", len(updatedRows), op.TableName)}
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

func (o *OperationsImpl) UpdateTableWithRows(table *database.Table, rows [][]any, op *Operation) error {
	primaryKeyIndex, err := o.GetPrimaryKeyIndex(table)
	if err != nil {
		return err
	}

	// For each table row, find matching updated row by primary key and replace
	for tIdx, tableRow := range table.Data {
		for _, row := range rows {
			if tableRow[primaryKeyIndex] == row[primaryKeyIndex] {
				table.Data[tIdx] = row
				break
			}
		}
	}

	err = o.writeTableWithShadow(op, table, table.Metadata.Name)
	if err != nil {
		return err
	}

	return nil
}
