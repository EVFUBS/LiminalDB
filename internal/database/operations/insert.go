package operations

import (
	"LiminalDb/internal/database/indexing"
	"fmt"
	"strings"
)

func (o *OperationsImpl) WriteRows(op *Operation) *Result {
	logger.Info("Writing %d rows to table: %s", len(op.Data.Insert), op.TableName)

	table, err := o.Serializer.ReadTableFromPath(o.getWorkingTablePath(op, op.TableName))
	if err != nil {
		return &Result{Err: err}
	}

	logger.Debug("Checking primary key constraints for rows: %v", op.Data)
	for _, newRow := range op.Data.Insert {
		for _, existingRow := range table.Data {
			for i, col := range table.Metadata.Columns {
				if col.IsPrimaryKey {
					if existingRow[i] == newRow[i] {
						return &Result{
							Err: fmt.Errorf("primary key violation: duplicate value for column %s", col.Name),
						}
					}
				}
			}
		}

		logger.Debug("Checking foreign key constraints for row: %v", newRow)
		err := o.writeForeignKeyCheck(op, table, newRow)
		if err != nil {
			return &Result{Err: err}
		}
	}

	logger.Debug("Writing rows to table: %s", op.TableName)
	startRowID := len(table.Data)
	table.Data = append(table.Data, op.Data.Insert...)

	logger.Debug("Updating indexes for rows: %v", op.Data)
	for _, idx := range table.Metadata.Indexes {
		logger.Debug("Updating index %s", idx.Name)
		index, err := o.loadIndex(op, op.TableName, idx.Name)
		if err != nil {
			return &Result{Err: fmt.Errorf("failed to load index %s: %v", idx.Name, err)}
		}

		for i, row := range op.Data.Insert {
			rowID := int64(startRowID + i)
			key, err := o.extractIndexKeyFromRow(row, idx.Columns, table.Metadata.Columns)
			if err != nil {
				return &Result{Err: fmt.Errorf("failed to extract index key: %v", err)}
			}

			if idx.IsUnique {
				if values, found := index.Tree.Search(key); found && len(values) > 0 {
					colName := idx.Columns[0]
					if len(idx.Columns) > 1 {
						colName = strings.Join(idx.Columns, ", ")
					}
					return &Result{Err: fmt.Errorf("unique constraint violation: duplicate value for column(s) %s", colName)}
				}
			}

			if err := index.Tree.Insert(key, rowID); err != nil {
				return &Result{Err: fmt.Errorf("failed to insert index key: %v", err)}
			}
		}

		logger.Debug("Updated index %s", idx.Name)
		indexBytes, err := indexing.SerializeIndex(index)
		if err != nil {
			return &Result{Err: fmt.Errorf("failed to serialize index %s: %v", idx.Name, err)}
		}

		logger.Debug("Writing index %s to file", idx.Name)
		if err := o.writeIndexWithShadow(op, indexBytes, op.TableName, idx.Name); err != nil {
			return &Result{Err: fmt.Errorf("failed to write index %s to file: %v", idx.Name, err)}
		}
	}

	err = o.writeTableWithShadow(op, table, op.TableName)
	if err != nil {
		return &Result{Err: fmt.Errorf("failed to write table %s to file: %v", op.TableName, err)}
	}

	return &Result{Message: fmt.Sprintf("Successfully inserted %d rows into %s", len(op.Data.Insert), op.TableName)}
}
