package operations

import (
	"LiminalDb/internal/database"
	"LiminalDb/internal/database/indexing"
	"fmt"
	"os"
)

func (o *OperationsImpl) DeleteRows(op *Operation) *Result {
	logger.Info("Deleting rows from table: %s", op.TableName)

	table, err := o.Serializer.ReadTableFromFile(op.TableName)
	if err != nil {
		return &Result{Err: err}
	}

	rowsToDelete, err := o.DetermineRowsToDelete(table, op.Filter)
	if err != nil {
		return &Result{Err: err}
	}

	err = o.deleteRowForeignKeyCheck(table, rowsToDelete)
	if err != nil {
		return &Result{Err: err}
	}

	indexes := make(map[string]*indexing.Index)
	for _, idx := range table.Metadata.Indexes {
		index, err := o.loadIndex(op.TableName, idx.Name)
		if err != nil {
			logger.Error("Failed to load index %s: %v", idx.Name, err)
			continue
		}
		indexes[idx.Name] = index
	}

	originalLength := len(table.Data)
	newData := make([][]any, 0, originalLength)
	newRowIDs := make(map[int]int)

	for i, row := range table.Data {
		if !rowsToDelete[i] {
			newRowIDs[i] = len(newData)
			newData = append(newData, row)
		} else {
			for idxName, idx := range indexes {
				for _, idxMeta := range table.Metadata.Indexes {
					if idxMeta.Name == idxName {
						key, err := o.extractIndexKeyFromRow(row, idxMeta.Columns, table.Metadata.Columns)
						if err != nil {
							logger.Error("Failed to extract index key: %v", err)
							continue
						}
						if err := idx.Tree.Delete(key, int64(i)); err != nil {
							logger.Error("Failed to delete from index %s: %v", idxName, err)
						}
						break
					}
				}
			}
		}
	}

	deletedCount := int64(originalLength - len(newData))
	if deletedCount > 0 {
		table.Data = newData

		for idxName, idx := range indexes {
			newIndex := indexing.NewIndex(idx.Name, op.TableName, idx.Columns, idx.IsUnique)

			for _, newRowID := range newRowIDs {
				for _, idxMeta := range table.Metadata.Indexes {
					if idxMeta.Name == idxName {
						key, err := o.extractIndexKeyFromRow(table.Data[newRowID], idxMeta.Columns, table.Metadata.Columns)
						if err != nil {
							logger.Error("Failed to extract index key: %v", err)
							continue
						}
						if err := newIndex.Tree.Insert(key, int64(newRowID)); err != nil {
							logger.Error("Failed to insert into new index %s: %v", idxName, err)
						}
						break
					}
				}
			}

			indexBytes, err := indexing.SerializeIndex(newIndex)
			if err != nil {
				logger.Error("Failed to serialize index %s: %v", idxName, err)
				continue
			}

			indexFilePath := getIndexFilePath(op.TableName, idxName)
			if err := os.WriteFile(indexFilePath, indexBytes, 0666); err != nil {
				logger.Error("Failed to write index file %s: %v", indexFilePath, err)
			}
		}

		err = o.Serializer.WriteTableToFile(table, op.TableName)
		if err != nil {
			return &Result{Err: fmt.Errorf("failed to write updated table: %w", err)}
		}
	}

	logger.Info("Successfully deleted %d rows from table %s", deletedCount, op.TableName)
	return &Result{RowsAffected: deletedCount}
}

func (o *OperationsImpl) DetermineRowsToDelete(table *database.Table, filter func([]any, []database.Column) (bool, error)) ([]bool, error) {
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
