package operations

import (
	"LiminalDb/internal/database"
	"fmt"
	"strings"
)

// getIndexFilePath returns the file path for an index
func getIndexFilePath(tableName string, indexName string) string {
	return database.TableDir + "/" + tableName + "_" + indexName + ".idx"
}

// extractIndexKeyFromRow extracts the key for an index from a row
func (o *OperationsImpl) extractIndexKeyFromRow(row []any, indexColumns []string, tableColumns []database.Column) (any, error) {
	if len(indexColumns) == 1 {
		// Single column index
		for i, col := range tableColumns {
			if col.Name == indexColumns[0] {
				return row[i], nil
			}
		}
		return nil, fmt.Errorf("column %s not found", indexColumns[0])
	} else {
		// Composite index - create a string representation
		var keyParts []string
		for _, colName := range indexColumns {
			for i, col := range tableColumns {
				if col.Name == colName {
					keyParts = append(keyParts, fmt.Sprintf("%v", row[i]))
					break
				}
			}
		}
		return strings.Join(keyParts, "|"), nil
	}
}

// GetColumnIndex returns the index of a column in a table
func (o *OperationsImpl) GetColumnIndex(table *database.Table, columnName string) (int, error) {
	for idx, col := range table.Metadata.Columns {
		if col.Name == columnName {
			return idx, nil
		}
	}
	return -1, fmt.Errorf("column %s not found in table %s", columnName, table.Metadata.Name)
}

func (o *OperationsImpl) GetPrimaryKeyIndexFromMetadata(table *database.Table) (int, error) {
	for idx, col := range table.Metadata.Columns {
		if col.IsPrimaryKey {
			return idx, nil
		}
	}
	return -1, fmt.Errorf("no primary key found in table %s", table.Metadata.Name)
}
