package operations

import (
	"LiminalDb/internal/database"
	"fmt"
	"strings"
)

// extractIndexKeyFromRow extracts the key for an index from a row
func (o *OperationsImpl) extractIndexKeyFromRow(row []any, indexColumns []string, tableColumns []database.Column) (any, error) {
	if len(indexColumns) == 1 {
		for i, col := range tableColumns {
			if col.Name == indexColumns[0] {
				return row[i], nil
			}
		}
		return nil, fmt.Errorf("column %s not found", indexColumns[0])
	} else {
		var keyParts []string
		for _, colName := range indexColumns {
			found := false
			for i, col := range tableColumns {
				if col.Name == colName {
					keyParts = append(keyParts, fmt.Sprintf("%v", row[i]))
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column %s not found", colName)
			}
		}
		return strings.Join(keyParts, "|"), nil
	}
}

func (o *OperationsImpl) GetColumnIndex(table *database.Table, columnName string) (int, error) {
	for idx, col := range table.Metadata.Columns {
		if col.Name == columnName {
			return idx, nil
		}
	}
	return -1, fmt.Errorf("column %s not found in table %s", columnName, table.Metadata.Name)
}

// GetPrimaryKeyIndex returns the index of the primary key column in a table
func (o *OperationsImpl) GetPrimaryKeyIndex(table *database.Table) (int, error) {
	for idx, col := range table.Metadata.Columns {
		if col.IsPrimaryKey {
			return idx, nil
		}
	}
	return -1, fmt.Errorf("no primary key found in table %s", table.Metadata.Name)
}
