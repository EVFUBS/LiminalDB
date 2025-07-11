package operations

import (
	"LiminalDb/internal/database"
	"fmt"
)

func (o *OperationsImpl) AddColumnsToTable(tableName string, columns []database.Column) error {
	table, err := o.Serializer.ReadTableFromFile(tableName)
	if err != nil {
		return err
	}

	for _, newCol := range columns {
		if len(table.Data) == 0 {
			table.Metadata.Columns = append(table.Metadata.Columns, newCol)
			continue
		}

		if newCol.DefaultValue == nil {
			if newCol.IsNullable {
				table.Metadata.Columns = append(table.Metadata.Columns, newCol)
				for i := range table.Data {
					table.Data[i] = append(table.Data[i], nil)
				}
				continue
			} else {
				return fmt.Errorf("column %s requires a default value or must be nullable for non-empty table", newCol.Name)
			}
		}

		table.Metadata.Columns = append(table.Metadata.Columns, newCol)
		for i := range table.Data {
			table.Data[i] = append(table.Data[i], newCol.DefaultValue)
		}
	}

	if err := o.Serializer.WriteTableToFile(table, tableName); err != nil {
		return err
	}

	return nil
}
