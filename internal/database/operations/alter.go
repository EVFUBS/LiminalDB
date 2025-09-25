package operations

import (
	"fmt"
)

func (o *OperationsImpl) AddColumnsToTable(op *Operation) *Result {
	table, err := o.Serializer.ReadTableFromFile(op.TableName)
	if err != nil {
		return &Result{Err: err}
	}

	for _, newCol := range op.Columns {
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
				return &Result{Err: fmt.Errorf("column %s requires a default value or must be nullable for non-empty table", newCol.Name)}
			}
		}

		table.Metadata.Columns = append(table.Metadata.Columns, newCol)
		for i := range table.Data {
			table.Data[i] = append(table.Data[i], newCol.DefaultValue)
		}
	}

	if err := o.Serializer.WriteTableToFile(table, op.TableName); err != nil {
		return &Result{Err: err}
	}

	return &Result{}
}
