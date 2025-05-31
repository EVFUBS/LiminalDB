package operations

import (
	"LiminalDb/internal/database"
	"LiminalDb/internal/logger"
	"fmt"
)

func (o *OperationsImpl) writeForeignKeyCheck(table *database.Table, newRow []interface{}) error {
	for _, foreignKey := range table.Metadata.ForeignKeys {

		refTable, err := o.Serializer.ReadTableFromFile(foreignKey.ReferencedTable)
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

func (o *OperationsImpl) deleteRowForeignKeyCheck(table *database.Table, rowsToDelete []bool) error {
	tables, err := o.Serializer.ListTables()
	if err != nil {
		return fmt.Errorf("failed to list tables for foreign key check: %w", err)
	}

	logger.Debug("Checking foreign key constraints for rows: %v", rowsToDelete)
	for _, otherTableName := range tables {
		if otherTableName == table.Metadata.Name {
			continue
		}

		otherTable, err := o.Serializer.ReadTableFromFile(otherTableName)
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
