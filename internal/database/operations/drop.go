package operations

import (
	"LiminalDb/internal/database"
	"LiminalDb/internal/logger"
	"fmt"
	"os"
)

func (o *OperationsImpl) DropTable(tableName string) error {
	logger.Info("Dropping table: %s", tableName)

	table, err := o.Serializer.ReadTableFromFile(tableName)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Info("Table %s does not exist", tableName)
			return fmt.Errorf("table %s does not exist", tableName)
		}
		logger.Error("Failed to read table %s: %v", tableName, err)
		return err
	}

	for _, idx := range table.Metadata.Indexes {
		indexFilePath := getIndexFilePath(tableName, idx.Name)
		if err := os.Remove(indexFilePath); err != nil && !os.IsNotExist(err) {
			logger.Error("Failed to remove index file %s: %v", indexFilePath, err)
		} else {
			logger.Info("Dropped index %s from table %s", idx.Name, tableName)
		}
	}

	err = os.Remove(database.GetTableFilePath(tableName))
	if err != nil {
		logger.Error("Failed to drop table %s: %v", tableName, err)
		return err
	}

	logger.Info("Table %s dropped successfully", tableName)
	return nil
}

func (o *OperationsImpl) DropConstraint(tableName string, constraintName string) error {
	logger.Info("Dropping constraint: %s", constraintName)

	table, err := o.Serializer.ReadTableFromFile(tableName)
	if err != nil {
		logger.Error("Failed to read table %s: %v", tableName, err)
		return err
	}

	for i := len(table.Metadata.ForeignKeys) - 1; i >= 0; i-- {
		if table.Metadata.ForeignKeys[i].Name == constraintName {
			table.Metadata.ForeignKeys = append(table.Metadata.ForeignKeys[:i], table.Metadata.ForeignKeys[i+1:]...)
		}
	}

	if err := o.Serializer.WriteTableToFile(table, tableName); err != nil {
		logger.Error("Failed to save table after dropping constraint: %v", err)
		return err
	}

	logger.Info("Constraint %s dropped successfully from table %s", constraintName, tableName)
	return nil
}
