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
