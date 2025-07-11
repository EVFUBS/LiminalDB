package operations

import (
	"LiminalDb/internal/database/indexing"
	"LiminalDb/internal/logger"
	"fmt"
	"os"
	"strings"
)

func (o *OperationsImpl) WriteRows(tableName string, data [][]any) error {
	logger.Info("Writing %d rows to table: %s", len(data), tableName)

	table, err := o.Serializer.ReadTableFromFile(tableName)
	if err != nil {
		return err
	}

	logger.Debug("Checking primary key constraints for rows: %v", data)
	for _, newRow := range data {
		for _, existingRow := range table.Data {
			for i, col := range table.Metadata.Columns {
				if col.IsPrimaryKey {
					if existingRow[i] == newRow[i] {
						return fmt.Errorf("primary key violation: duplicate value for column %s", col.Name)
					}
				}
			}
		}

		logger.Debug("Checking foreign key constraints for row: %v", newRow)
		err := o.writeForeignKeyCheck(table, newRow)
		if err != nil {
			return err
		}
	}

	logger.Debug("Writing rows to table: %s", tableName)
	startRowID := len(table.Data)
	table.Data = append(table.Data, data...)

	logger.Debug("Updating indexes for rows: %v", data)
	for _, idx := range table.Metadata.Indexes {
		logger.Debug("Updating index %s", idx.Name)
		index, err := o.loadIndex(tableName, idx.Name)
		if err != nil {
			return err
		}

		for i, row := range data {
			rowID := int64(startRowID + i)
			key, err := o.extractIndexKeyFromRow(row, idx.Columns, table.Metadata.Columns)
			if err != nil {
				return err
			}

			if idx.IsUnique {
				if values, found := index.Tree.Search(key); found && len(values) > 0 {
					colName := idx.Columns[0]
					if len(idx.Columns) > 1 {
						colName = strings.Join(idx.Columns, ", ")
					}
					return fmt.Errorf("unique constraint violation: duplicate value for column(s) %s", colName)
				}
			}

			if err := index.Tree.Insert(key, rowID); err != nil {
				return err
			}
		}

		logger.Debug("Updated index %s", idx.Name)
		indexBytes, err := indexing.SerializeIndex(index)
		if err != nil {
			return err
		}

		logger.Debug("Writing index %s to file", idx.Name)
		indexFilePath := getIndexFilePath(tableName, idx.Name)
		if err := os.WriteFile(indexFilePath, indexBytes, 0666); err != nil {
			return err
		}
	}

	return o.Serializer.WriteTableToFile(table, tableName)
}
