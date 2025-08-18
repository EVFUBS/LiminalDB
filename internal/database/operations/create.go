package operations

import (
	"LiminalDb/internal/database"
	"LiminalDb/internal/database/indexing"
	"LiminalDb/internal/logger"
	"os"
)

func (o *OperationsImpl) CreateTable(op *Operation) *Result {
	metadata := op.Metadata
	logger.Info("Creating table: %s", metadata.Name)

	if metadata.Indexes == nil {
		metadata.Indexes = []database.IndexMetadata{}
	}

	var primaryKeyColumns []string
	for _, col := range metadata.Columns {
		if col.IsPrimaryKey {
			primaryKeyColumns = append(primaryKeyColumns, col.Name)
		}
	}

	if len(primaryKeyColumns) > 0 {
		pkIndexName := "pk_" + metadata.Name
		pkIndex := database.IndexMetadata{
			Name:      pkIndexName,
			Columns:   primaryKeyColumns,
			IsUnique:  true,
			IsPrimary: true,
		}
		metadata.Indexes = append(metadata.Indexes, pkIndex)
	}

	table := &database.Table{
		Header: database.FileHeader{
			Magic:   database.MagicNumber,
			Version: database.CurrentVersion,
		},
		Metadata: metadata,
		Data:     [][]any{},
	}

	err := o.Serializer.WriteTableToFile(table, metadata.Name)
	if err != nil {
		logger.Error("Failed to create table %s: %v", metadata.Name, err)
		return &Result{Err: err}
	}

	for _, idx := range metadata.Indexes {
		index := indexing.NewIndex(idx.Name, metadata.Name, idx.Columns, idx.IsUnique)

		indexBytes, err := indexing.SerializeIndex(index)
		if err != nil {
			logger.Error("Failed to serialize index %s: %v", idx.Name, err)
			return &Result{Err: err}
		}

		indexFilePath := getIndexFilePath(metadata.Name, idx.Name)
		if err := os.WriteFile(indexFilePath, indexBytes, 0666); err != nil {
			logger.Error("Failed to write index file %s: %v", indexFilePath, err)
			return &Result{Err: err}
		}

		logger.Info("Created index %s on table %s", idx.Name, metadata.Name)
	}

	logger.Info("Table %s created successfully", metadata.Name)
	return &Result{Data: &database.QueryResult{Rows: [][]any{}}}
}
