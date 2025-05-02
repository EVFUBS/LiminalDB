package database

import (
	"LiminalDb/internal/logger"
	"fmt"
	"os"
	"strings"
)

const MagicNumber uint32 = 0x4D444247
const CurrentVersion uint16 = 1

type Operations interface {
	CreateTable(metadata TableMetadata) error
	DropTable(tableName string) error
	ReadMetadata(filename string) (TableMetadata, error)
	WriteRows(tableName string, data [][]interface{}) error
	ReadRows(tableName string, fields []string, filter func([]interface{}, []Column) (bool, error)) (*QueryResult, error)
	DeleteRows(tableName string, filter func([]interface{}, []Column) (bool, error)) (int64, error)
	CreateIndex(tableName string, indexName string, columns []string, isUnique bool) error
	DropIndex(tableName string, indexName string) error
	ListIndexes(tableName string) ([]IndexMetadata, error)
}

type OperationsImpl struct {
	serializer BinarySerializer
}

func (o *OperationsImpl) CreateTable(metadata TableMetadata) error {
	logger.Info("Creating table: %s", metadata.Name)

	// Initialize indexes array if not already initialized
	if metadata.Indexes == nil {
		metadata.Indexes = []IndexMetadata{}
	}

	// Find primary key columns
	var primaryKeyColumns []string
	for _, col := range metadata.Columns {
		if col.IsPrimaryKey {
			primaryKeyColumns = append(primaryKeyColumns, col.Name)
		}
	}

	// Create primary key index if primary key columns exist
	if len(primaryKeyColumns) > 0 {
		pkIndexName := "pk_" + metadata.Name
		pkIndex := IndexMetadata{
			Name:      pkIndexName,
			Columns:   primaryKeyColumns,
			IsUnique:  true,
			IsPrimary: true,
		}
		metadata.Indexes = append(metadata.Indexes, pkIndex)
	}

	table := Table{
		Header: FileHeader{
			Magic:   MagicNumber,
			Version: CurrentVersion,
		},
		Metadata: metadata,
		Data:     [][]interface{}{},
	}

	err := o.serializer.WriteTableToFile(table, metadata.Name)
	if err != nil {
		logger.Error("Failed to create table %s: %v", metadata.Name, err)
		return err
	}

	// Create B-tree indexes
	for _, idx := range metadata.Indexes {
		index := NewIndex(idx.Name, metadata.Name, idx.Columns, idx.IsUnique)

		// Save index to disk
		indexBytes, err := SerializeIndex(index)
		if err != nil {
			logger.Error("Failed to serialize index %s: %v", idx.Name, err)
			return err
		}

		indexFilePath := getIndexFilePath(metadata.Name, idx.Name)
		if err := os.WriteFile(indexFilePath, indexBytes, 0666); err != nil {
			logger.Error("Failed to write index file %s: %v", indexFilePath, err)
			return err
		}

		logger.Info("Created index %s on table %s", idx.Name, metadata.Name)
	}

	logger.Info("Table %s created successfully", metadata.Name)
	return nil
}

func (o *OperationsImpl) DropTable(tableName string) error {
	logger.Info("Dropping table: %s", tableName)

	// Read table metadata to get indexes
	table, err := o.serializer.ReadTableFromFile(tableName)
	if err != nil {
		// If the table doesn't exist, just return success
		if os.IsNotExist(err) {
			logger.Info("Table %s does not exist", tableName)
			return nil
		}
		logger.Error("Failed to read table %s: %v", tableName, err)
		return err
	}

	// Drop all indexes
	for _, idx := range table.Metadata.Indexes {
		indexFilePath := getIndexFilePath(tableName, idx.Name)
		if err := os.Remove(indexFilePath); err != nil && !os.IsNotExist(err) {
			logger.Error("Failed to remove index file %s: %v", indexFilePath, err)
			// Continue anyway to try to remove the table
		} else {
			logger.Info("Dropped index %s from table %s", idx.Name, tableName)
		}
	}

	// Drop the table file
	err = os.Remove(getTableFilePath(tableName))
	if err != nil {
		logger.Error("Failed to drop table %s: %v", tableName, err)
		return err
	}

	logger.Info("Table %s dropped successfully", tableName)
	return nil
}

func (o *OperationsImpl) ReadMetadata(filename string) (TableMetadata, error) {
	logger.Debug("Reading metadata for table: %s", filename)

	table, err := o.serializer.ReadTableFromFile(filename)
	if err != nil {
		logger.Error("Failed to read metadata for table %s: %v", filename, err)
		return TableMetadata{}, err
	}

	logger.Debug("Successfully read metadata for table %s", filename)
	return table.Metadata, nil
}

func (o *OperationsImpl) WriteRows(tableName string, data [][]interface{}) error {
	logger.Info("Writing %d rows to table: %s", len(data), tableName)

	table, err := o.serializer.ReadTableFromFile(tableName)
	if err != nil {
		return err
	}

	// Check for primary key violations and foreign key constraints
	for _, newRow := range data {
		// Check primary key constraints
		for _, existingRow := range table.Data {
			for i, col := range table.Metadata.Columns {
				if col.IsPrimaryKey {
					if existingRow[i] == newRow[i] {
						return fmt.Errorf("primary key violation: duplicate value for column %s", col.Name)
					}
				}
			}
		}

		// Check foreign key constraints
		err := o.writeForeignKeyCheck(table, newRow)
		if err != nil {
			return err
		}
	}

	// Add rows to table
	startRowID := len(table.Data)
	table.Data = append(table.Data, data...)

	// Update indexes
	for _, idx := range table.Metadata.Indexes {
		// Load index
		index, err := o.loadIndex(tableName, idx.Name)
		if err != nil {
			return err
		}

		// Add new rows to index
		for i, row := range data {
			rowID := int64(startRowID + i)
			key, err := o.extractIndexKey(row, idx.Columns, table.Metadata.Columns)
			if err != nil {
				return err
			}

			// Check for uniqueness constraint
			if idx.IsUnique {
				if values, found := index.Tree.Search(key); found && len(values) > 0 {
					// Find the column name for better error message
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

		// Save index
		indexBytes, err := SerializeIndex(index)
		if err != nil {
			return err
		}

		indexFilePath := getIndexFilePath(tableName, idx.Name)
		if err := os.WriteFile(indexFilePath, indexBytes, 0666); err != nil {
			return err
		}
	}

	// Save table
	return o.serializer.WriteTableToFile(table, tableName)
}

// TODO: can already see this implementation being crap for performance so circle back to this
func (o *OperationsImpl) writeForeignKeyCheck(table Table, newRow []interface{}) error {
	for _, foreignKey := range table.Metadata.ForeignKeys {

		refTable, err := o.serializer.ReadTableFromFile(foreignKey.ReferencedTable)
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

func (o *OperationsImpl) ReadRows(tableName string, fields []string, filter func([]interface{}, []Column) (bool, error)) (*QueryResult, error) {
	logger.Debug("Reading rows from table: %s", tableName)

	table, err := o.serializer.ReadTableFromFile(tableName)
	if err != nil {
		logger.Error("Failed to read rows from table %s: %v", tableName, err)
		return &QueryResult{}, err
	}

	result := &QueryResult{
		Columns: table.Metadata.Columns,
	}

	// Check if we can use an index for this query
	indexInfo, indexKey := o.findBestIndex(table, filter)
	if indexInfo != nil && indexKey != nil {
		logger.Debug("Using index %s for query on table %s", indexInfo.Name, tableName)

		// Load the index
		index, err := o.loadIndex(tableName, indexInfo.Name)
		if err != nil {
			logger.Error("Failed to load index %s: %v", indexInfo.Name, err)
			// Fall back to full table scan
		} else {
			// Search the index
			rowIDs, found := index.Tree.Search(indexKey)
			if found {
				// Process the matching rows
				for _, rowID := range rowIDs {
					if int(rowID) >= len(table.Data) {
						logger.Error("Invalid row ID %d in index %s", rowID, indexInfo.Name)
						continue
					}

					row := table.Data[rowID]

					// Double-check with the filter (in case of a partial index match)
					if filter != nil {
						matches, err := filter(row, table.Metadata.Columns)
						if err != nil {
							logger.Error("Filter error: %v", err)
							return nil, err
						}
						if !matches {
							continue
						}
					}

					selectedRow, err := o.selectRowFields(row, fields, table, nil)
					if err != nil {
						logger.Error("Failed to select row fields: %v", err)
						return nil, err
					}

					if selectedRow != nil {
						result.Rows = append(result.Rows, selectedRow)
					}
				}

				logger.Debug("Successfully read %d rows from table %s using index %s",
					len(result.Rows), tableName, indexInfo.Name)
				return result, nil
			}
		}
	}

	// Fall back to full table scan if no suitable index or index search failed
	logger.Debug("Performing full table scan on table %s", tableName)
	for _, row := range table.Data {
		selectedRow, err := o.selectRowFields(row, fields, table, filter)

		if err != nil {
			logger.Error("Failed to select row fields from table %s: %v", tableName, err)
			return nil, err
		}

		if selectedRow == nil {
			continue
		}

		result.Rows = append(result.Rows, selectedRow)
	}

	logger.Debug("Successfully read %d rows from table %s", len(result.Rows), tableName)
	return result, nil
}

// findBestIndex tries to find the best index for a given filter
// Returns the index metadata and the key to search for, or nil if no suitable index is found
func (o *OperationsImpl) findBestIndex(table Table, filter func([]interface{}, []Column) (bool, error)) (*IndexMetadata, interface{}) {
	// If no filter, we can't use an index
	if filter == nil {
		return nil, nil
	}

	// TODO: Implement a more sophisticated index selection algorithm
	// For now, we'll just check if the filter is a simple equality comparison on a single column

	// This is a very basic implementation that only works for simple filters
	// In a real database, we would need to analyze the filter expression and find the best index

	// For demonstration purposes, we'll just check if any index exists and use the first one
	if len(table.Metadata.Indexes) > 0 {
		// Prefer primary key index if available
		for _, idx := range table.Metadata.Indexes {
			if idx.IsPrimary {
				// For simplicity, we'll just return a dummy key that will match all rows
				// In a real implementation, we would extract the key from the filter
				return &idx, nil
			}
		}

		// Otherwise use the first index
		idx := table.Metadata.Indexes[0]
		return &idx, nil
	}

	return nil, nil
}

func (o *OperationsImpl) selectRowFields(row []interface{}, fields []string, table Table, filter func([]interface{}, []Column) (bool, error)) ([]interface{}, error) {
	if filter != nil {
		matches, err := filter(row, table.Metadata.Columns)
		if err != nil {
			return nil, err
		}
		if !matches {
			return nil, nil
		}
	}

	var selectedRow []interface{}
	if len(fields) == 0 || (len(fields) == 1 && fields[0] == "*") {
		selectedRow = make([]interface{}, len(row))
		copy(selectedRow, row)
	} else {
		selectedRow = []interface{}{}
		for _, field := range fields {
			found := false
			for index, col := range table.Metadata.Columns {
				if strings.EqualFold(col.Name, field) {
					selectedRow = append(selectedRow, row[index])
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column not found: %s", field)
			}
		}
	}

	return selectedRow, nil
}

func (o *OperationsImpl) DeleteRows(tableName string, filter func([]interface{}, []Column) (bool, error)) (int64, error) {
	logger.Info("Deleting rows from table: %s", tableName)

	table, err := o.serializer.ReadTableFromFile(tableName)
	if err != nil {
		return 0, err
	}

	rowsToDelete, err := o.DetermineRowsToDelete(table, filter)
	if err != nil {
		return 0, err
	}

	err = o.DeleteRowForeignKeyCheck(table, rowsToDelete)
	if err != nil {
		return 0, err
	}

	// Load all indexes
	indexes := make(map[string]*Index)
	for _, idx := range table.Metadata.Indexes {
		index, err := o.loadIndex(tableName, idx.Name)
		if err != nil {
			logger.Error("Failed to load index %s: %v", idx.Name, err)
			// Continue without this index
			continue
		}
		indexes[idx.Name] = index
	}

	originalLength := len(table.Data)
	newData := make([][]interface{}, 0, originalLength)
	newRowIDs := make(map[int]int) // Maps old row IDs to new row IDs

	// Remove rows from table data
	for i, row := range table.Data {
		if !rowsToDelete[i] {
			newRowIDs[i] = len(newData)
			newData = append(newData, row)
		} else {
			// Remove row from indexes
			for idxName, idx := range indexes {
				for _, idxMeta := range table.Metadata.Indexes {
					if idxMeta.Name == idxName {
						key, err := o.extractIndexKey(row, idxMeta.Columns, table.Metadata.Columns)
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
		// Update table data
		table.Data = newData

		// Update row IDs in indexes for rows that moved
		for idxName, idx := range indexes {
			// Create a new index with updated row IDs
			newIndex := NewIndex(idx.Name, tableName, idx.Columns, idx.IsUnique)

			// Rebuild the index with new row IDs
			for _, newRowID := range newRowIDs {
				for _, idxMeta := range table.Metadata.Indexes {
					if idxMeta.Name == idxName {
						key, err := o.extractIndexKey(table.Data[newRowID], idxMeta.Columns, table.Metadata.Columns)
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

			// Save the updated index
			indexBytes, err := SerializeIndex(newIndex)
			if err != nil {
				logger.Error("Failed to serialize index %s: %v", idxName, err)
				continue
			}

			indexFilePath := getIndexFilePath(tableName, idxName)
			if err := os.WriteFile(indexFilePath, indexBytes, 0666); err != nil {
				logger.Error("Failed to write index file %s: %v", indexFilePath, err)
			}
		}

		// Save the updated table
		err = o.serializer.WriteTableToFile(table, tableName)
		if err != nil {
			return 0, fmt.Errorf("failed to write updated table: %w", err)
		}
	}

	logger.Info("Successfully deleted %d rows from table %s", deletedCount, tableName)
	return deletedCount, nil
}

func (o *OperationsImpl) DetermineRowsToDelete(table Table, filter func([]interface{}, []Column) (bool, error)) ([]bool, error) {
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

func (o *OperationsImpl) DeleteRowForeignKeyCheck(table Table, rowsToDelete []bool) error {
	tables, err := o.serializer.ListTables()
	if err != nil {
		return fmt.Errorf("failed to list tables for foreign key check: %w", err)
	}

	for _, otherTableName := range tables {
		if otherTableName == table.Metadata.Name {
			continue
		}

		otherTable, err := o.serializer.ReadTableFromFile(otherTableName)
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

func (o *OperationsImpl) GetColumnIndex(table Table, columnName string) (int, error) {
	for idx, col := range table.Metadata.Columns {
		if col.Name == columnName {
			return idx, nil
		}
	}
	return -1, fmt.Errorf("column %s not found in table %s", columnName, table.Metadata.Name)
}

func (o *OperationsImpl) CreateIndex(tableName string, indexName string, columns []string, isUnique bool) error {
	logger.Info("Creating index %s on table %s", indexName, tableName)

	table, err := o.serializer.ReadTableFromFile(tableName)
	if err != nil {
		logger.Error("Failed to read table %s: %v", tableName, err)
		return err
	}

	// Check if index already exists
	for _, idx := range table.Metadata.Indexes {
		if idx.Name == indexName {
			return fmt.Errorf("index %s already exists on table %s", indexName, tableName)
		}
	}

	// Validate columns
	for _, col := range columns {
		found := false
		for _, tableCol := range table.Metadata.Columns {
			if tableCol.Name == col {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column %s not found in table %s", col, tableName)
		}
	}

	// Create index metadata
	isPrimary := false
	if len(columns) == 1 {
		for _, col := range table.Metadata.Columns {
			if col.Name == columns[0] && col.IsPrimaryKey {
				isPrimary = true
				break
			}
		}
	}

	indexMetadata := IndexMetadata{
		Name:      indexName,
		Columns:   columns,
		IsUnique:  isUnique,
		IsPrimary: isPrimary,
	}

	// Add index to table metadata
	table.Metadata.Indexes = append(table.Metadata.Indexes, indexMetadata)

	// Create B-tree index
	index := NewIndex(indexName, tableName, columns, isUnique)

	// Populate index with existing data
	for rowID, row := range table.Data {
		key, err := o.extractIndexKey(row, columns, table.Metadata.Columns)
		if err != nil {
			return err
		}

		if err := index.Tree.Insert(key, int64(rowID)); err != nil {
			return err
		}
	}

	// Save index to disk
	indexBytes, err := SerializeIndex(index)
	if err != nil {
		return err
	}

	// Save index file
	indexFilePath := getIndexFilePath(tableName, indexName)
	if err := os.WriteFile(indexFilePath, indexBytes, 0666); err != nil {
		return err
	}

	// Update table metadata
	return o.serializer.WriteTableToFile(table, tableName)
}

func (o *OperationsImpl) DropIndex(tableName string, indexName string) error {
	logger.Info("Dropping index %s from table %s", indexName, tableName)

	table, err := o.serializer.ReadTableFromFile(tableName)
	if err != nil {
		logger.Error("Failed to read table %s: %v", tableName, err)
		return err
	}

	// Find index
	indexFound := false
	for i, idx := range table.Metadata.Indexes {
		if idx.Name == indexName {
			// Check if it's a primary key index
			if idx.IsPrimary {
				return fmt.Errorf("cannot drop primary key index")
			}

			// Remove index from metadata
			table.Metadata.Indexes = append(table.Metadata.Indexes[:i], table.Metadata.Indexes[i+1:]...)
			indexFound = true
			break
		}
	}

	if !indexFound {
		return fmt.Errorf("index %s not found on table %s", indexName, tableName)
	}

	// Remove index file
	indexFilePath := getIndexFilePath(tableName, indexName)
	if err := os.Remove(indexFilePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Update table metadata
	return o.serializer.WriteTableToFile(table, tableName)
}

func (o *OperationsImpl) ListIndexes(tableName string) ([]IndexMetadata, error) {
	logger.Debug("Listing indexes for table %s", tableName)

	table, err := o.serializer.ReadTableFromFile(tableName)
	if err != nil {
		logger.Error("Failed to read table %s: %v", tableName, err)
		return nil, err
	}

	return table.Metadata.Indexes, nil
}

func (o *OperationsImpl) extractIndexKey(row []interface{}, indexColumns []string, tableColumns []Column) (interface{}, error) {
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

func getIndexFilePath(tableName string, indexName string) string {
	return DatabaseDir + "/" + tableName + "_" + indexName + ".idx"
}

func (o *OperationsImpl) loadIndex(tableName string, indexName string) (*Index, error) {
	indexFilePath := getIndexFilePath(tableName, indexName)

	// Check if index file exists
	if _, err := os.Stat(indexFilePath); os.IsNotExist(err) {
		// If not, create a new index
		table, err := o.serializer.ReadTableFromFile(tableName)
		if err != nil {
			return nil, err
		}

		// Find index metadata
		var indexMetadata *IndexMetadata
		for _, idx := range table.Metadata.Indexes {
			if idx.Name == indexName {
				indexMetadata = &idx
				break
			}
		}

		if indexMetadata == nil {
			return nil, fmt.Errorf("index %s not found on table %s", indexName, tableName)
		}

		// Create new index
		index := NewIndex(indexName, tableName, indexMetadata.Columns, indexMetadata.IsUnique)

		// Populate index with existing data
		for rowID, row := range table.Data {
			key, err := o.extractIndexKey(row, indexMetadata.Columns, table.Metadata.Columns)
			if err != nil {
				return nil, err
			}

			if err := index.Tree.Insert(key, int64(rowID)); err != nil {
				return nil, err
			}
		}

		return index, nil
	}

	// Read index file
	indexBytes, err := os.ReadFile(indexFilePath)
	if err != nil {
		return nil, err
	}

	// Deserialize index
	index, err := DeserializeIndex(indexBytes)
	if err != nil {
		return nil, err
	}

	return index, nil
}
