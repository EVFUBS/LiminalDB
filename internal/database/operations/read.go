package operations

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/database"
	"LiminalDb/internal/database/indexing"
	"LiminalDb/internal/logger"
	"fmt"
	"strings"
)

type ReadRowRequest struct {
	TableName string
	Fields    []string
	Filter    func([]interface{}, []database.Column) (bool, error)
	Where     ast.Expression
}

type IndexQuery struct {
	Table         *database.Table
	TableName     string
	Fields        []string
	Result        *database.QueryResult
	Filter        func([]any, []database.Column) (bool, error)
	Index         *indexing.Index
	IndexMetaData *database.IndexMetadata
	IndexKey      any
}

func (o *OperationsImpl) ReadMetadata(filename string) (database.TableMetadata, error) {
	logger.Debug("Reading metadata for table: %s", filename)

	table, err := o.Serializer.ReadTableFromFile(filename)
	if err != nil {
		logger.Error("Failed to read metadata for table %s: %v", filename, err)
		return database.TableMetadata{}, err
	}

	logger.Debug("Successfully read metadata for table %s", filename)
	return table.Metadata, nil
}

func (o *OperationsImpl) ReadRows(tableName string, columns []string, filter func([]interface{}, []database.Column) (bool, error), where ast.Expression) (*database.QueryResult, error) {
	logger.Debug("Reading rows from table: %s", tableName)

	table, err := o.Serializer.ReadTableFromFile(tableName)
	if err != nil {
		logger.Error("Failed to read rows from table %s: %v", tableName, err)
		return &database.QueryResult{}, err
	}

	filteredColumns := filterColumns(columns, table.Metadata.Columns)

	result := &database.QueryResult{
		Columns: filteredColumns,
	}

	indexInfo, indexKey := o.findBestIndex(table, filter, where)
	if indexInfo != nil && indexKey != nil {
		logger.Debug("Using index %s for query on table %s", indexInfo.Name, tableName)

		index, err := o.loadIndex(tableName, indexInfo.Name)
		if err != nil {
			logger.Error("Failed to load index %s: %v", indexInfo.Name, err)
		} else {
			result, err = o.findRowsByIndex(&IndexQuery{
				Table:         table,
				TableName:     tableName,
				Fields:        columns,
				Result:        result,
				Filter:        filter,
				Index:         index,
				IndexMetaData: indexInfo,
				IndexKey:      indexKey,
			})

			if err != nil {
				logger.Error("Failed to find rows using index %s: %v", indexInfo.Name, err)
			}

			if result != nil {
				return result, nil
			}
		}
	}

	logger.Debug("Performing full table scan on table %s", tableName)
	for _, row := range table.Data {
		selectedRow, err := o.selectRowColumns(row, columns, table, filter)

		if err != nil {
			logger.Error("Failed to select row columns from table %s: %v", tableName, err)
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

func (o *OperationsImpl) findRowsByIndex(indexQuery *IndexQuery) (*database.QueryResult, error) {
	logger.Debug("Searching index %s for query on table %s", indexQuery.IndexMetaData.Name, indexQuery.TableName)
	rowIDs, found := indexQuery.Index.Tree.Search(indexQuery.IndexKey)
	if found {
		for _, rowID := range rowIDs {
			if int(rowID) >= len(indexQuery.Table.Data) {
				logger.Error("Invalid row ID %d in index %s", rowID, indexQuery.IndexMetaData.Name)
				continue
			}
			row := indexQuery.Table.Data[rowID]

			if indexQuery.Filter != nil {
				matches, err := indexQuery.Filter(row, indexQuery.Table.Metadata.Columns)
				if err != nil {
					logger.Error("Filter error: %v", err)
					return nil, err
				}
				if !matches {
					continue
				}
			}

			selectedRow, err := o.selectRowColumns(row, indexQuery.Fields, indexQuery.Table, nil)
			if err != nil {
				logger.Error("Failed to select row fields: %v", err)
				return nil, err
			}

			if selectedRow != nil {
				indexQuery.Result.Rows = append(indexQuery.Result.Rows, selectedRow)
			}
		}

		logger.Debug("Successfully read %d rows from table %s using index %s",
			len(indexQuery.Result.Rows), indexQuery.TableName, indexQuery.IndexMetaData.Name)
		return indexQuery.Result, nil
	}

	return nil, nil
}

func (o *OperationsImpl) selectRowColumns(row []any, columns []string, table *database.Table, filter func([]any, []database.Column) (bool, error)) ([]any, error) {
	if filter != nil {
		matches, err := filter(row, table.Metadata.Columns)
		if err != nil {
			return nil, err
		}
		if !matches {
			return nil, nil
		}
	}

	if len(columns) == 0 || isWildcard(columns) {
		selectedRow := make([]any, len(row))
		copy(selectedRow, row)
		return selectedRow, nil
	}

	selectedRow := make([]any, 0, len(columns))
	columnMap := buildColumnMap(table.Metadata.Columns)

	for _, field := range columns {
		index, exists := columnMap[strings.ToLower(field)]
		if !exists {
			return nil, fmt.Errorf("column not found: %s", field)
		}
		selectedRow = append(selectedRow, row[index])
	}

	return selectedRow, nil
}

func isWildcard(fields []string) bool {
	return len(fields) == 1 && fields[0] == "*"
}

func buildColumnMap(columns []database.Column) map[string]int {
	columnMap := make(map[string]int, len(columns))
	for i, col := range columns {
		columnMap[strings.ToLower(col.Name)] = i
	}
	return columnMap
}

func filterColumns(columns []string, tableColumns []database.Column) []database.Column {
	if isWildcard(columns) {
		return tableColumns
	}

	columnMap := make(map[string]struct{})
	for _, field := range columns {
		columnMap[strings.ToLower(field)] = struct{}{}
	}

	var filteredColumns []database.Column
	for _, col := range tableColumns {
		if _, ok := columnMap[strings.ToLower(col.Name)]; ok {
			filteredColumns = append(filteredColumns, col)
		}
	}
	return filteredColumns
}
