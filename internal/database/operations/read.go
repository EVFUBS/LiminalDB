package operations

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/database"
	"LiminalDb/internal/database/indexing"
	"fmt"
	"strings"
)

type ReadRowRequest struct {
	TableName string
	Fields    []string
	Filter    func([]any, []database.Column) (bool, error)
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
	Op            *Operation
}

func (o *OperationsImpl) ReadMetadata(op *Operation) *Result {
	logger.Debug("Reading metadata for table: %s", op.TableName)

	table, err := o.Serializer.ReadTableFromPath(o.getWorkingTablePath(op, op.TableName))
	if err != nil {
		logger.Error("Failed to read metadata for table %s: %v", op.TableName, err)
		return &Result{Err: err}
	}

	logger.Debug("Successfully read metadata for table %s", op.TableName)
	return &Result{Metadata: &table.Metadata}
}

func (o *OperationsImpl) ReadRows(op *Operation) *Result {
	logger.Debug("Reading rows from table: %s", op.TableName)

	table, err := o.Serializer.ReadTableFromPath(o.getWorkingTablePath(op, op.TableName))
	if err != nil {
		logger.Error("Failed to read rows from table %s: %v", op.TableName, err)
		return &Result{Err: err}
	}

	// this needs to be unified
	columnsToUse := op.Fields
	if len(columnsToUse) == 0 {
		columnsToUse = op.ColumnNames
	}

	result := BuildResultWithFilteredColumns(columnsToUse, table.Metadata.Columns)

	indexResult, err := o.ReadRowsUsingIndex(&IndexQuery{
		Table:         table,
		TableName:     op.TableName,
		Fields:        columnsToUse,
		Result:        result,
		Filter:        op.Filter,
		Index:         nil,
		IndexMetaData: nil,
		IndexKey:      nil,
		Op:            op,
	}, op.Where)
	if err != nil {
		logger.Error("Failed to read rows using index: %v", err)
		return &Result{Err: err}
	}

	if indexResult != nil {
		return &Result{Data: indexResult}
	}

	logger.Debug("No suitable index found for query on table %s", op.TableName)

	result, err = o.ReadRowsFullScan(table, columnsToUse, op.Filter, result)
	if err != nil {
		logger.Error("Failed to perform full table scan: %v", err)
		return &Result{Err: err}
	}

	logger.Debug("Successfully read %d rows from table %s", len(result.Rows), op.TableName)
	return &Result{Data: result}
}

func (o *OperationsImpl) ReadRowsFullScan(table *database.Table, columns []string, filter Filter, result *database.QueryResult) (*database.QueryResult, error) {
	logger.Debug("Reading rows from table: %s", table.Metadata.Name)
	logger.Debug("Performing full table scan on table %s", table.Metadata.Name)
	for _, row := range table.Data {
		selectedRow, err := o.ReadRowFilterWithRequestedColumns(row, columns, table, filter)
		if err != nil {
			logger.Error("Failed to select row columns from table %s: %v", table.Metadata.Name, err)
			return nil, err
		}

		if selectedRow == nil {
			continue
		}

		result.Rows = append(result.Rows, selectedRow)
	}

	return result, nil
}

func (o *OperationsImpl) ReadRowsUsingIndex(indexQuery *IndexQuery, where ast.Expression) (*database.QueryResult, error) {
	logger.Debug("Finding best index for query on table %s", indexQuery.TableName)
	indexInfo, indexKey := o.findBestIndexColumn(indexQuery.Table, where)

	if indexInfo != nil && indexKey != nil {
		index, err := o.loadIndex(indexQuery.Op, indexQuery.TableName, indexInfo.Name)
		if err != nil {
			logger.Error("Failed to load index %s: %v", indexInfo.Name, err)
		} else {
			indexQuery.Index = index
			indexQuery.IndexMetaData = indexInfo
			indexQuery.IndexKey = indexKey

			result, err := o.findRowsByIndex(indexQuery)
			if err != nil {
				logger.Error("Failed to find rows using index %s: %v", indexInfo.Name, err)
			}

			if result != nil {
				return result, nil
			}
		}
	}

	return nil, nil
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

			selectedRow, err := o.ReadRowFilterWithRequestedColumns(row, indexQuery.Fields, indexQuery.Table, nil)
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

func (o *OperationsImpl) ReadRowFilterWithRequestedColumns(row []any, columns []string, table *database.Table, filter func([]any, []database.Column) (bool, error)) ([]any, error) {
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

func BuildResultWithFilteredColumns(columns []string, tableColumns []database.Column) *database.QueryResult {
	if isWildcard(columns) {
		return &database.QueryResult{
			Columns: tableColumns,
		}
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

	result := &database.QueryResult{
		Columns: filteredColumns,
	}

	return result
}
