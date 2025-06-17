package operations

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/common"
	"LiminalDb/internal/database"
	"LiminalDb/internal/database/indexing"
	"LiminalDb/internal/logger"
	"fmt"
	"os"
)

type candidateIndex struct {
	index *database.IndexMetadata
	key   any
}

func (o *OperationsImpl) findBestIndexColumn(table *database.Table, where ast.Expression) (*database.IndexMetadata, any) {
	if where == nil {
		return nil, nil
	}

	colNameFromFilter, valFromFilter, foundEquality := extractAssignment(where)
	if !foundEquality {
		return nil, nil
	}

	var candidates []candidateIndex
	for i := range table.Metadata.Indexes {
		idx := &table.Metadata.Indexes[i]

		// Only consider single-column indexes that match the equality condition.
		// TODO: Extend to support composite indexes. This would involve checking if the 'where'
		// clause provides values for the leading columns of a composite index.
		if len(idx.Columns) == 1 && idx.Columns[0] == colNameFromFilter {
			candidates = append(candidates, candidateIndex{index: idx, key: valFromFilter})
		}
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	bestCandidate := findPrimaryIndex(candidates)
	if bestCandidate == nil {
		bestCandidate = findUniqueIndex(candidates)
	}

	if bestCandidate == nil && len(candidates) > 0 {
		bestCandidate = &candidates[0]
	}

	if bestCandidate != nil {
		return bestCandidate.index, bestCandidate.key
	}

	return nil, nil
}

func extractAssignments(where ast.Expression) (assignments map[string]any) {
	if where == nil {
		return nil
	}

	assignments = make(map[string]any)

	switch expr := where.(type) {
	case *ast.AssignmentExpression:
		if colName, val, found := extractAssignment(expr); found {
			assignments[colName] = val
		}
	case *ast.BinaryExpression:
		if common.LogicalOperators[expr.Op] {
			leftAssignments := extractAssignments(expr.Left)
			rightAssignments := extractAssignments(expr.Right)

			for k, v := range leftAssignments {
				assignments[k] = v
			}
			for k, v := range rightAssignments {
				assignments[k] = v
			}
		}
	}

	return assignments
}

func extractAssignment(expr ast.Expression) (colName string, val interface{}, found bool) {
	binExpr, ok := expr.(*ast.AssignmentExpression)
	if !ok || binExpr.Op != "=" {
		return "", nil, false
	}

	if leftIdent, okL := binExpr.Left.(*ast.Identifier); okL {
		if rightLit, okR := binExpr.Right.(ast.Expression); okR {
			return leftIdent.Value, rightLit.GetValue(), true
		}
	}

	if rightIdent, okR := binExpr.Right.(*ast.Identifier); okR {
		if leftLit, okL := binExpr.Left.(ast.Expression); okL {
			return rightIdent.Value, leftLit.GetValue(), true
		}
	}

	return "", nil, false
}

func findPrimaryIndex(candidates []candidateIndex) *candidateIndex {
	for i := range candidates {
		if candidates[i].index.IsPrimary {
			return &candidates[i]
		}
	}
	return nil
}

func findUniqueIndex(candidates []candidateIndex) *candidateIndex {
	for i := range candidates {
		if candidates[i].index.IsUnique {
			return &candidates[i]
		}
	}
	return nil
}

func (o *OperationsImpl) CreateIndex(tableName string, indexName string, columns []string, isUnique bool) error {
	logger.Info("Creating index %s on table %s", indexName, tableName)

	table, err := o.Serializer.ReadTableFromFile(tableName)
	if err != nil {
		logger.Error("Failed to read table %s: %v", tableName, err)
		return err
	}

	for _, idx := range table.Metadata.Indexes {
		if idx.Name == indexName {
			return fmt.Errorf("index %s already exists on table %s", indexName, tableName)
		}
	}

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

	isPrimary := false
	if len(columns) == 1 {
		for _, col := range table.Metadata.Columns {
			if col.Name == columns[0] && col.IsPrimaryKey {
				isPrimary = true
				break
			}
		}
	}

	indexMetadata := database.IndexMetadata{
		Name:      indexName,
		Columns:   columns,
		IsUnique:  isUnique,
		IsPrimary: isPrimary,
	}

	table.Metadata.Indexes = append(table.Metadata.Indexes, indexMetadata)

	index := indexing.NewIndex(indexName, tableName, columns, isUnique)

	err = o.insertIndexIntoTree(table, index, columns)
	if err != nil {
		return err
	}

	err = o.SaveIndexToFile(index, tableName, indexName)
	if err != nil {
		return err
	}

	return o.Serializer.WriteTableToFile(table, tableName)
}

func (o *OperationsImpl) SaveIndexToFile(index *indexing.Index, tableName string, indexName string) error {
	indexBytes, err := indexing.SerializeIndex(index)
	if err != nil {
		return err
	}

	indexFilePath := getIndexFilePath(tableName, indexName)
	if err := os.WriteFile(indexFilePath, indexBytes, 0666); err != nil {
		return err
	}

	return nil
}

func (o *OperationsImpl) DropIndex(tableName string, indexName string) error {
	logger.Info("Dropping index %s from table %s", indexName, tableName)

	table, err := o.Serializer.ReadTableFromFile(tableName)
	if err != nil {
		logger.Error("Failed to read table %s: %v", tableName, err)
		return err
	}

	indexFound := false
	for i, idx := range table.Metadata.Indexes {
		if idx.Name == indexName {
			if idx.IsPrimary {
				return fmt.Errorf("cannot drop primary key index")
			}

			table.Metadata.Indexes = append(table.Metadata.Indexes[:i], table.Metadata.Indexes[i+1:]...)
			indexFound = true
			break
		}
	}

	if !indexFound {
		return fmt.Errorf("index %s not found on table %s", indexName, tableName)
	}

	indexFilePath := getIndexFilePath(tableName, indexName)
	if err := os.Remove(indexFilePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return o.Serializer.WriteTableToFile(table, tableName)
}

func (o *OperationsImpl) ListIndexes(tableName string) ([]database.IndexMetadata, error) {
	logger.Debug("Listing indexes for table %s", tableName)

	table, err := o.Serializer.ReadTableFromFile(tableName)
	if err != nil {
		logger.Error("Failed to read table %s: %v", tableName, err)
		return nil, err
	}

	return table.Metadata.Indexes, nil
}

func (o *OperationsImpl) loadIndex(tableName string, indexName string) (*indexing.Index, error) {
	indexFilePath := getIndexFilePath(tableName, indexName)

	if _, err := os.Stat(indexFilePath); os.IsNotExist(err) {
		table, err := o.Serializer.ReadTableFromFile(tableName)
		if err != nil {
			return nil, err
		}

		var indexMetadata *database.IndexMetadata
		for _, idx := range table.Metadata.Indexes {
			if idx.Name == indexName {
				indexMetadata = &idx
				break
			}
		}

		if indexMetadata == nil {
			return nil, fmt.Errorf("index %s not found on table %s", indexName, tableName)
		}

		index := indexing.NewIndex(indexName, tableName, indexMetadata.Columns, indexMetadata.IsUnique)

		err = o.insertIndexIntoTree(table, index, indexMetadata.Columns)
		if err != nil {
			return nil, err
		}

		return index, nil
	}

	indexBytes, err := os.ReadFile(indexFilePath)
	if err != nil {
		return nil, err
	}

	index, err := indexing.DeserializeIndex(indexBytes)
	if err != nil {
		return nil, err
	}

	return index, nil
}

func (o *OperationsImpl) insertIndexIntoTree(table *database.Table, index *indexing.Index, columns []string) error {
	for rowID, row := range table.Data {
		key, err := o.extractIndexKeyFromRow(row, columns, table.Metadata.Columns)
		if err != nil {
			return err
		}

		if err := index.Tree.Insert(key, int64(rowID)); err != nil {
			return err
		}
	}
	return nil
}
