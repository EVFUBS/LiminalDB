package operations

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/common"
	"LiminalDb/internal/database"
	"LiminalDb/internal/database/indexing"
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

func extractAssignment(expr ast.Expression) (colName string, val any, found bool) {
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

func (o *OperationsImpl) CreateIndex(op *Operation) *Result {
	logger.Info("Creating index %s on table %s", op.IndexName, op.TableName)

	table, err := o.Serializer.ReadTableFromPath(o.getWorkingTablePath(op, op.TableName))
	if err != nil {
		logger.Error("Failed to read table %s: %v", op.TableName, err)
		return &Result{Err: err}
	}

	for _, idx := range table.Metadata.Indexes {
		if idx.Name == op.IndexName {
			return &Result{Err: fmt.Errorf("index %s already exists on table %s", op.IndexName, op.TableName)}
		}
	}

	for _, col := range op.ColumnNames {
		found := false
		for _, tableCol := range table.Metadata.Columns {
			if tableCol.Name == col {
				found = true
				break
			}
		}
		if !found {
			return &Result{Err: fmt.Errorf("column %s not found in table %s", col, op.TableName)}
		}
	}

	isPrimary := false
	if len(op.Columns) == 1 {
		for _, col := range table.Metadata.Columns {
			if col.Name == op.ColumnNames[0] && col.IsPrimaryKey {
				isPrimary = true
				break
			}
		}
	}

	indexMetadata := database.IndexMetadata{
		Name:      op.IndexName,
		Columns:   op.ColumnNames,
		IsUnique:  op.IsUnique,
		IsPrimary: isPrimary,
	}

	table.Metadata.Indexes = append(table.Metadata.Indexes, indexMetadata)

	index := indexing.NewIndex(op.IndexName, op.TableName, op.ColumnNames, op.IsUnique)

	err = o.insertIndexIntoTree(table, index, op.ColumnNames)
	if err != nil {
		return &Result{Err: err}
	}

	indexBytes, err := indexing.SerializeIndex(index)
	if err != nil {
		logger.Error("Failed to serialize index %s: %v", op.IndexName, err)
		return &Result{Err: err}
	}

	if err := o.writeIndexWithShadow(op, indexBytes, op.TableName, op.IndexName); err != nil {
		logger.Error("Failed to write index file %s: %v", op.IndexName, err)
		return &Result{Err: err}
	}

	err = o.writeTableWithShadow(op, table, op.TableName)
	if err != nil {
		logger.Error("Failed to write table metadata %s: %v", op.TableName, err)
		return &Result{Err: err}
	}

	return &Result{}
}

func (o *OperationsImpl) DropIndex(op *Operation) *Result {
	logger.Info("Dropping index %s from table %s", op.IndexName, op.TableName)

	table, err := o.Serializer.ReadTableFromPath(o.getWorkingTablePath(op, op.TableName))
	if err != nil {
		logger.Error("Failed to read table %s: %v", op.TableName, err)
		return &Result{Err: err}
	}

	indexFound := false
	for i, idx := range table.Metadata.Indexes {
		if idx.Name == op.IndexName {
			if idx.IsPrimary {
				return &Result{Err: fmt.Errorf("cannot drop primary key index")}
			}

			table.Metadata.Indexes = append(table.Metadata.Indexes[:i], table.Metadata.Indexes[i+1:]...)
			indexFound = true
			break
		}
	}

	if !indexFound {
		return &Result{Err: fmt.Errorf("index %s not found on table %s", op.IndexName, op.TableName)}
	}

	// Delete the index file from the working path (shadow or real)
	workingIndexPath := o.getWorkingIndexPath(op, op.TableName, op.IndexName)
	if err := os.Remove(workingIndexPath); err != nil && !os.IsNotExist(err) {
		return &Result{Err: fmt.Errorf("failed to delete index file: %w", err)}
	}

	err = o.writeTableWithShadow(op, table, op.TableName)
	if err != nil {
		return &Result{Err: err}
	}

	return &Result{}
}

func (o *OperationsImpl) ListIndexes(op *Operation) *Result {
	logger.Debug("Listing indexes for table %s", op.TableName)

	table, err := o.Serializer.ReadTableFromPath(o.getWorkingTablePath(op, op.TableName))
	if err != nil {
		logger.Error("Failed to read table %s: %v", op.TableName, err)
		return &Result{Err: err}
	}

	return &Result{IndexMetaData: table.Metadata.Indexes}
}

func (o *OperationsImpl) loadIndex(op *Operation, tableName string, indexName string) (*indexing.Index, error) {
	workingIndexPath := o.getWorkingIndexPath(op, tableName, indexName)

	// Check if index file exists at the working path (shadow or real)
	if _, err := os.Stat(workingIndexPath); os.IsNotExist(err) {
		// Index file doesn't exist, rebuild from table data
		table, err := o.Serializer.ReadTableFromPath(o.getWorkingTablePath(op, tableName))
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

	// Load index from the working path
	indexBytes, err := os.ReadFile(workingIndexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read index file: %w", err)
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
