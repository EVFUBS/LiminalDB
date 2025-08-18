package eval

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/database"
	ops "LiminalDb/internal/database/operations"
	"fmt"
)

func (e *Evaluator) selectData(tableName string, fields []string, where ast.Expression) (*database.QueryResult, error) {
	return e.operations.ReadRows(tableName, fields, e.filter(where), where)
}

func (e *Evaluator) insertData(tableName string, fields []string, values [][]ast.Expression) (any, error) {
	data := [][]any{}
	for _, value := range values {
		row := make([]any, len(fields))
		for i := range fields {
			if i < len(value) {
				row[i] = value[i].GetValue()
			} else {
				row[i] = nil
			}
		}
		data = append(data, row)
	}

	err := e.operations.WriteRows(tableName, data)
	if err != nil {
		return nil, fmt.Errorf("failed to write table: %w", err)
	}

	return "Insert successful", nil
}

func (e *Evaluator) deleteData(tableName string, where ast.Expression) (any, error) {
	filter := func(row []any, columns []database.Column) (bool, error) {
		if where == nil {
			return true, nil
		}
		matches, err := e.Evaluate(where, row, columns)
		if err != nil {
			return false, err
		}
		return matches.(bool), nil
	}

	deletedCount, err := e.operations.DeleteRows(tableName, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to delete rows: %w", err)
	}

	if deletedCount == 0 {
		return "No rows deleted", nil
	}
	return fmt.Sprintf("%d row(s) deleted", deletedCount), nil
}

func (e *Evaluator) dropTable(tableName string) (any, error) {
	result := e.operations.DropTable(&ops.Operation{TableName: tableName})
	if result.Err != nil {
		return nil, fmt.Errorf("failed to drop table: %w", result.Err)
	}

	return "Drop table successful", nil
}

func (e *Evaluator) describeTable(tableName string) (any, error) {
	metadata, err := e.operations.ReadMetadata(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	return metadata, nil
}
