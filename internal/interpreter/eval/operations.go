package eval

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/common"
	"LiminalDb/internal/database"
	ops "LiminalDb/internal/database/operations"
)

func (e *Evaluator) insertData(tableName string, fields []string, values [][]ast.Expression) (*ops.Operation, error) {
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

	return &ops.Operation{TableName: tableName, Data: ops.Data{Insert: data}, ExecuteMethod: e.operations.WriteRows, Type: common.Insert}, nil
}

func (e *Evaluator) deleteData(tableName string, where ast.Expression) (*ops.Operation, error) {
	filter := func(row []any, columns []database.Column) (bool, error) {
		if where == nil {
			return true, nil
		}
		matches, err := e.EvaluateValue(where, row, columns)
		if err != nil {
			return false, err
		}
		return matches.(bool), nil
	}

	operation := &ops.Operation{TableName: tableName, Filter: filter, ExecuteMethod: e.operations.DeleteRows, Type: common.Delete}
	logger.Debug("Built DELETE operation with filter: %s", where)

	return operation, nil
}

func (e *Evaluator) dropTable(tableName string) (*ops.Operation, error) {
	operation := &ops.Operation{TableName: tableName, ExecuteMethod: e.operations.DropTable, Type: common.DropTable}
	logger.Debug("Built DROP TABLE operation for table: %s", tableName)
	return operation, nil
}
