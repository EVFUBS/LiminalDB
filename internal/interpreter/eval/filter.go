package eval

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/database"
)

func (e *Evaluator) filter(where ast.Expression) func(row []any, columns []database.Column) (bool, error) {
	return func(row []any, columns []database.Column) (bool, error) {
		if where == nil {
			return true, nil
		}

		matches, err := e.EvaluateValue(where, row, columns)
		if err != nil {
			return false, err
		}
		return matches.(bool), nil
	}
}
