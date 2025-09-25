package eval

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/database"
	c "LiminalDb/internal/interpreter/common"
	"fmt"
)

func (e *Evaluator) Evaluate(expr ast.Expression, row []any, columns []database.Column) (any, error) {
	switch expr := expr.(type) {
	case *ast.Identifier:
		for i, col := range columns {
			if col.Name == expr.Value {
				return row[i], nil
			}
		}
		return nil, fmt.Errorf("column not found: %s", expr.Value)
	case *ast.StringLiteral:
		return expr.Value, nil
	case *ast.Int64Literal:
		return expr.Value, nil
	case *ast.Float64Literal:
		return expr.Value, nil
	case *ast.BooleanLiteral:
		return expr.Value, nil
	case *ast.BinaryExpression:
		left, err := e.Evaluate(expr.Left, row, columns)
		if err != nil {
			return nil, err
		}
		right, err := e.Evaluate(expr.Right, row, columns)
		if err != nil {
			return nil, err
		}

		// Convert operands to numeric types if needed
		leftNum, rightNum, err := convertToNumeric(left, right)
		if err != nil {
			return nil, err
		}

		switch expr.Op {
		case "+":
			return leftNum + rightNum, nil
		case "-":
			return leftNum - rightNum, nil
		case "*":
			return leftNum * rightNum, nil
		case "/":
			if rightNum == 0 {
				return nil, fmt.Errorf("division by zero")
			}
			return leftNum / rightNum, nil
		default:
			return nil, fmt.Errorf("unsupported binary operator: %s", expr.Op)
		}
	case *ast.AssignmentExpression:
		left, err := e.Evaluate(expr.Left, row, columns)
		if err != nil {
			return nil, err
		}
		right, err := e.Evaluate(expr.Right, row, columns)
		if err != nil {
			return nil, err
		}
		switch expr.Op {
		case "=":
			// Try numeric comparison first
			leftNum, rightNum, err := tryNumericComparison(left, right)
			if err == nil {
				// Both values are numeric, compare them as float64
				return leftNum == rightNum, nil
			}
			// Fall back to direct comparison
			return left == right, nil
		case "!=":
			// Try numeric comparison first
			leftNum, rightNum, err := tryNumericComparison(left, right)
			if err == nil {
				// Both values are numeric, compare them as float64
				return leftNum != rightNum, nil
			}
			// Fall back to direct comparison
			return left != right, nil
		case ">":
			shouldReturn, result, err := c.GreaterThanComparison(left, right)
			if shouldReturn {
				return result, err
			}
			return false, nil
		case ">=":
			shouldReturn, result, err := c.GreaterThanOrEqualComparison(left, right)
			if shouldReturn {
				return result, err
			}
			return false, nil
		case "<":
			shouldReturn, result, err := c.LessThanComparison(left, right)
			if shouldReturn {
				return result, err
			}
			return false, nil
		case "<=":
			shouldReturn, result, err := c.LessThanOrEqualComparison(left, right)
			if shouldReturn {
				return result, err
			}
			return false, nil
		case "AND":
			return left.(bool) && right.(bool), nil
		case "OR":
			return left.(bool) || right.(bool), nil
		default:
			return nil, fmt.Errorf("unsupported operator: %s", expr.Op)
		}
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}
