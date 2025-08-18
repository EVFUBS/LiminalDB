package eval

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/common"
	"fmt"
)

func convertToNumeric(left, right any) (float64, float64, error) {
	var leftNum, rightNum float64

	switch l := left.(type) {
	case int:
		leftNum = float64(l)
	case int32:
		leftNum = float64(l)
	case int64:
		leftNum = float64(l)
	case float32:
		leftNum = float64(l)
	case float64:
		leftNum = l
	default:
		return 0, 0, fmt.Errorf("left operand is not a number: %v (%T)", left, left)
	}

	switch r := right.(type) {
	case int:
		rightNum = float64(r)
	case int32:
		rightNum = float64(r)
	case int64:
		rightNum = float64(r)
	case float32:
		rightNum = float64(r)
	case float64:
		rightNum = r
	default:
		return 0, 0, fmt.Errorf("right operand is not a number: %v (%T)", right, right)
	}

	return leftNum, rightNum, nil
}

func tryNumericComparison(left, right any) (float64, float64, error) {
	return convertToNumeric(left, right)
}

func buildUpdateData(values []ast.Expression) (map[string]any, error) {
	data := make(map[string]any)
	for _, value := range values {
		valueExpression := value.(*ast.AssignmentExpression)
		if valueExpression.Op != common.ASSIGN {
			return nil, fmt.Errorf("unsupported operator: %s", valueExpression.Op)
		}
		data[valueExpression.Left.(*ast.Identifier).Value] = valueExpression.Right.GetValue()
	}

	return data, nil
}
