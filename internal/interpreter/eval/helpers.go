package eval

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/common"
	"LiminalDb/internal/database"
	"LiminalDb/internal/database/operations"
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

func buildOperationFromStatement(stmt ast.Statement) operations.Operation {
	switch s := stmt.(type) {
	case *ast.SelectStatement:
		return operations.Operation{
			TableName: s.TableName,
			Fields:    s.Fields,
			Where:     s.Where,
		}
	case *ast.InsertStatement:
		var insertData [][]any
		for _, valueList := range s.ValueLists {
			row := make([]any, len(valueList))
			for i, expr := range valueList {
				row[i] = expr.GetValue()
			}
			insertData = append(insertData, row)
		}
		return operations.Operation{
			TableName: s.TableName,
			Fields:    s.Columns,
			Data:      operations.Data{Insert: insertData},
		}
	case *ast.UpdateStatement:
		data, _ := buildUpdateData(s.Values)
		return operations.Operation{
			TableName: s.TableName,
			Data:      operations.Data{Update: data},
			Where:     s.Where,
		}
	case *ast.DeleteStatement:
		return operations.Operation{
			TableName: s.TableName,
			Where:     s.Where,
		}
	case *ast.CreateTableStatement:
		return operations.Operation{
			Metadata: database.TableMetadata{
				Name:        s.TableName,
				Columns:     s.Columns,
				ForeignKeys: s.ForeignKeys,
			},
		}
	case *ast.DropTableStatement:
		return operations.Operation{
			TableName: s.TableName,
		}
	case *ast.CreateIndexStatement:
		return operations.Operation{
			TableName:   s.TableName,
			IndexName:   s.IndexName,
			ColumnNames: s.Columns,
			IsUnique:    s.IsUnique,
		}
	case *ast.DropIndexStatement:
		return operations.Operation{
			TableName: s.TableName,
			IndexName: s.IndexName,
		}
	case *ast.ShowIndexesStatement:
		return operations.Operation{
			TableName: s.TableName,
		}
	case *ast.AlterTableStatement:
		op := operations.Operation{
			TableName: s.TableName,
		}
		if s.DropConstraint {
			op.ConstraintName = s.ConstraintName
		}
		if s.AddColumn {
			op.Columns = s.Columns
		}
		return op
	default:
		return operations.Operation{}
	}
}
