package sqlparser

import (
	"LiminalDb/internal/db"
	"fmt"
)

type Evaluator struct {
	parser *Parser
}

func NewEvaluator(parser *Parser) *Evaluator {
	return &Evaluator{parser: parser}
}

func (e *Evaluator) Execute(query string) (interface{}, error) {
	e.parser.lexer = NewLexer(query)
	e.parser.nextToken()
	e.parser.nextToken()

	stmt := e.parser.ParseStatement()
	if stmt == nil {
		return nil, fmt.Errorf("failed to parse query: %s", query)
	}

	switch stmt := stmt.(type) {
	case *SelectStatement:
		return e.executeSelect(stmt)
	default:
		return nil, fmt.Errorf("unsupported statement type")
	}
}

func (e *Evaluator) executeSelect(stmt *SelectStatement) (interface{}, error) {
	// For simplicity, let's assume we have a function fetchData that takes a table name and fields and returns the data
	data, err := e.fetchData(stmt.TableName, stmt.Fields, stmt.Where)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (e *Evaluator) Evaluate(expr Expression, row []interface{}, columns []db.Column) (interface{}, error) {
	switch expr := expr.(type) {
	case *Identifier:
		// Find the index of the column with the given name
		for i, col := range columns {
			if col.Name == expr.Value {
				return row[i], nil
			}
		}
		return nil, fmt.Errorf("column not found: %s", expr.Value)
	case *Literal:
		return expr.Value, nil
	case *WhereExpression:
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
			return left == right, nil
		case "!=":
			return left != right, nil
		case ">":
			return left.(int) > right.(int), nil
		case ">=":
			return left.(int) >= right.(int), nil
		case "<":
			return left.(int) < right.(int), nil
		case "<=":
			return left.(int) <= right.(int), nil
		default:
			return nil, fmt.Errorf("unsupported operator: %s", expr.Op)
		}
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

// Dummy fetchData function for demonstration purposes
func (e *Evaluator) fetchData(tableName string, fields []string, where Expression) (interface{}, error) {
	serializer := db.BinarySerializer{}

	table, err := serializer.ReadTableFromFile(tableName)
	if err != nil {
		return nil, err
	}

	filteredData := [][]interface{}{}

	for _, row := range table.Data {
		if where != nil {
			matches, err := e.Evaluate(where, row, table.Metadata.Columns)
			if err != nil {
				return nil, err
			}
			if !matches.(bool) {
				continue
			}
		}
		filteredData = append(filteredData, row)
	}
	return filteredData, nil
}
