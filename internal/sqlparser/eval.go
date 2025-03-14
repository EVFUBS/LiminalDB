package sqlparser

import (
	"LiminalDb/internal/db"
	"fmt"
	"strings"
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
	case *InsertStatement:
		return e.executeInsert(stmt)
	case *CreateTableStatement:
		return e.executeCreateTable(stmt)
	default:
		return nil, fmt.Errorf("unsupported statement type")
	}
}

func (e *Evaluator) executeSelect(stmt *SelectStatement) (interface{}, error) {
	// For simplicity, let's assume we have a function fetchData that takes a table name and fields and returns the data
	data, err := e.selectData(stmt.TableName, stmt.Fields, stmt.Where)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (e *Evaluator) executeInsert(stmt *InsertStatement) (interface{}, error) {
	data, err := e.insertData(stmt.TableName, stmt.Columns, stmt.ValueLists)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (e *Evaluator) executeCreateTable(stmt *CreateTableStatement) (interface{}, error) {
	data, err := e.createTable(stmt.TableName, stmt.Columns)
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
	case *StringLiteral:
		return expr.Value, nil
	case *Int64Literal:
		return expr.Value, nil
	case *Float64Literal:
		return expr.Value, nil
	case *BooleanLiteral:
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

// Dummy selectData function for demonstration purposes
func (e *Evaluator) selectData(tableName string, fields []string, where Expression) (interface{}, error) {
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

			filteredRow := []interface{}{}
			for _, field := range fields {
				for index, col := range table.Metadata.Columns {
					if strings.EqualFold(col.Name, field) {
						filteredRow = append(filteredRow, row[index])
					}
				}
			}
			filteredData = append(filteredData, filteredRow)
		}
	}
	return filteredData, nil
}

func (e *Evaluator) insertData(tableName string, fields []string, values [][]Expression) (interface{}, error) {
	serializer := db.BinarySerializer{}

	fmt.Printf("Reading table: %s\n", tableName)
	table, err := serializer.ReadTableFromFile(tableName)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Current table data: %v\n", table.Data)

	for _, value := range values {
		row := make([]interface{}, len(fields))
		for i, _ := range fields {
			if i < len(value) {
				row[i] = value[i].GetValue()
			} else {
				row[i] = nil
			}
		}
		fmt.Printf("Adding new row: %v\n", row)
		table.Data = append(table.Data, row)
	}
	fmt.Printf("Table data after insert: %v\n", table.Data)

	fmt.Printf("Writing table back to file: %s\n", tableName)
	err = serializer.WriteTableToFile(table, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to write table: %w", err)
	}

	// Verify the write by reading it back
	fmt.Printf("Verifying write by reading table again\n")
	verifyTable, err := serializer.ReadTableFromFile(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to verify write: %w", err)
	}
	fmt.Printf("Table data after re-reading: %v\n", verifyTable.Data)

	return table, nil
}

func (e *Evaluator) createTable(tableName string, columns []ColumnDefinition) (interface{}, error) {
	serializer := db.BinarySerializer{}

	// Convert column definitions to db.Column format
	dbColumns := make([]db.Column, len(columns))
	for i, col := range columns {

		columnType, err := convertTokenTypeToColumnType(col.DataType)
		if err != nil {
			return nil, err
		}

		dbColumns[i] = db.Column{
			Name:       col.Name,
			DataType:   columnType,
			Length:     uint16(col.Length),
			IsNullable: col.Nullable,
		}
	}

	// Create new table structure
	table := db.Table{
		Header: db.FileHeader{
			Magic:   db.MagicNumber,
			Version: db.CurrentVersion,
		},
		Metadata: db.TableMetadata{
			Name:    tableName,
			Columns: dbColumns,
		},
		Data: [][]interface{}{},
	}

	// Write the new table to file
	err := serializer.WriteTableToFile(table, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return table, nil
}

func convertTokenTypeToColumnType(tokenType TokenType) (db.ColumnType, error) {
	switch tokenType {
	case INTTYPE:
		return db.TypeInteger64, nil
	case FLOATTYPE:
		return db.TypeFloat64, nil
	case STRINGTYPE:
		return db.TypeString, nil
	case BOOLTYPE:
		return db.TypeBoolean, nil
	}

	return 0, fmt.Errorf("unsupported token type: %s", tokenType)
}
