package sqlparser

import (
	"LiminalDb/internal/database"
	"LiminalDb/internal/storedproc"
	"fmt"
	"strings"
)

type Evaluator struct {
	parser     *Parser
	operations database.Operations
}

func NewEvaluator(parser *Parser) *Evaluator {
	return &Evaluator{
		parser:     parser,
		operations: &database.OperationsImpl{},
	}
}

func (e *Evaluator) Execute(query string) (interface{}, error) {
	e.parser.lexer = NewLexer(query)
	e.parser.nextToken()
	e.parser.nextToken()

	stmt := e.parser.ParseStatement()
	if stmt == nil {
		return nil, fmt.Errorf("failed to parse query: %s", query)
	}

	result, err := e.executeStatement(stmt)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (e *Evaluator) executeStatement(stmt Statement) (interface{}, error) {
	switch stmt := stmt.(type) {
	case *SelectStatement:
		return e.executeSelect(stmt)
	case *InsertStatement:
		return e.executeInsert(stmt)
	case *CreateTableStatement:
		return e.executeCreateTable(stmt)
	case *DeleteStatement:
		return e.executeDelete(stmt)
	case *DropTableStatement:
		return e.executeDropTable(stmt)
	case *DescribeTableStatement:
		return e.executeDescribeTable(stmt)
	case *CreateProcedureStatement:
		return e.executeCreateProcedure(stmt)
	case *AlterProcedureStatement:
		return e.executeAlterProcedure(stmt)
	case *ExecStatement:
		return e.executeStoredProcedure(stmt)
	default:
		return nil, fmt.Errorf("unsupported statement type")
	}
}

func (e *Evaluator) executeSelect(stmt *SelectStatement) (*database.QueryResult, error) {
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

func (e *Evaluator) executeDelete(stmt *DeleteStatement) (interface{}, error) {
	data, err := e.deleteData(stmt.TableName, stmt.Where)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (e *Evaluator) executeDropTable(stmt *DropTableStatement) (interface{}, error) {
	data, err := e.dropTable(stmt.TableName)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (e *Evaluator) executeDescribeTable(stmt *DescribeTableStatement) (interface{}, error) {
	data, err := e.describeTable(stmt.TableName)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (e *Evaluator) executeCreateProcedure(stmt *CreateProcedureStatement) (interface{}, error) {
	proc := storedproc.NewStoredProc(
		stmt.Name,
		stmt.Body,
		stmt.Parameters,
		stmt.Description,
	)

	err := proc.WriteToFile(stmt.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to create stored procedure: %w", err)
	}

	return "Stored procedure created successfully", nil
}

func (e *Evaluator) executeAlterProcedure(stmt *AlterProcedureStatement) (interface{}, error) {
	proc := storedproc.NewStoredProc(
		stmt.Name,
		stmt.Body,
		stmt.Parameters,
		stmt.Description,
	)

	err := proc.WriteToFile(stmt.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to alter stored procedure: %w", err)
	}

	return "Stored procedure altered successfully", nil
}

func (e *Evaluator) executeStoredProcedure(stmt *ExecStatement) (interface{}, error) {
	proc := &storedproc.StoredProc{}
	err := proc.ReadFromFile(stmt.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to read stored procedure: %w", err)
	}

	if len(stmt.Parameters) != len(proc.Parameters) {
		return nil, fmt.Errorf("parameter count mismatch: expected %d, got %d",
			len(proc.Parameters), len(stmt.Parameters))
	}

	paramValues := make(map[string]interface{})
	for i, param := range proc.Parameters {
		paramValues[param.Name] = stmt.Parameters[i].GetValue()
	}

	processedBody := proc.Body
	for name, value := range paramValues {
		var valueStr string
		switch v := value.(type) {
		case string:
			valueStr = "'" + v + "'"
		default:
			valueStr = fmt.Sprintf("%v", v)
		}

		processedBody = strings.Replace(processedBody, name, valueStr, -1)
	}

	proc.Body = processedBody
	result, err := e.Execute(proc.Body)

	if err != nil {
		return nil, fmt.Errorf("failed to execute stored procedure: %w", err)
	}

	return result, nil
}

func (e *Evaluator) Evaluate(expr Expression, row []interface{}, columns []database.Column) (interface{}, error) {
	switch expr := expr.(type) {
	case *Identifier:
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
		// TODO: THIS IS A HUGE PROBLEM FIX AS IT COULD BE FLOATS AS WELL FOR EXAMPLE
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

func (e *Evaluator) selectData(tableName string, fields []string, where Expression) (*database.QueryResult, error) {
	filter := func(row []interface{}, columns []database.Column) (bool, error) {
		if where == nil {
			return true, nil
		}
		matches, err := e.Evaluate(where, row, columns)
		if err != nil {
			return false, err
		}
		return matches.(bool), nil
	}

	return e.operations.ReadRows(tableName, fields, filter)
}

func (e *Evaluator) insertData(tableName string, fields []string, values [][]Expression) (interface{}, error) {
	data := [][]interface{}{}
	for _, value := range values {
		row := make([]interface{}, len(fields))
		for i, _ := range fields {
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

func (e *Evaluator) createTable(tableName string, columns []database.Column) (interface{}, error) {
	metadata := database.TableMetadata{
		Name:    tableName,
		Columns: columns,
	}

	err := e.operations.CreateTable(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return "Create table successful", nil
}

func (e *Evaluator) deleteData(tableName string, where Expression) (interface{}, error) {
	filter := func(row []interface{}, columns []database.Column) (bool, error) {
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

func (e *Evaluator) dropTable(tableName string) (interface{}, error) {
	err := e.operations.DropTable(tableName)

	if err != nil {
		return nil, fmt.Errorf("failed to drop table: %w", err)
	}

	return "Drop table successful", nil
}

func (e *Evaluator) describeTable(tableName string) (interface{}, error) {
	metadata, err := e.operations.ReadMetadata(tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}
	return metadata, nil
}

func convertTokenTypeToColumnType(tokenType TokenType) (database.ColumnType, error) {
	switch tokenType {
	case INTTYPE:
		return database.TypeInteger64, nil
	case FLOATTYPE:
		return database.TypeFloat64, nil
	case STRINGTYPE:
		return database.TypeString, nil
	case BOOLTYPE:
		return database.TypeBoolean, nil
	}

	return 0, fmt.Errorf("unsupported token type: %s", tokenType)
}
