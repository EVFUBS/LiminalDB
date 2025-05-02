package interpreter

import (
	"LiminalDb/internal/database"
	"LiminalDb/internal/logger"
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
	logger.Debug("Executing query: %s", query)

	e.parser.lexer = NewLexer(query)
	e.parser.nextToken()
	e.parser.nextToken()

	stmt, err := e.parser.ParseStatement()
	if err != nil || stmt == nil {
		logger.Error("Failed to parse query: %s with error: %s", query, err)
		return nil, fmt.Errorf("failed to parse query: %s", query)
	}

	result, err := e.executeStatement(stmt)
	if err != nil {
		logger.Error("Failed to execute statement: %v", err)
		return nil, err
	}

	logger.Debug("Query executed successfully")
	return result, nil
}

func (e *Evaluator) executeStatement(stmt Statement) (interface{}, error) {
	logger.Debug("Executing statement of type: %T", stmt)

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
		logger.Error("Unsupported statement type: %T", stmt)
		return nil, fmt.Errorf("unsupported statement type")
	}
}

func (e *Evaluator) executeSelect(stmt *SelectStatement) (*database.QueryResult, error) {
	logger.Debug("Executing SELECT statement on table: %s", stmt.TableName)

	data, err := e.selectData(stmt.TableName, stmt.Fields, stmt.Where)
	if err != nil {
		logger.Error("Failed to execute SELECT statement: %v", err)
		return nil, err
	}

	logger.Debug("SELECT statement executed successfully")
	return data, nil
}

func (e *Evaluator) executeInsert(stmt *InsertStatement) (interface{}, error) {
	logger.Debug("Executing INSERT statement on table: %s", stmt.TableName)

	data, err := e.insertData(stmt.TableName, stmt.Columns, stmt.ValueLists)
	if err != nil {
		logger.Error("Failed to execute INSERT statement: %v", err)
		return nil, err
	}

	logger.Debug("INSERT statement executed successfully")
	return data, nil
}

func (e *Evaluator) executeCreateTable(stmt *CreateTableStatement) (interface{}, error) {
	logger.Debug("Executing CREATE TABLE statement for table: %s", stmt.TableName)

	data, err := e.createTable(stmt.TableName, stmt.Columns)
	if err != nil {
		logger.Error("Failed to execute CREATE TABLE statement: %v", err)
		return nil, err
	}

	logger.Debug("CREATE TABLE statement executed successfully")
	return data, nil
}

func (e *Evaluator) executeDelete(stmt *DeleteStatement) (interface{}, error) {
	logger.Debug("Executing DELETE statement on table: %s", stmt.TableName)

	data, err := e.deleteData(stmt.TableName, stmt.Where)
	if err != nil {
		logger.Error("Failed to execute DELETE statement: %v", err)
		return nil, err
	}

	logger.Debug("DELETE statement executed successfully")
	return data, nil
}

func (e *Evaluator) executeDropTable(stmt *DropTableStatement) (interface{}, error) {
	logger.Debug("Executing DROP TABLE statement for table: %s", stmt.TableName)

	data, err := e.dropTable(stmt.TableName)
	if err != nil {
		logger.Error("Failed to execute DROP TABLE statement: %v", err)
		return nil, err
	}

	logger.Debug("DROP TABLE statement executed successfully")
	return data, nil
}

func (e *Evaluator) executeDescribeTable(stmt *DescribeTableStatement) (interface{}, error) {
	logger.Debug("Executing DESCRIBE TABLE statement for table: %s", stmt.TableName)

	data, err := e.describeTable(stmt.TableName)
	if err != nil {
		logger.Error("Failed to execute DESCRIBE TABLE statement: %v", err)
		return nil, err
	}

	logger.Debug("DESCRIBE TABLE statement executed successfully")
	return data, nil
}

func (e *Evaluator) executeCreateProcedure(stmt *CreateProcedureStatement) (interface{}, error) {
	logger.Debug("Executing CREATE PROCEDURE statement for procedure: %s", stmt.Name)

	proc := storedproc.NewStoredProc(
		stmt.Name,
		stmt.Body,
		stmt.Parameters,
		stmt.Description,
	)

	err := proc.WriteToFile(stmt.Name)
	if err != nil {
		logger.Error("Failed to create stored procedure: %v", err)
		return nil, fmt.Errorf("failed to create stored procedure: %w", err)
	}

	logger.Debug("CREATE PROCEDURE statement executed successfully")
	return "Stored procedure created successfully", nil
}

func (e *Evaluator) executeAlterProcedure(stmt *AlterProcedureStatement) (interface{}, error) {
	logger.Debug("Executing ALTER PROCEDURE statement for procedure: %s", stmt.Name)

	proc := storedproc.NewStoredProc(
		stmt.Name,
		stmt.Body,
		stmt.Parameters,
		stmt.Description,
	)

	err := proc.WriteToFile(stmt.Name)
	if err != nil {
		logger.Error("Failed to alter stored procedure: %v", err)
		return nil, fmt.Errorf("failed to alter stored procedure: %w", err)
	}

	logger.Debug("ALTER PROCEDURE statement executed successfully")
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

	// Split the body into individual statements
	statements := strings.Split(processedBody, ";")
	var lastResult interface{}
	var lastErr error

	// Execute each statement
	for _, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}

		e.parser.lexer = NewLexer(statement)
		e.parser.nextToken()
		e.parser.nextToken()

		stmt, err := e.parser.ParseStatement()
		if err != nil || stmt == nil {
			return nil, fmt.Errorf("failed to parse statement in stored procedure: %s", statement)
		}

		lastResult, lastErr = e.executeStatement(stmt)
		if lastErr != nil {
			return nil, fmt.Errorf("failed to execute statement in stored procedure: %w", lastErr)
		}
	}

	return lastResult, nil
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
		case ">":
			shouldReturn, result, err := greaterThanComparison(left, right)
			if shouldReturn {
				return result, err
			}
			return false, nil
		case ">=":
			shouldReturn, result, err := greaterThanOrEqualComparison(left, right)
			if shouldReturn {
				return result, err
			}
			return false, nil
		case "<":
			shouldReturn, result, err := lessThanComparison(left, right)
			if shouldReturn {
				return result, err
			}
			return false, nil
		case "<=":
			shouldReturn, result, err := lessThanOrEqualComparison(left, right)
			if shouldReturn {
				return result, err
			}
			return false, nil
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
