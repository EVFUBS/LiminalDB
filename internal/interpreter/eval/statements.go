package eval

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/database"
	ops "LiminalDb/internal/database/operations"
	"LiminalDb/internal/database/transaction"
	"LiminalDb/internal/storedproc"
	"fmt"
	"strings"
)

func (e *Evaluator) executeStatement(stmt ast.Statement) (any, error) {
	logger.Debug("Executing statement of type: %T", stmt)

	switch stmt := stmt.(type) {
	case *ast.SelectStatement:
		return e.executeSelect(stmt)
	case *ast.InsertStatement:
		return e.executeInsert(stmt)
	case *ast.CreateTableStatement:
		return e.executeCreateTable(stmt)
	case *ast.UpdateStatement:
		return e.executeUpdate(stmt)
	case *ast.DeleteStatement:
		return e.executeDelete(stmt)
	case *ast.DropTableStatement:
		return e.executeDropTable(stmt)
	case *ast.DescribeTableStatement:
		return e.executeDescribeTable(stmt)
	case *ast.CreateProcedureStatement:
		return e.executeCreateProcedure(stmt)
	case *ast.AlterProcedureStatement:
		return e.executeAlterProcedure(stmt)
	case *ast.ExecStatement:
		return e.executeStoredProcedure(stmt)
	case *ast.CreateIndexStatement:
		return e.executeCreateIndex(stmt)
	case *ast.DropIndexStatement:
		return e.executeDropIndex(stmt)
	case *ast.ShowIndexesStatement:
		return e.executeShowIndexes(stmt)
	case *ast.AlterTableStatement:
		return e.executeAlterTable(stmt)
	case *ast.TransactionStatement:
		return e.executeTransaction(stmt)
	default:
		logger.Error("Unsupported statement type: %T", stmt)
		return nil, fmt.Errorf("unsupported statement type")
	}
}

func (e *Evaluator) executeSelect(stmt *ast.SelectStatement) (*database.QueryResult, error) {
	logger.Debug("Executing SELECT statement on table: %s", stmt.TableName)

	data, err := e.selectData(stmt.TableName, stmt.Fields, stmt.Where)
	if err != nil {
		logger.Error("Failed to execute SELECT statement: %v", err)
		return nil, err
	}

	logger.Debug("SELECT statement executed successfully")
	return data, nil
}

func (e *Evaluator) executeUpdate(stmt *ast.UpdateStatement) (any, error) {
	data, err := buildUpdateData(stmt.Values)
	if err != nil {
		return nil, fmt.Errorf("failed to build update data: %w", err)
	}

	logger.Debug("Executing UPDATE statement on table: %s", stmt.TableName)
	result := e.operations.UpdateRows(&ops.Operation{TableName: stmt.TableName, Data: ops.Data{Update: data}, Filter: e.filter(stmt.Where)})
	if result.Err != nil {
		return nil, fmt.Errorf("failed to update table: %w", result.Err)
	}

	return "Update successful", nil
}

func (e *Evaluator) executeInsert(stmt *ast.InsertStatement) (any, error) {
	logger.Debug("Executing INSERT statement on table: %s", stmt.TableName)

	data, err := e.insertData(stmt.TableName, stmt.Columns, stmt.ValueLists)
	if err != nil {
		logger.Error("Failed to execute INSERT statement: %v", err)
		return nil, err
	}

	logger.Debug("INSERT statement executed successfully")
	return data, nil
}

func (e *Evaluator) executeCreateTable(stmt *ast.CreateTableStatement) (any, error) {
	logger.Debug("Executing CREATE TABLE statement for table: %s", stmt.TableName)

	data, err := e.createTable(stmt)
	if err != nil {
		logger.Error("Failed to execute CREATE TABLE statement: %v", err)
		return nil, err
	}

	logger.Debug("CREATE TABLE statement executed successfully")
	return data, nil
}

func (e *Evaluator) executeDelete(stmt *ast.DeleteStatement) (any, error) {
	logger.Debug("Executing DELETE statement on table: %s", stmt.TableName)

	data, err := e.deleteData(stmt.TableName, stmt.Where)
	if err != nil {
		logger.Error("Failed to execute DELETE statement: %v", err)
		return nil, err
	}

	logger.Debug("DELETE statement executed successfully")
	return data, nil
}

func (e *Evaluator) executeDropTable(stmt *ast.DropTableStatement) (any, error) {
	logger.Debug("Executing DROP TABLE statement for table: %s", stmt.TableName)

	data, err := e.dropTable(stmt.TableName)
	if err != nil {
		logger.Error("Failed to execute DROP TABLE statement: %v", err)
		return nil, err
	}

	logger.Debug("DROP TABLE statement executed successfully")
	return data, nil
}

func (e *Evaluator) executeDescribeTable(stmt *ast.DescribeTableStatement) (any, error) {
	logger.Debug("Executing DESCRIBE TABLE statement for table: %s", stmt.TableName)

	data, err := e.describeTable(stmt.TableName)
	if err != nil {
		logger.Error("Failed to execute DESCRIBE TABLE statement: %v", err)
		return nil, err
	}

	logger.Debug("DESCRIBE TABLE statement executed successfully")
	return data, nil
}

func (e *Evaluator) executeCreateProcedure(stmt *ast.CreateProcedureStatement) (any, error) {
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

func (e *Evaluator) executeAlterProcedure(stmt *ast.AlterProcedureStatement) (any, error) {
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

func (e *Evaluator) executeStoredProcedure(stmt *ast.ExecStatement) (any, error) {
	proc := &storedproc.StoredProc{}
	err := proc.ReadFromFile(stmt.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to read stored procedure: %w", err)
	}

	if len(stmt.Parameters) != len(proc.Parameters) {
		return nil, fmt.Errorf("parameter count mismatch: expected %d, got %d",
			len(proc.Parameters), len(stmt.Parameters))
	}

	paramValues := make(map[string]any)
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
	var lastResult any
	var lastErr error

	// Execute each statement
	for _, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}

		e.parser.Lexer.SetInput(statement)
		e.parser.NextToken()
		e.parser.NextToken()

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

func (e *Evaluator) createTable(stmt *ast.CreateTableStatement) (any, error) {
	metadata := database.TableMetadata{
		Name:        stmt.TableName,
		Columns:     stmt.Columns,
		ForeignKeys: stmt.ForeignKeys,
	}

	operation := ops.Operation{
		Metadata: metadata,
	}

	result := e.operations.CreateTable(&operation)
	if result.Err != nil {
		return nil, fmt.Errorf("failed to create table: %w", result.Err)
	}

	return "Create table successful", nil
}

func (e *Evaluator) executeCreateIndex(stmt *ast.CreateIndexStatement) (any, error) {
	logger.Debug("Executing CREATE INDEX statement for index: %s on table: %s", stmt.IndexName, stmt.TableName)

	operation := &ops.Operation{
		TableName:   stmt.TableName,
		IndexName:   stmt.IndexName,
		ColumnNames: stmt.Columns,
		IsUnique:    stmt.IsUnique,
	}

	err := e.operations.CreateIndex(operation)

	if err != nil {
		logger.Error("Failed to execute CREATE INDEX statement: %v", err)
		return nil, fmt.Errorf("failed to create index: %w", err)
	}

	logger.Debug("CREATE INDEX statement executed successfully")
	return "Create index successful", nil
}

func (e *Evaluator) executeDropIndex(stmt *ast.DropIndexStatement) (any, error) {
	logger.Debug("Executing DROP INDEX statement for index: %s on table: %s", stmt.IndexName, stmt.TableName)

	operation := &ops.Operation{
		TableName: stmt.TableName,
		IndexName: stmt.IndexName,
	}

	result := e.operations.DropIndex(operation)
	if result.Err != nil {
		logger.Error("Failed to execute DROP INDEX statement: %v", result.Err)
		return nil, fmt.Errorf("failed to drop index: %w", result.Err)
	}

	logger.Debug("DROP INDEX statement executed successfully")
	return "Drop index successful", nil
}

func (e *Evaluator) executeShowIndexes(stmt *ast.ShowIndexesStatement) (any, error) {
	logger.Debug("Executing SHOW INDEXES statement for table: %s", stmt.TableName)

	operation := &ops.Operation{
		TableName: stmt.TableName,
	}

	result := e.operations.ListIndexes(operation)
	if result.Err != nil {
		logger.Error("Failed to execute SHOW INDEXES statement: %v", result.Err)
		return nil, fmt.Errorf("failed to list indexes: %w", result.Err)
	}

	logger.Debug("SHOW INDEXES statement executed successfully")
	return result.IndexMetaData, nil
}

func (e *Evaluator) executeAlterTable(stmt *ast.AlterTableStatement) (any, error) {
	logger.Debug("Executing ALTER TABLE statement for table: %s", stmt.TableName)

	if stmt.DropConstraint {
		operation := &ops.Operation{
			TableName:      stmt.TableName,
			ConstraintName: stmt.ConstraintName,
		}

		logger.Debug("Dropping constraint: %s from table: %s", stmt.ConstraintName, stmt.TableName)
		result := e.operations.DropConstraint(operation)

		if result.Err != nil {
			logger.Error("Failed to execute DROP CONSTRAINT statement: %v", result.Err)
			return nil, fmt.Errorf("failed to drop constraint: %w", result.Err)
		}
	}

	if stmt.AddColumn {
		operation := &ops.Operation{
			TableName: stmt.TableName,
			Columns:   stmt.Columns,
		}

		result := e.operations.AddColumnsToTable(operation)
		if result.Err != nil {
			logger.Error("Failed to execute ADD CONSTRAINT statement: %v", result.Err)
			return nil, fmt.Errorf("failed to add constraint: %w", result.Err)
		}
	}

	return nil, nil
}

func (e *Evaluator) executeTransaction(stmt *ast.TransactionStatement) (any, error) {
	tx := e.TransactionManager.Begin()

	for _, s := range stmt.Statements {
		var change transaction.Change
		if _, ok := s.(*ast.BeginStatement); ok {
			continue
		}

		if _, ok := s.(*ast.CommitStatement); ok {
			change = transaction.Change{Commit: true}
		}

		if _, ok := s.(*ast.RollbackStatement); ok {
			change = transaction.Change{Rollback: true}
		}

		if !change.Commit && !change.Rollback {
			op := buildOperationFromStatement(s)
			change = transaction.Change{Operation: op, Statement: s}
		}

		e.TransactionManager.AddChange(tx, change)
	}

	results, err := e.TransactionManager.Execute(tx, e.executeStatement)

	if err != nil {
		return nil, err
	}

	return results, nil
}
