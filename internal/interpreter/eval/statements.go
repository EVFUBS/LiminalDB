package eval

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/common"
	"LiminalDb/internal/database"
	ops "LiminalDb/internal/database/operations"
	"LiminalDb/internal/storedprocedure"
	"fmt"
)

func (e *Evaluator) evaluateStatement(stmt ast.Statement) (*[]ops.Operation, error) {
	logger.Debug("Executing statement of type: %T", stmt)

	switch stmt := stmt.(type) {
	case *ast.SelectStatement:
		return wrapOperationInArray(e.evaluateSelect(stmt)), nil
	case *ast.InsertStatement:
		return wrapOperationInArray(e.evaluateInsert(stmt)), nil
	case *ast.CreateTableStatement:
		return wrapOperationInArray(e.evaluateCreateTable(stmt)), nil
	case *ast.UpdateStatement:
		return wrapOperationInArray(e.evaluateUpdate(stmt)), nil
	case *ast.DeleteStatement:
		return wrapOperationInArray(e.evaluateDelete(stmt)), nil
	case *ast.DropTableStatement:
		return wrapOperationInArray(e.evaluateDropTable(stmt)), nil
	case *ast.DescribeTableStatement:
		return wrapOperationInArray(e.evaluateDescribeTable(stmt)), nil
	case *ast.CreateProcedureStatement:
		return wrapOperationInArray(e.executeCreateProcedure(stmt)), nil
	case *ast.AlterProcedureStatement:
		return wrapOperationInArray(e.executeAlterProcedure(stmt)), nil
	case *ast.ExecStatement:
		return wrapOperationInArray(e.executeStoredProcedure(stmt)), nil
	case *ast.CreateIndexStatement:
		return wrapOperationInArray(e.evaluateCreateIndex(stmt)), nil
	case *ast.DropIndexStatement:
		return wrapOperationInArray(e.evaluateDropIndex(stmt)), nil
	case *ast.ShowIndexesStatement:
		return wrapOperationInArray(e.evaluateShowIndexes(stmt)), nil
	case *ast.AlterTableStatement:
		return e.evaluateAlterTable(stmt)
	case *ast.TransactionStatement:
		return e.evaluateTransaction(stmt)
	default:
		logger.Error("Unsupported statement type: %T", stmt)
		return nil, fmt.Errorf("unsupported statement type")
	}
}

func wrapOperationInArray(op *ops.Operation, err error) *[]ops.Operation {
	if err != nil {
		return &[]ops.Operation{}
	}
	return &[]ops.Operation{*op}
}

func (e *Evaluator) evaluateSelect(stmt *ast.SelectStatement) (*ops.Operation, error) {
	logger.Debug("Built SELECT operation with fields: %s, where: %s", stmt.Fields, stmt.Where)
	operation := &ops.Operation{TableName: stmt.TableName, Fields: stmt.Fields, Where: stmt.Where, Filter: e.filter(stmt.Where), ExecuteMethod: e.operations.ReadRows, Type: common.Read}
	return operation, nil
}

func (e *Evaluator) evaluateUpdate(stmt *ast.UpdateStatement) (*ops.Operation, error) {
	logger.Debug("Evaluating Update statement for table: %s", stmt.TableName)
	data, err := buildUpdateData(stmt.Values)
	if err != nil {
		return nil, fmt.Errorf("failed to build update data: %w", err)
	}

	operation := &ops.Operation{TableName: stmt.TableName, Data: ops.Data{Update: data}, Filter: e.filter(stmt.Where), ExecuteMethod: e.operations.UpdateRows, Type: common.Alter}

	logger.Debug("Built UPDATE operation with fields: %s, where: %s", stmt.Values, stmt.Where)
	return operation, nil
}

func (e *Evaluator) evaluateInsert(stmt *ast.InsertStatement) (*ops.Operation, error) {
	logger.Debug("Evaluating INSERT statement on table: %s", stmt.TableName)
	operation, err := e.insertData(stmt.TableName, stmt.Columns, stmt.ValueLists)
	if operation != nil {
		operation.Type = common.Insert
	}
	return operation, err
}

func (e *Evaluator) evaluateCreateTable(stmt *ast.CreateTableStatement) (*ops.Operation, error) {
	logger.Debug("Evaluating CREATE TABLE statement for table: %s", stmt.TableName)

	operation, err := e.createTable(stmt)
	if err != nil {
		logger.Error("Failed to evaluate CREATE TABLE statement: %v", err)
		return nil, err
	}
	if operation != nil {
		operation.Type = common.CreateTable
	}
	return operation, nil
}

func (e *Evaluator) evaluateDelete(stmt *ast.DeleteStatement) (*ops.Operation, error) {
	logger.Debug("Evaluating DELETE statement on table: %s", stmt.TableName)

	operation, err := e.deleteData(stmt.TableName, stmt.Where)
	if err != nil {
		logger.Error("Failed to evaluate DELETE statement: %v", err)
		return nil, err
	}
	if operation != nil {
		operation.Type = common.Delete
	}
	return operation, nil
}

func (e *Evaluator) evaluateDropTable(stmt *ast.DropTableStatement) (*ops.Operation, error) {
	logger.Debug("Evaluating DROP TABLE statement for table: %s", stmt.TableName)

	operation, err := e.dropTable(stmt.TableName)
	if err != nil {
		logger.Error("Failed to evaluate DROP TABLE statement: %v", err)
		return nil, err
	}
	if operation != nil {
		operation.Type = common.DropTable
	}
	return operation, nil
}

func (e *Evaluator) evaluateDescribeTable(stmt *ast.DescribeTableStatement) (*ops.Operation, error) {
	logger.Debug("Evaluating DESCRIBE TABLE statement for table: %s", stmt.TableName)
	operation := &ops.Operation{TableName: stmt.TableName, ExecuteMethod: e.operations.ReadMetadata, Type: common.Read}
	return operation, nil
}

// TODO: sproc this needs to be done via by the db
func (e *Evaluator) executeCreateProcedure(stmt *ast.CreateProcedureStatement) (*ops.Operation, error) {
	logger.Debug("Executing CREATE PROCEDURE statement for procedure: %s", stmt.Name)

	procedure := storedprocedure.NewStoredProcedure(
		stmt.Name,
		stmt.Body,
		stmt.Parameters,
		stmt.Description,
	)

	operation := &ops.Operation{
		StoredProcedureOperation: &ops.StoredProcedureOperation{
			StoredProcedure:              procedure,
			StoredProcedureOperationType: ops.CreateStoredProcedure,
		},
		Type: common.CreateProcedure,
	}

	logger.Debug("CREATE PROCEDURE statement executed successfully")
	return operation, nil
}

func (e *Evaluator) executeAlterProcedure(stmt *ast.AlterProcedureStatement) (*ops.Operation, error) {
	logger.Debug("Executing ALTER PROCEDURE statement for procedure: %s", stmt.Name)

	procedure := storedprocedure.NewStoredProcedure(
		stmt.Name,
		stmt.Body,
		stmt.Parameters,
		stmt.Description,
	)

	operation := &ops.Operation{
		StoredProcedureOperation: &ops.StoredProcedureOperation{
			StoredProcedure:              procedure,
			StoredProcedureOperationType: ops.AlterStoredProcedure,
		},
		Type: common.AlterProcedure,
	}

	logger.Debug("ALTER PROCEDURE statement executed successfully")
	return operation, nil
}

func (e *Evaluator) executeStoredProcedure(stmt *ast.ExecStatement) (*ops.Operation, error) {
	procedure := &storedprocedure.StoredProcedure{
		Name: stmt.Name,
	}

	operation := &ops.Operation{
		StoredProcedureOperation: &ops.StoredProcedureOperation{
			StoredProcedure:              procedure,
			StoredProcedureOperationType: ops.ExecuteStoredProcedure,
		},
		Type: common.ExecuteProcedure,
	}
	return operation, nil
}

func (e *Evaluator) createTable(stmt *ast.CreateTableStatement) (*ops.Operation, error) {
	metadata := database.TableMetadata{
		Name:        stmt.TableName,
		Columns:     stmt.Columns,
		ForeignKeys: stmt.ForeignKeys,
	}

	operation := &ops.Operation{
		Metadata:      metadata,
		ExecuteMethod: e.operations.CreateTable,
		Type:          common.CreateTable,
	}

	logger.Debug("Built CREATE TABLE operation for table: %s", stmt.TableName)
	return operation, nil
}

func (e *Evaluator) evaluateCreateIndex(stmt *ast.CreateIndexStatement) (*ops.Operation, error) {
	logger.Debug("Built CREATE INDEX operation for index: %s on table: %s", stmt.IndexName, stmt.TableName)

	operation := &ops.Operation{
		TableName:     stmt.TableName,
		IndexName:     stmt.IndexName,
		ColumnNames:   stmt.Columns,
		IsUnique:      stmt.IsUnique,
		ExecuteMethod: e.operations.CreateIndex,
		Type:          common.CreateIndex,
	}

	return operation, nil
}

func (e *Evaluator) evaluateDropIndex(stmt *ast.DropIndexStatement) (*ops.Operation, error) {
	logger.Debug("Built DROP INDEX operation for index: %s on table: %s", stmt.IndexName, stmt.TableName)

	operation := &ops.Operation{
		TableName:     stmt.TableName,
		IndexName:     stmt.IndexName,
		ExecuteMethod: e.operations.DropIndex,
		Type:          common.DropIndex,
	}

	return operation, nil
}

func (e *Evaluator) evaluateShowIndexes(stmt *ast.ShowIndexesStatement) (*ops.Operation, error) {
	logger.Debug("Built SHOW INDEXES operation for table: %s", stmt.TableName)

	operation := &ops.Operation{
		TableName:     stmt.TableName,
		ExecuteMethod: e.operations.ListIndexes,
		Type:          common.Read,
	}

	return operation, nil
}

func (e *Evaluator) evaluateAlterTable(stmt *ast.AlterTableStatement) (*[]ops.Operation, error) {
	logger.Debug("Built ALTER TABLE operations for table: %s", stmt.TableName)

	var opsList []ops.Operation

	if stmt.DropConstraint {
		operation := ops.Operation{
			TableName:      stmt.TableName,
			ConstraintName: stmt.ConstraintName,
			ExecuteMethod:  e.operations.DropConstraint,
			Type:           common.Alter,
		}
		logger.Debug("Built DROP CONSTRAINT operation: %s on table: %s", stmt.ConstraintName, stmt.TableName)
		opsList = append(opsList, operation)
	}

	if stmt.AddColumn {
		operation := ops.Operation{
			TableName:     stmt.TableName,
			Columns:       stmt.Columns,
			ExecuteMethod: e.operations.AddColumnsToTable,
			Type:          common.Alter,
		}
		logger.Debug("Built ADD COLUMNS operation on table: %s", stmt.TableName)
		opsList = append(opsList, operation)
	}

	if len(opsList) == 0 {
		return nil, nil
	}

	return &opsList, nil
}

func (e *Evaluator) evaluateTransaction(stmt *ast.TransactionStatement) (*[]ops.Operation, error) {
	var opsList []ops.Operation

	for _, s := range stmt.Statements {
		if _, ok := s.(*ast.BeginStatement); ok {
			continue
		}

		if _, ok := s.(*ast.CommitStatement); ok {
			// Handle commit if needed
			continue
		}

		if _, ok := s.(*ast.RollbackStatement); ok {
			// Handle rollback if needed
			continue
		}

		operations, err := e.evaluateStatement(s)
		if err != nil {
			return nil, err
		}
		if operations != nil {
			for i := range *operations {
				(*operations)[i].Type = common.Transaction
				opsList = append(opsList, (*operations)[i])
			}
		}
	}

	return &opsList, nil
}
