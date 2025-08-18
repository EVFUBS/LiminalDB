package operations

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/database"
	"LiminalDb/internal/database/serializer"
)

type Filter func([]any, []database.Column) (bool, error)

type Operation struct {
	TableName      string
	Fields         []string
	Data           [][]any
	Filter         Filter
	Where          ast.Expression
	IndexName      string
	Columns        []string
	IsUnique       bool
	ConstraintName string
	Metadata       database.TableMetadata
	UpdateData     map[string]any
	Filename       string
}

type Result struct {
	Data *database.QueryResult
	Err  error
}

type Operations interface {
	CreateTable(op *Operation) *Result
	DropTable(op *Operation) *Result
	ReadMetadata(op *Operation) *Result
	WriteRows(tableName string, data [][]any) error
	UpdateRows(tableName string, data map[string]any, filter Filter) error
	ReadRows(tableName string, fields []string, filter Filter, where ast.Expression) (*database.QueryResult, error)
	DeleteRows(tableName string, filter Filter) (int64, error)
	CreateIndex(tableName string, indexName string, columns []string, isUnique bool) error
	DropIndex(tableName string, indexName string) error
	ListIndexes(tableName string) ([]database.IndexMetadata, error)
	DropConstraint(tableName string, constraintName string) error
	AddColumnsToTable(tableName string, columns []database.Column) error
}

type OperationsImpl struct {
	Serializer serializer.BinarySerializer
}
