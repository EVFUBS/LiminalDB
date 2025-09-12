package operations

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/database"
	"LiminalDb/internal/database/serializer"
	l "LiminalDb/internal/logger"
)

var logger *l.Logger

type Filter func([]any, []database.Column) (bool, error)

type Operation struct {
	TableName      string
	Fields         []string
	Data           Data
	Filter         Filter
	Where          ast.Expression
	IndexName      string
	Columns        []database.Column
	ColumnNames    []string
	IsUnique       bool
	ConstraintName string
	Metadata       database.TableMetadata
	Filename       string
}

type Data struct {
	Insert [][]any
	Update map[string]any
}

type Result struct {
	Data          *database.QueryResult
	Metadata      *database.TableMetadata
	IndexMetaData []database.IndexMetadata
	RowsAffected  int64
	Err           error
}

type Operations interface {
	CreateTable(op *Operation) *Result
	DropTable(op *Operation) *Result
	ReadMetadata(op *Operation) *Result
	WriteRows(op *Operation) *Result
	UpdateRows(op *Operation) *Result
	ReadRows(op *Operation) *Result
	DeleteRows(op *Operation) *Result
	CreateIndex(op *Operation) *Result
	DropIndex(op *Operation) *Result
	ListIndexes(op *Operation) *Result
	DropConstraint(op *Operation) *Result
	AddColumnsToTable(op *Operation) *Result
}

type OperationsImpl struct {
	Serializer serializer.BinarySerializer
}

func NewOperationsImpl() *OperationsImpl {
	logger = l.Get("sql")

	return &OperationsImpl{
		Serializer: *serializer.NewBinarySerializer(),
	}
}
