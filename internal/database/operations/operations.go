package operations

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/database"
	"LiminalDb/internal/database/serializer"
	l "LiminalDb/internal/logger"
	"LiminalDb/internal/storedprocedure"
    "LiminalDb/internal/common"
)

var logger *l.Logger

type Filter func([]any, []database.Column) (bool, error)

type Operation struct {
	ExecuteMethod            func(*Operation) *Result
	TableName                string
	Fields                   []string
	Data                     Data
	Filter                   Filter
	Where                    ast.Expression
	IndexName                string
	Columns                  []database.Column
	ColumnNames              []string
	IsUnique                 bool
	ConstraintName           string
	Metadata                 database.TableMetadata
	Filename                 string
	StoredProcedureOperation *StoredProcedureOperation
	Type                     common.OperationType
}

type StoredProcedureOperation struct {
	StoredProcedure              *storedprocedure.StoredProcedure
	StoredProcedureOperationType StoredProcedureOperationType
}

type StoredProcedureOperationType int

const (
	CreateStoredProcedure StoredProcedureOperationType = iota
	ExecuteStoredProcedure
	AlterStoredProcedure
)

type Data struct {
	Insert [][]any
	Update map[string]any
}

type Result struct {
	Data          *database.QueryResult    `json:"data,omitempty"`
	Table         *database.Table          `json:"table,omitempty"`
	Metadata      *database.TableMetadata  `json:"metadata,omitempty"`
	IndexMetaData []database.IndexMetadata `json:"index_metadata,omitempty"`
	RowsAffected  int64                    `json:"rows_affected,omitempty"`
	Err           error                    `json:"error,omitempty"`
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
	CreateStoredProcedure(op *Operation) *Result
	ExecuteStoredProcedure(op *Operation) *Result
	AlterStoredProcedure(op *Operation) *Result
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

func (o *Operation) Execute() *Result {
	return o.ExecuteMethod(o)
}
