package operations

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/common"
	"LiminalDb/internal/database"
	DbCommon "LiminalDb/internal/database/common"
	"LiminalDb/internal/database/serializer"
	l "LiminalDb/internal/logger"
	"LiminalDb/internal/storedprocedure"
	"os"
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
	ShadowManager            interface{} // Interface to avoid circular import
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
	Message       string                   `json:"message,omitempty"`
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

// ShadowManagerProvider interface to avoid circular import
type ShadowManagerProvider interface {
	GetWorkingTablePath(tableName string) string
	GetWorkingIndexPath(tableName, indexName string) string
	MarkTableToBeDropped(tableName string)
}

// getWorkingTablePath returns the path to use for table operations (shadow or real)
func (o *OperationsImpl) getWorkingTablePath(op *Operation, tableName string) string {
	if op.ShadowManager != nil {
		if sp, ok := op.ShadowManager.(ShadowManagerProvider); ok {
			return sp.GetWorkingTablePath(tableName)
		}
	}
	return DbCommon.GetTableFilePath(tableName)
}

// getWorkingIndexPath returns the path to use for index operations (shadow or real)
func (o *OperationsImpl) getWorkingIndexPath(op *Operation, tableName, indexName string) string {
	if op.ShadowManager != nil {
		if sp, ok := op.ShadowManager.(ShadowManagerProvider); ok {
			return sp.GetWorkingIndexPath(tableName, indexName)
		}
	}
	return DbCommon.GetIndexFilePath(tableName, indexName)
}

// writeTableWithShadow writes a table using shadow path if available
func (o *OperationsImpl) writeTableWithShadow(op *Operation, table *database.Table, tableName string) error {
	workingPath := o.getWorkingTablePath(op, tableName)
	return o.Serializer.WriteTableToPath(table, tableName, workingPath)
}

// writeIndexWithShadow writes an index using shadow path if available
func (o *OperationsImpl) writeIndexWithShadow(op *Operation, indexBytes []byte, tableName, indexName string) error {
	workingPath := o.getWorkingIndexPath(op, tableName, indexName)
	return os.WriteFile(workingPath, indexBytes, 0666)
}
