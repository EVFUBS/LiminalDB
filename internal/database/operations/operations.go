package operations

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/database"
)

type Filter func([]any, []database.Column) (bool, error)

type Operations interface {
	CreateTable(metadata database.TableMetadata) error
	DropTable(tableName string) error
	ReadMetadata(filename string) (database.TableMetadata, error)
	WriteRows(tableName string, data [][]any) error
	UpdateRows(tableName string, data map[string]any, filter Filter) error
	ReadRows(tableName string, fields []string, filter Filter, where ast.Expression) (*database.QueryResult, error)
	DeleteRows(tableName string, filter Filter) (int64, error)
	CreateIndex(tableName string, indexName string, columns []string, isUnique bool) error
	DropIndex(tableName string, indexName string) error
	ListIndexes(tableName string) ([]database.IndexMetadata, error)
}

type OperationsImpl struct {
	Serializer database.BinarySerializer
}
