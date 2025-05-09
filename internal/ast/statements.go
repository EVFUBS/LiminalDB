package ast

import "LiminalDb/internal/database"

type Statement interface{}

type SelectStatement struct {
	Fields    []string
	TableName string
	Where     Expression
}

type InsertStatement struct {
	TableName  string
	Columns    []string
	ValueLists [][]Expression
}

type CreateTableStatement struct {
	TableName string
	Columns   []database.Column
}

type DeleteStatement struct {
	TableName string
	Where     Expression
}

type DropTableStatement struct {
	TableName string
}

type DescribeTableStatement struct {
	TableName string
}

type CreateIndexStatement struct {
	IndexName string
	TableName string
	Columns   []string
	IsUnique  bool
}

type DropIndexStatement struct {
	IndexName string
	TableName string
}

type ShowIndexesStatement struct {
	TableName string
}

type CreateProcedureStatement struct {
	Name        string
	Parameters  []database.Column
	Body        string
	Description string
}

type AlterProcedureStatement struct {
	Name        string
	Parameters  []database.Column
	Body        string
	Description string
}

type ExecStatement struct {
	Name       string
	Parameters []Expression
}
