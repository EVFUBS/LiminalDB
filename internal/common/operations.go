package common

type OperationType int

const (
	Read OperationType = iota
	Write
	Delete
	Alter
	Insert
	Transaction
	Unknown
	CreateTable
	DropTable
	CreateProcedure
	AlterProcedure
	ExecuteProcedure
	CreateIndex
	DropIndex
	Commit
	Rollback
)
