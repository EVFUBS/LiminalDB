package transaction

import (
	"LiminalDb/internal/ast"
	"LiminalDb/internal/database/operations"
)

type Change struct {
	Statement ast.Statement
	Operation operations.Operation
	Commit    bool
	Rollback  bool
}
