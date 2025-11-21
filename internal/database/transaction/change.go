package transaction

import (
	"LiminalDb/internal/database/operations"
)

type Change struct {
	Operation   *operations.Operation
	Ran         bool
	Commit      bool
	Rollback    bool
	Locks       map[string]Lock
	ShadowPaths map[string]string // original path → shadow path for this change
}
