package transaction

import (
	ops "LiminalDb/internal/database/operations"
)

type Change struct {
	Task      func(ops.Operation) ops.Result
	Operation ops.Operation
}

func (c *Change) execute(op ops.Operation) ops.Result {
	return c.Task(op)
}
