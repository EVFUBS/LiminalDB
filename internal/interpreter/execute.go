package interpreter

import (
	ops "LiminalDb/internal/database/operations"
	e "LiminalDb/internal/interpreter/eval"
)

// TODO: Better name for this file

func Evaluate(sql string) (*[]ops.Operation, error) {
	evaluator := e.NewEvaluator()
	return evaluator.Evaluate(sql)
}

func SetupEvaluator() *e.Evaluator {
	evaluator := e.NewEvaluator()
	return evaluator
}
