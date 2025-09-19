package interpreter

import (
	e "LiminalDb/internal/interpreter/eval"
)

func Execute(sql string) (any, error) {
	evaluator := e.NewEvaluator()
	return evaluator.Execute(sql)
}

func SetupEvaluator() *e.Evaluator {
	evaluator := e.NewEvaluator()
	return evaluator
}
