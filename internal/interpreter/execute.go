package interpreter

import (
	e "LiminalDb/internal/interpreter/eval"
	l "LiminalDb/internal/interpreter/lexer"
	p "LiminalDb/internal/interpreter/parser"
)

func Execute(sql string) (any, error) {
	lexer := l.NewLexer()
	parser := p.NewParser(lexer)
	evaluator := e.NewEvaluator(parser)
	return evaluator.Execute(sql)
}

func SetupEvaluator() *e.Evaluator {
	lexer := l.NewLexer()
	parser := p.NewParser(lexer)
	evaluator := e.NewEvaluator(parser)

	return evaluator
}
