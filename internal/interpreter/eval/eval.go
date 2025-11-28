package eval

import (
	"LiminalDb/internal/database/operations"
	"LiminalDb/internal/interpreter/lexer"
	"LiminalDb/internal/interpreter/parser"
	log "LiminalDb/internal/logger"
	"fmt"
)

var logger *log.Logger

type Evaluator struct {
	operations *operations.OperationsImpl
}

func NewEvaluator() *Evaluator {
	logger = log.Get("interpreter")
	return &Evaluator{
		operations: operations.NewOperationsImpl(),
	}
}

func (e *Evaluator) Evaluate(query string) (*[]operations.Operation, error) {
	logger.Debug("Executing query: %s", query)

	lexer := lexer.NewLexer(query)
	parser := parser.NewParser(lexer)

	stmt, err := parser.ParseStatement()
	if err != nil || stmt == nil {
		logger.Error("Failed to parse query: %s with error: %s", query, err)
		return nil, fmt.Errorf("failed to parse query: %s with error: %s", query, err)
	}

	operations, err := e.evaluateStatement(stmt)
	if err != nil {
		logger.Error("Failed to evaluate statement: %v", err)
		return nil, err
	}

	logger.Debug("Query executed successfully")
	return operations, nil
}
