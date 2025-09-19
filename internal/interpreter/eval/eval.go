package eval

import (
	"LiminalDb/internal/database/operations"
	"LiminalDb/internal/database/transaction"
	lex "LiminalDb/internal/interpreter/lexer"
	p "LiminalDb/internal/interpreter/parser"
	log "LiminalDb/internal/logger"
	"fmt"
)

var logger *log.Logger

type Evaluator struct {
	parser             *p.Parser
	operations         *operations.OperationsImpl
	TransactionManager *transaction.TransactionManager
}

func NewEvaluator() *Evaluator {
	logger = log.Get("interpreter")

	return &Evaluator{
		operations:         operations.NewOperationsImpl(),
		TransactionManager: transaction.NewTransactionManager(),
	}
}

func (e *Evaluator) Execute(query string) (any, error) {
	logger.Debug("Executing query: %s", query)

	l := lex.NewLexer(query)
	pr := p.NewParser(l)

	stmt, err := pr.ParseStatement()
	if err != nil || stmt == nil {
		logger.Error("Failed to parse query: %s with error: %s", query, err)
		return nil, fmt.Errorf("failed to parse query: %s with error: %s", query, err)
	}

	result, err := e.executeStatement(stmt)
	if err != nil {
		logger.Error("Failed to execute statement: %v", err)
		return nil, err
	}

	logger.Debug("Query executed successfully")
	return result, nil
}
