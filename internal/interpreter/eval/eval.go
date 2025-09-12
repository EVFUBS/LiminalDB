package eval

import (
	"LiminalDb/internal/database/operations"
	"LiminalDb/internal/database/transaction"
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

func NewEvaluator(parser *p.Parser) *Evaluator {
	logger = log.Get("interpreter")

	return &Evaluator{
		parser:             parser,
		operations:         operations.NewOperationsImpl(),
		TransactionManager: transaction.NewTransactionManager(),
	}
}

func (e *Evaluator) Execute(query string) (any, error) {
	logger.Debug("Executing query: %s", query)

	e.parser.Lexer.SetInput(query)
	e.parser.NextToken()
	e.parser.NextToken()

	stmt, err := e.parser.ParseStatement()
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
