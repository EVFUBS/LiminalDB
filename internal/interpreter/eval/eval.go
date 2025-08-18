package eval

import (
	"LiminalDb/internal/database/operations"
	l "LiminalDb/internal/interpreter/lexer"
	p "LiminalDb/internal/interpreter/parser"
	"LiminalDb/internal/logger"
	"fmt"
)

type Evaluator struct {
	parser     *p.Parser
	operations operations.Operations
}

func NewEvaluator(parser *p.Parser) *Evaluator {
	return &Evaluator{
		parser:     parser,
		operations: &operations.OperationsImpl{},
	}
}

func (e *Evaluator) Execute(query string) (any, error) {
	logger.Debug("Executing query: %s", query)

	e.parser.Lexer = l.NewLexer(query)
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


