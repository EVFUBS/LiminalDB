package parser

import l "LiminalDb/internal/interpreter/lexer"

func NewParser(l *l.Lexer) *Parser {
	p := &Parser{
		Lexer:  l,
		errors: []string{},
	}
	p.NextToken()
	p.NextToken()
	return p
}
