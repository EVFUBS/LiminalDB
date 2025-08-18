package parser

import l "LiminalDb/internal/interpreter/lexer"

type Parser struct {
	Lexer     *l.Lexer
	errors    []string
	curToken  l.Token
	peekToken l.Token
}
