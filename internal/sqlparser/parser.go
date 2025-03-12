package sqlparser

import "fmt"

type Parser struct {
	lexer     *Lexer
	errors    []string
	curToken  Token
	peekToken Token
}

func NewParser(l *Lexer) *Parser {
	p := &Parser{
		lexer:  l,
		errors: []string{},
	}
	p.nextToken()
	p.nextToken() // read two tokens, so curToken and peekToken are both set
	return p
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

func (p *Parser) ParseStatement() Statement {
	switch p.curToken.Type {
	case SELECT:
		return p.parseSelectStatement()
	default:
		return nil
	}
}

func (p *Parser) parseSelectStatement() *SelectStatement {
	stmt := &SelectStatement{}

	if !p.expectPeek(IDENT) {
		return nil
	}

	stmt.Fields = p.parseIdentifierList()

	if !p.expectPeek(FROM) {
		return nil
	}

	if !p.expectPeek(IDENT) {
		return nil
	}

	stmt.TableName = p.curToken.Literal

	if p.peekTokenIs(WHERE) {
		p.nextToken()
		p.nextToken()
		stmt.Where = p.parseExpression()
	}

	return stmt
}

func (p *Parser) parseIdentifierList() []string {
	identifiers := []string{p.curToken.Literal}

	for p.peekTokenIs(COMMA) {
		p.nextToken()
		p.nextToken()
		identifiers = append(identifiers, p.curToken.Literal)
	}

	return identifiers
}

func (p *Parser) parseExpression() Expression {
	if p.peekTokenIs(ASSIGN) {
		return p.parseAssignment()
	}

	if p.curToken.Type == QUOTE {
		p.nextToken()
		return p.parseLiteral()
	}

	if p.curToken.Type == IDENT {
		return p.parseIdentifier()
	}

	return nil
}

func (p *Parser) parseIdentifier() Expression {
	return &Identifier{Value: p.curToken.Literal}
}

func (p *Parser) parseLiteral() Expression {
	return &Literal{Value: p.curToken.Literal}
}

func (p *Parser) parseAssignment() Expression {
	expr := &WhereExpression{
		Left: p.parseIdentifier(),
	}

	if !p.expectPeek(ASSIGN) {
		return nil
	}

	expr.Op = p.curToken.Literal

	if p.peekTokenIs(QUOTE) {
		p.nextToken()
		expr.Right = p.parseExpression()
	} else {
		expr.Right = p.parseIdentifier()
	}

	return expr
}

func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()
		return true
	} else {
		p.peekError(t)
		return false
	}
}

func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) peekError(t TokenType) {
	msg := fmt.Sprintf("expected next token to be %s, got %s instead", t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
}

type Statement interface{}

type SelectStatement struct {
	Fields    []string
	TableName string
	Where     Expression
}

type Expression interface{}

type WhereExpression struct {
	Left  Expression
	Right Expression
	Op    string
}

type Identifier struct {
	Value string
}

type Literal struct {
	Value interface{}
}
