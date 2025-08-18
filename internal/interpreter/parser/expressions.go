package parser

import (
	"LiminalDb/internal/ast"
	. "LiminalDb/internal/common"
	l "LiminalDb/internal/interpreter/lexer"
	"fmt"
	"strconv"
	"time"
)

const (
	_ int = iota
	LOWEST
	LOGICAL    // AND OR
	EQUALS     // =
	COMPARISON // < <= > >=
	SUM        // + -
	PRODUCT    // * /
	PREFIX     // -X
	CALL       // myFunction(X)
)

var precedences = map[l.TokenType]int{
	ASSIGN:             EQUALS,
	LESS_THAN:          COMPARISON,
	LESS_THAN_OR_EQ:    COMPARISON,
	GREATER_THAN:       COMPARISON,
	GREATER_THAN_OR_EQ: COMPARISON,
	PLUS:               SUM,
	MINUS:              SUM,
	MULTIPLY:           PRODUCT,
	DIVIDE:             PRODUCT,
	AND:                LOGICAL,
	OR:                 LOGICAL,
}

func (p *Parser) peekPrecedence() int {
	if p, ok := precedences[p.peekToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *Parser) curPrecedence() int {
	if p, ok := precedences[p.curToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *Parser) parseExpression() ast.Expression {
	return p.parseExpressionWithPrecedence(LOWEST)
}

func (p *Parser) parseExpressionWithPrecedence(precedence int) ast.Expression {
	var leftExpr ast.Expression

	// Parse prefix expression
	switch {
	case p.curToken.Type == VARIABLE:
		leftExpr = p.parseVariable()
	case p.curToken.Type == DATETIME:
		leftExpr = p.parseDateTimeLiteral()
	case p.curToken.Type == STRING:
		leftExpr = p.parseStringLiteral()
	case p.curToken.Type == INT:
		leftExpr = p.parseIntLiteral()
	case p.curToken.Type == FLOAT:
		leftExpr = p.parseFloatLiteral()
	case p.curToken.Type == BOOL:
		leftExpr = p.parseBooleanLiteral()
	case p.curToken.Type == IDENT:
		leftExpr = p.parseIdentifier()
	default:
		return nil
	}

	// Parse infix expressions with higher precedence
	for !p.peekTokenIs(EOF) && precedence < p.peekPrecedence() {
		switch p.peekToken.Type {
		case PLUS, MINUS, MULTIPLY, DIVIDE:
			p.NextToken()
			leftExpr = p.parseBinaryExpression(leftExpr)
		case ASSIGN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN, GREATER_THAN_OR_EQ:
			if precedence >= EQUALS {
				return leftExpr
			}
			p.NextToken()
			leftExpr = p.parseComparisonExpression(leftExpr)
		case AND, OR:
			if precedence >= LOGICAL {
				return leftExpr
			}
			p.NextToken()
			leftExpr = p.parseLogicalExpression(leftExpr)
		default:
			return leftExpr
		}
	}

	return leftExpr
}

func (p *Parser) parseStringLiteral() ast.Expression {
	return &ast.StringLiteral{Value: p.curToken.Literal}
}

func (p *Parser) parseIntLiteral() ast.Expression {
	value, err := strconv.Atoi(p.curToken.Literal)
	if err != nil {
		return nil
	}
	return &ast.Int64Literal{Value: int64(value)}
}

func (p *Parser) parseFloatLiteral() ast.Expression {
	value, err := strconv.ParseFloat(p.curToken.Literal, 64)
	if err != nil {
		return nil
	}
	return &ast.Float64Literal{Value: value}
}

func (p *Parser) parseBooleanLiteral() ast.Expression {
	value, err := strconv.ParseBool(p.curToken.Literal)
	if err != nil {
		return nil
	}
	return &ast.BooleanLiteral{Value: value}
}

func (p *Parser) parseDateTimeLiteral() ast.Expression {
	value, err := time.ParseInLocation("2006-01-02 15:04:05", p.curToken.Literal, time.UTC)
	if err != nil {
		return nil
	}
	return &ast.DateTimeLiteral{Value: value.UTC()}
}

func (p *Parser) parseIdentifier() ast.Expression {
	return &ast.Identifier{Value: p.curToken.Literal}
}

func (p *Parser) parseVariable() ast.Expression {
	name := p.curToken.Literal[1:]
	return &ast.VariableExpression{Name: name}
}

func (p *Parser) parseBinaryExpression(left ast.Expression) ast.Expression {
	operator := p.curToken.Literal
	precedence := p.curPrecedence()

	p.NextToken()
	right := p.parseExpressionWithPrecedence(precedence)

	return &ast.BinaryExpression{
		Left:  left,
		Op:    operator,
		Right: right,
	}
}

func (p *Parser) parseComparisonExpression(left ast.Expression) ast.Expression {
	operator := p.curToken.Literal
	precedence := p.curPrecedence()

	p.NextToken()
	right := p.parseExpressionWithPrecedence(precedence)

	return &ast.AssignmentExpression{
		Left:  left,
		Op:    operator,
		Right: right,
	}
}

func (p *Parser) parseLogicalExpression(left ast.Expression) ast.Expression {
	operator := p.curToken.Literal
	precedence := p.curPrecedence()

	p.NextToken()
	right := p.parseExpressionWithPrecedence(precedence)

	return &ast.AssignmentExpression{
		Left:  left,
		Op:    operator,
		Right: right,
	}
}

func (p *Parser) parseValueLists() [][]ast.Expression {
	valueLists := [][]ast.Expression{}

	values := p.parseValueList()
	if values != nil {
		valueLists = append(valueLists, values)
	}

	for p.peekTokenIs(COMMA) {
		p.NextToken()
		p.NextToken()
		values := p.parseValueList()
		if values != nil {
			valueLists = append(valueLists, values)
		}
	}

	return valueLists
}

func (p *Parser) parseValueList() []ast.Expression {
	// TODO: Possible bug here with expectPeek since it moves the token forward
	if !p.curTokenIs(LPAREN) && !p.expectPeek(LPAREN) {
		return nil
	}

	values := p.parseValueListWithoutBrackets()

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return values
}

func (p *Parser) parseValueListWithoutBrackets() []ast.Expression {
	values := []ast.Expression{}

	p.NextToken()

	value := p.parseExpression()
	if value != nil {
		values = append(values, value)
	}

	for p.peekTokenIs(COMMA) {
		p.NextToken()
		p.NextToken()
		value := p.parseExpression()
		if value != nil {
			values = append(values, value)
		}
	}

	return values
}

func (p *Parser) curTokenIs(t l.TokenType) bool {
	return p.curToken.Type == t
}

func (p *Parser) expectPeek(t l.TokenType) bool {
	if p.peekTokenIs(t) {
		p.NextToken()
		return true
	}
	p.peekError(t)
	return false
}

func (p *Parser) peekTokenIs(t l.TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) peekError(t l.TokenType) {
	msg := fmt.Sprintf("expected next token to be %s, got %s instead", t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
}
