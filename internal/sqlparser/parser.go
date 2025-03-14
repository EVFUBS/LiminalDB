package sqlparser

import (
	"fmt"
	"strconv"
)

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

// Statements
type Statement interface{}

type CreateTableStatement struct {
	TableName string
	Columns   []ColumnDefinition
}

type SelectStatement struct {
	Fields    []string
	TableName string
	Where     Expression
}

type InsertStatement struct {
	TableName  string
	Columns    []string
	ValueLists [][]Expression
}

// Expressions
type Expression interface {
	GetValue() interface{}
}

type WhereExpression struct {
	Left  Expression
	Right Expression
	Op    string
}

func (w *WhereExpression) GetValue() interface{} {
	return nil
}

type AllExpression struct {
}

func (a *AllExpression) GetValue() interface{} {
	return nil
}

type Identifier struct {
	Value string
}

func (i *Identifier) GetValue() interface{} {
	return i.Value
}

type StringLiteral struct {
	Value string
}

func (s *StringLiteral) GetValue() interface{} {
	return s.Value
}

type Int64Literal struct {
	Value int64
}

func (i *Int64Literal) GetValue() interface{} {
	return i.Value
}

type Float64Literal struct {
	Value float64
}

func (f *Float64Literal) GetValue() interface{} {
	return f.Value
}

type BooleanLiteral struct {
	Value bool
}

func (b *BooleanLiteral) GetValue() interface{} {
	return b.Value
}

type Literal struct {
	Value interface{}
}

func (l *Literal) GetValue() interface{} {
	return l.Value
}

type ColumnDefinition struct {
	Name     string
	DataType TokenType
	Length   int
	Nullable bool
}

// Parsing
func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

func (p *Parser) ParseStatement() Statement {
	switch p.curToken.Type {
	case SELECT:
		return p.parseSelectStatement()
	case INSERT:
		return p.parseInsertStatement()
	case CREATE:
		return p.parseCreateTableStatement()
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

func (p *Parser) parseInsertStatement() *InsertStatement {
	stmt := &InsertStatement{}

	if !p.expectPeek(INTO) {
		return nil
	}

	if !p.expectPeek(IDENT) {
		return nil
	}

	stmt.TableName = p.curToken.Literal

	if !p.expectPeek(LPAREN) {
		return nil
	}

	p.nextToken()
	stmt.Columns = p.parseIdentifierList()

	if !p.expectPeek(RPAREN) {
		return nil
	}

	if !p.expectPeek(VALUES) {
		return nil
	}

	p.nextToken()
	stmt.ValueLists = p.parseValueLists()

	return stmt
}

func (p *Parser) parseCreateTableStatement() *CreateTableStatement {
	stmt := &CreateTableStatement{}

	if !p.expectPeek(TABLE) {
		return nil
	}

	if !p.expectPeek(IDENT) {
		return nil
	}
	stmt.TableName = p.curToken.Literal

	if !p.expectPeek(LPAREN) {
		return nil
	}

	stmt.Columns = p.parseColumnDefinitions()

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return stmt
}

func (p *Parser) parseColumnDefinitions() []ColumnDefinition {
	columns := []ColumnDefinition{}

	// Parse first column
	if !p.expectPeek(IDENT) {
		return nil
	}

	for {
		col := ColumnDefinition{
			Name:     p.curToken.Literal,
			Nullable: true, // Default to nullable
		}

		// Expect data type
		if !p.expectPeek(INTTYPE) && !p.expectPeek(FLOATTYPE) &&
			!p.expectPeek(STRINGTYPE) && !p.expectPeek(BOOLTYPE) {
			return nil
		}
		col.DataType = p.curToken.Type

		// Check for length specification
		if p.peekTokenIs(LPAREN) {
			p.nextToken() // consume (
			p.nextToken() // move to the number

			if p.curToken.Type != INT {
				p.errors = append(p.errors, "expected integer for length specification")
				return nil
			}

			length, err := strconv.Atoi(p.curToken.Literal)
			if err != nil {
				p.errors = append(p.errors, "invalid length specification")
				return nil
			}
			col.Length = length

			if !p.expectPeek(RPAREN) {
				return nil
			}
		}

		// Check for NULL/NOT NULL
		if p.peekTokenIs(NOT) {
			p.nextToken() // consume NOT
			if !p.expectPeek(NULL) {
				return nil
			}
			col.Nullable = false
		} else if p.peekTokenIs(NULL) {
			p.nextToken() // consume NULL
			col.Nullable = true
		}

		columns = append(columns, col)

		// If next token is not comma, break
		if !p.peekTokenIs(COMMA) {
			break
		}

		// Skip comma and continue to next column
		p.nextToken()
		if !p.expectPeek(IDENT) {
			return nil
		}
	}

	return columns
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

	if p.curToken.Type == STRING {
		return p.parseStringLiteral()
	}

	if p.curToken.Type == INT {
		return p.parseIntLiteral()
	}

	if p.curToken.Type == FLOAT {
		return p.parseFloatLiteral()
	}

	if p.curToken.Type == BOOL {
		return p.parseBooleanLiteral()
	}

	if p.curToken.Type == IDENT {
		return p.parseIdentifier()
	}

	return nil
}

func (p *Parser) parseIdentifier() Expression {
	return &Identifier{Value: p.curToken.Literal}
}

func (p *Parser) parseStringLiteral() Expression {
	return &StringLiteral{Value: p.curToken.Literal}
}

func (p *Parser) parseIntLiteral() Expression {
	value, err := strconv.Atoi(p.curToken.Literal)
	if err != nil {
		return nil
	}
	return &Int64Literal{Value: int64(value)}
}

func (p *Parser) parseFloatLiteral() Expression {
	value, err := strconv.ParseFloat(p.curToken.Literal, 64)
	if err != nil {
		return nil
	}
	return &Float64Literal{Value: value}
}

func (p *Parser) parseBooleanLiteral() Expression {
	value, err := strconv.ParseBool(p.curToken.Literal)
	if err != nil {
		return nil
	}
	return &BooleanLiteral{Value: value}
}

func (p *Parser) parseAssignment() Expression {
	expr := &WhereExpression{
		Left: p.parseIdentifier(),
	}

	if !p.expectPeek(ASSIGN) {
		return nil
	}

	expr.Op = p.curToken.Literal

	p.nextToken()
	expr.Right = p.parseExpression()

	return expr
}

func (p *Parser) parseAllExpression() Expression {
	return &AllExpression{}
}

func (p *Parser) parseValueLists() [][]Expression {
	valueLists := [][]Expression{}

	values := p.parseValueList()
	if values != nil {
		valueLists = append(valueLists, values)
	}

	for p.peekTokenIs(COMMA) {
		p.nextToken()
		p.nextToken()
		values := p.parseValueList()
		if values != nil {
			valueLists = append(valueLists, values)
		}
	}

	return valueLists
}

func (p *Parser) parseValueList() []Expression {
	values := []Expression{}

	if !p.curTokenIs(LPAREN) && !p.expectPeek(LPAREN) {
		return nil
	}

	p.nextToken()

	value := p.parseExpression()
	if value != nil {
		values = append(values, value)
	}

	for p.peekTokenIs(COMMA) {
		p.nextToken()
		p.nextToken()
		value := p.parseExpression()
		if value != nil {
			values = append(values, value)
		}
	}

	if !p.expectPeek(RPAREN) {
		return nil
	}

	return values
}

func (p *Parser) curTokenIs(t TokenType) bool {
	return p.curToken.Type == t
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
