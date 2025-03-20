package interpreter

import (
	"LiminalDb/internal/database"
	"fmt"
	"strconv"
	"strings"
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
	p.nextToken()
	return p
}

// Statements
type Statement interface{}

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

type CreateTableStatement struct {
	TableName string
	Columns   []database.Column
}

type DeleteStatement struct {
	TableName string
	Where     Expression
}

type DropTableStatement struct {
	TableName string
}

type DescribeTableStatement struct {
	TableName string
}

type CreateProcedureStatement struct {
	Name        string
	Parameters  []database.Column
	Body        string
	Description string
}

type AlterProcedureStatement struct {
	Name        string
	Parameters  []database.Column
	Body        string
	Description string
}

type ExecStatement struct {
	Name       string
	Parameters []Expression
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

type VariableExpression struct {
	Name string
}

func (v *VariableExpression) GetValue() interface{} {
	return v.Name
}

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
		return p.parseCreateStatement()
	case DELETE:
		return p.parseDeleteStatement()
	case DROP:
		return p.parseDropTableStatement()
	case DESC:
		return p.parseDescribeTableStatement()
	case ALTER:
		return p.parseAlterStatement()
	case EXEC:
		return p.parseExecStatement()
	default:
		p.peekError(p.curToken.Type)
		return nil
	}
}

func (p *Parser) parseSelectStatement() *SelectStatement {
	stmt := &SelectStatement{}

	if !p.expectPeek(IDENT) {
		if !p.expectPeek(ALL) {
			return nil
		}
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

func (p *Parser) parseCreateStatement() Statement {
	if !p.expectPeek(TABLE) && !p.expectPeek(PROCEDURE) {
		return nil
	}

	switch strings.ToUpper(p.curToken.Literal) {
	case "TABLE":
		return p.parseCreateTableStatement()
	case "PROCEDURE":
		return p.parseCreateProcedureStatement()
	default:
		p.peekError(p.curToken.Type)
		return nil
	}
}

func (p *Parser) parseCreateTableStatement() *CreateTableStatement {
	stmt := &CreateTableStatement{}

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

func (p *Parser) parseDeleteStatement() *DeleteStatement {
	stmt := &DeleteStatement{}

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

func (p *Parser) parseDropTableStatement() *DropTableStatement {
	stmt := &DropTableStatement{}

	if !p.expectPeek(TABLE) {
		return nil
	}

	if !p.expectPeek(IDENT) {
		return nil
	}

	stmt.TableName = p.curToken.Literal

	return stmt
}

func (p *Parser) parseDescribeTableStatement() *DescribeTableStatement {
	stmt := &DescribeTableStatement{}

	if !p.expectPeek(TABLE) {
		return nil
	}

	if !p.expectPeek(IDENT) {
		return nil
	}

	stmt.TableName = p.curToken.Literal

	return stmt
}

func (p *Parser) parseAlterStatement() Statement {
	if !p.expectPeek(PROCEDURE) {
		return nil
	}
	return p.parseAlterProcedureStatement()
}

func (p *Parser) parseCreateProcedureStatement() *CreateProcedureStatement {
	stmt := &CreateProcedureStatement{}

	if !p.expectPeek(IDENT) {
		return nil
	}
	stmt.Name = p.curToken.Literal

	if p.peekTokenIs(LPAREN) {
		p.nextToken()
		stmt.Parameters = p.parseColumnDefinitions()
		if !p.expectPeek(RPAREN) {
			return nil
		}
	}

	body, ok := p.parseProcedureBody()
	if !ok {
		return nil
	}
	stmt.Body = body

	return stmt
}

func (p *Parser) parseAlterProcedureStatement() *AlterProcedureStatement {
	stmt := &AlterProcedureStatement{}

	if !p.expectPeek(IDENT) {
		return nil
	}
	stmt.Name = p.curToken.Literal

	if p.peekTokenIs(LPAREN) {
		p.nextToken()
		p.nextToken()
		stmt.Parameters = p.parseColumnDefinitions()
		if !p.expectPeek(RPAREN) {
			return nil
		}
	}

	body, ok := p.parseProcedureBody()
	if !ok {
		return nil
	}
	stmt.Body = body

	return stmt
}

func (p *Parser) parseProcedureBody() (string, bool) {
	if !p.expectPeek(AS) {
		return "", false
	}
	if !p.expectPeek(BEGIN) {
		return "", false
	}

	var bodyBuilder strings.Builder
	for {
		p.nextToken()
		if p.curTokenIs(END) {
			break
		}
		if p.curToken.Type == EOF {
			return "", false
		}

		stmt := p.ParseStatement()
		if stmt == nil {
			return "", false
		}

		bodyBuilder.WriteString(p.curToken.Literal)
		bodyBuilder.WriteString(" ")

		if p.peekTokenIs(SEMICOLON) {
			p.nextToken()
			bodyBuilder.WriteString("; ")
		}
	}

	return bodyBuilder.String(), true
}

func (p *Parser) parseColumnDefinitions() []database.Column {
	columns := []database.Column{}

	if !p.expectPeek(IDENT) && !p.expectPeek(VARIABLE) {
		return nil
	}

	for {
		col := p.parseColumnDefinition()
		if col == nil {
			return nil
		}
		columns = append(columns, *col)

		if !p.peekTokenIs(COMMA) {
			break
		}

		p.nextToken()
		if !p.expectPeek(IDENT) {
			return nil
		}
	}

	return columns
}

func (p *Parser) parseColumnDefinition() *database.Column {
	col := &database.Column{
		Name:         p.curToken.Literal,
		IsNullable:   true,
		IsPrimaryKey: false,
	}

	if !p.expectPeek(INTTYPE) && !p.expectPeek(FLOATTYPE) &&
		!p.expectPeek(STRINGTYPE) && !p.expectPeek(BOOLTYPE) {
		return nil
	}

	var dataType database.ColumnType
	var err error
	dataType, err = convertTokenTypeToColumnType(p.curToken.Type)
	if err != nil {
		p.errors = append(p.errors, err.Error())
		return nil
	}

	col.DataType = dataType

	if p.peekTokenIs(LPAREN) {
		p.nextToken()
		p.nextToken()

		if p.curToken.Type != INT {
			p.errors = append(p.errors, "expected integer for length specification")
			return nil
		}

		length, err := strconv.Atoi(p.curToken.Literal)
		if err != nil {
			p.errors = append(p.errors, "invalid length specification")
			return nil
		}

		col.Length = uint16(length)

		if !p.expectPeek(RPAREN) {
			return nil
		}
	}

	if p.peekTokenIs(NOT) {
		p.nextToken()
		if !p.expectPeek(NULL) {
			return nil
		}
		col.IsNullable = false
	} else if p.peekTokenIs(NULL) {
		p.nextToken()
		col.IsNullable = true
	}

	if p.peekTokenIs(PRIMARY) {
		p.nextToken()
		if !p.expectPeek(KEY) {
			return nil
		}
		col.IsPrimaryKey = true
	}

	return col
}

func (p *Parser) parseExecStatement() *ExecStatement {
	stmt := &ExecStatement{}

	if !p.expectPeek(IDENT) {
		return nil
	}
	stmt.Name = p.curToken.Literal

	// Parse parameters if they exist
	if p.peekTokenIs(LPAREN) {
		stmt.Parameters = p.parseValueList()
		if !p.curTokenIs(RPAREN) {
			return nil
		}
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
	switch {
	case p.peekTokenIs(ASSIGN):
		return p.parseAssignment()
	case p.curToken.Type == VARIABLE:
		return p.parseVariable()
	case p.curToken.Type == STRING:
		return p.parseStringLiteral()
	case p.curToken.Type == INT:
		return p.parseIntLiteral()
	case p.curToken.Type == FLOAT:
		return p.parseFloatLiteral()
	case p.curToken.Type == BOOL:
		return p.parseBooleanLiteral()
	case p.curToken.Type == IDENT:
		return p.parseIdentifier()
	default:
		return nil
	}
}

func (p *Parser) parseLiteral() Expression {
	switch p.curToken.Type {
	case STRING:
		return p.parseStringLiteral()
	case INT:
		return p.parseIntLiteral()
	case FLOAT:
		return p.parseFloatLiteral()
	case BOOL:
		return p.parseBooleanLiteral()
	default:
		return nil
	}
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

func (p *Parser) parseIdentifier() Expression {
	return &Identifier{Value: p.curToken.Literal}
}

func (p *Parser) parseVariable() Expression {
	name := p.curToken.Literal[1:]
	return &VariableExpression{Name: name}
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
	}
	p.peekError(t)
	return false
}

func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) peekError(t TokenType) {
	msg := fmt.Sprintf("expected next token to be %s, got %s instead", t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
}
