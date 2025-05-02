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

type CreateIndexStatement struct {
	IndexName string
	TableName string
	Columns   []string
	IsUnique  bool
}

type DropIndexStatement struct {
	IndexName string
	TableName string
}

type ShowIndexesStatement struct {
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

func (p *Parser) ParseStatement() (Statement, error) {
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
		return p.parseDropStatement()
	case DESC:
		return p.parseDescribeTableStatement()
	case ALTER:
		return p.parseAlterStatement()
	case EXEC:
		return p.parseExecStatement()
	case SHOW:
		return p.parseShowStatement()
	default:
		p.peekError(p.curToken.Type)
		return nil, fmt.Errorf("expected statement, got %s", p.curToken.Literal)
	}
}

func (p *Parser) parseSelectStatement() (*SelectStatement, error) {
	stmt := &SelectStatement{}

	if !p.expectPeek(IDENT) {
		if !p.expectPeek(ALL) {
			return nil, fmt.Errorf("expected identifier or all, got %s", p.curToken.Literal)
		}
	}

	stmt.Fields = p.parseIdentifierList()

	if !p.expectPeek(FROM) {
		return nil, fmt.Errorf("expected from, got %s", p.curToken.Literal)
	}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt.TableName = p.curToken.Literal

	if p.peekTokenIs(WHERE) {
		p.nextToken()
		p.nextToken()
		stmt.Where = p.parseExpression()
	}

	if !p.expectPeek(SEMICOLON) && !p.expectPeek(EOF) {
		return nil, fmt.Errorf("expected semicolon or eof, got %s", p.curToken.Literal)
	}

	return stmt, nil
}

func (p *Parser) parseInsertStatement() (*InsertStatement, error) {
	stmt := &InsertStatement{}

	if !p.expectPeek(INTO) {
		return nil, fmt.Errorf("expected into, got %s", p.curToken.Literal)
	}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt.TableName = p.curToken.Literal

	if !p.expectPeek(LPAREN) {
		return nil, fmt.Errorf("expected left parenthesis, got %s", p.curToken.Literal)
	}

	p.nextToken()
	stmt.Columns = p.parseIdentifierList()

	if !p.expectPeek(RPAREN) {
		return nil, fmt.Errorf("expected right parenthesis, got %s", p.curToken.Literal)
	}

	if !p.expectPeek(VALUES) {
		return nil, fmt.Errorf("expected values, got %s", p.curToken.Literal)
	}

	p.nextToken()
	stmt.ValueLists = p.parseValueLists()

	return stmt, nil
}

func (p *Parser) parseCreateStatement() (Statement, error) {
	if !p.expectPeek(TABLE) && !p.expectPeek(PROCEDURE) && !p.expectPeek(INDEX) && !p.expectPeek(UNIQUE) {
		return nil, fmt.Errorf("expected table, procedure, index, or unique, got %s", p.curToken.Literal)
	}

	switch p.curToken.Type {
	case TABLE:
		return p.parseCreateTableStatement()
	case PROCEDURE:
		return p.parseCreateProcedureStatement()
	case INDEX:
		return p.parseCreateIndexStatement(false)
	case UNIQUE:
		if !p.expectPeek(INDEX) {
			return nil, fmt.Errorf("expected INDEX after UNIQUE, got %s", p.curToken.Literal)
		}
		return p.parseCreateIndexStatement(true)
	default:
		p.peekError(p.curToken.Type)
		return nil, fmt.Errorf("expected table, procedure, index, or unique, got %s", p.curToken.Literal)
	}
}

func (p *Parser) parseCreateIndexStatement(isUnique bool) (*CreateIndexStatement, error) {
	stmt := &CreateIndexStatement{
		IsUnique: isUnique,
	}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt.IndexName = p.curToken.Literal

	if !p.expectPeek(ON) {
		return nil, fmt.Errorf("expected ON, got %s", p.curToken.Literal)
	}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt.TableName = p.curToken.Literal

	if !p.expectPeek(LPAREN) {
		return nil, fmt.Errorf("expected left parenthesis, got %s", p.curToken.Literal)
	}

	p.nextToken()
	stmt.Columns = p.parseIdentifierList()

	if !p.expectPeek(RPAREN) {
		return nil, fmt.Errorf("expected right parenthesis, got %s", p.curToken.Literal)
	}

	return stmt, nil
}

func (p *Parser) parseCreateTableStatement() (*CreateTableStatement, error) {
	stmt := &CreateTableStatement{}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}
	stmt.TableName = p.curToken.Literal

	if !p.expectPeek(LPAREN) {
		return nil, fmt.Errorf("expected left parenthesis, got %s", p.curToken.Literal)
	}

	stmt.Columns = p.parseColumnDefinitions()

	if !p.expectPeek(RPAREN) {
		return nil, fmt.Errorf("expected right parenthesis, got %s", p.curToken.Literal)
	}

	return stmt, nil
}

func (p *Parser) parseDeleteStatement() (*DeleteStatement, error) {
	stmt := &DeleteStatement{}

	if !p.expectPeek(FROM) {
		return nil, fmt.Errorf("expected from, got %s", p.curToken.Literal)
	}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt.TableName = p.curToken.Literal

	if p.peekTokenIs(WHERE) {
		p.nextToken()
		p.nextToken()
		stmt.Where = p.parseExpression()
	}

	return stmt, nil
}

func (p *Parser) parseDropStatement() (Statement, error) {
	if !p.expectPeek(TABLE) && !p.expectPeek(INDEX) {
		return nil, fmt.Errorf("expected table or index, got %s", p.curToken.Literal)
	}

	switch p.curToken.Type {
	case TABLE:
		return p.parseDropTableStatement()
	case INDEX:
		return p.parseDropIndexStatement()
	default:
		return nil, fmt.Errorf("expected table or index, got %s", p.curToken.Literal)
	}
}

func (p *Parser) parseDropTableStatement() (*DropTableStatement, error) {
	stmt := &DropTableStatement{}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt.TableName = p.curToken.Literal

	return stmt, nil
}

func (p *Parser) parseDropIndexStatement() (*DropIndexStatement, error) {
	stmt := &DropIndexStatement{}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt.IndexName = p.curToken.Literal

	if !p.expectPeek(ON) {
		return nil, fmt.Errorf("expected ON, got %s", p.curToken.Literal)
	}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt.TableName = p.curToken.Literal

	return stmt, nil
}

func (p *Parser) parseShowStatement() (Statement, error) {
	if !p.expectPeek(INDEXES) {
		return nil, fmt.Errorf("expected INDEXES, got %s", p.curToken.Literal)
	}

	if !p.expectPeek(FROM) {
		return nil, fmt.Errorf("expected FROM, got %s", p.curToken.Literal)
	}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt := &ShowIndexesStatement{
		TableName: p.curToken.Literal,
	}

	return stmt, nil
}

func (p *Parser) parseDescribeTableStatement() (*DescribeTableStatement, error) {
	stmt := &DescribeTableStatement{}

	if !p.expectPeek(TABLE) {
		return nil, fmt.Errorf("expected table, got %s", p.curToken.Literal)
	}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt.TableName = p.curToken.Literal

	return stmt, nil
}

func (p *Parser) parseAlterStatement() (Statement, error) {
	if !p.expectPeek(PROCEDURE) {
		return nil, fmt.Errorf("expected procedure, got %s", p.curToken.Literal)
	}
	return p.parseAlterProcedureStatement()
}

func (p *Parser) parseCreateProcedureStatement() (*CreateProcedureStatement, error) {
	stmt := &CreateProcedureStatement{}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}
	stmt.Name = p.curToken.Literal

	if p.peekTokenIs(LPAREN) {
		p.nextToken()
		stmt.Parameters = p.parseColumnDefinitions()
		if !p.expectPeek(RPAREN) {
			return nil, fmt.Errorf("expected right parenthesis, got %s", p.curToken.Literal)
		}
	}

	body, ok, err := p.parseProcedureBody()
	if !ok || err != nil {
		return nil, err
	}
	stmt.Body = body

	return stmt, nil
}

func (p *Parser) parseAlterProcedureStatement() (*AlterProcedureStatement, error) {
	stmt := &AlterProcedureStatement{}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}
	stmt.Name = p.curToken.Literal

	if p.peekTokenIs(LPAREN) {
		p.nextToken()
		p.nextToken()
		stmt.Parameters = p.parseColumnDefinitions()
		if !p.expectPeek(RPAREN) {
			return nil, fmt.Errorf("expected right parenthesis, got %s", p.curToken.Literal)
		}
	}

	body, ok, err := p.parseProcedureBody()
	if !ok || err != nil {
		return nil, err
	}
	stmt.Body = body

	return stmt, nil
}

func (p *Parser) parseProcedureBody() (string, bool, error) {
	if !p.expectPeek(AS) {
		return "", false, fmt.Errorf("expected as, got %s", p.curToken.Literal)
	}
	if !p.expectPeek(BEGIN) {
		return "", false, fmt.Errorf("expected begin, got %s", p.curToken.Literal)
	}

	var bodyBuilder strings.Builder
	for {
		p.nextToken()
		if p.curTokenIs(END) {
			break
		}
		if p.curToken.Type == EOF {
			return "", false, fmt.Errorf("expected end, got %s", p.curToken.Literal)
		}

		stmt, err := p.ParseStatement()
		if stmt == nil {
			return "", false, err
		}

		bodyBuilder.WriteString(p.curToken.Literal)
		bodyBuilder.WriteString(" ")

		if p.peekTokenIs(SEMICOLON) {
			p.nextToken()
			bodyBuilder.WriteString("; ")
		}
	}

	return bodyBuilder.String(), true, nil
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

func (p *Parser) parseExecStatement() (*ExecStatement, error) {
	stmt := &ExecStatement{}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}
	stmt.Name = p.curToken.Literal

	// Parse parameters if they exist
	if p.peekTokenIs(LPAREN) {
		stmt.Parameters = p.parseValueList()
		if !p.curTokenIs(RPAREN) {
			return nil, fmt.Errorf("expected right parenthesis, got %s", p.curToken.Literal)
		}
	}

	return stmt, nil
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
	case p.peekTokenIs(ASSIGN) || p.peekTokenIs(LESS_THAN) || p.peekTokenIs(LESS_THAN_OR_EQ) || p.peekTokenIs(GREATER_THAN) || p.peekTokenIs(GREATER_THAN_OR_EQ):
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

	if !p.expectPeek(ASSIGN) && !p.expectPeek(LESS_THAN) && !p.expectPeek(LESS_THAN_OR_EQ) && !p.expectPeek(GREATER_THAN) && !p.expectPeek(GREATER_THAN_OR_EQ) {
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
