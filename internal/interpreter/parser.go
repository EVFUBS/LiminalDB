package interpreter

import (
	"LiminalDb/internal/ast"
	. "LiminalDb/internal/common"
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

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.lexer.NextToken()
}

func (p *Parser) ParseStatement() (ast.Statement, error) {
	switch p.curToken.Type {
	case SELECT:
		return p.parseSelectStatement()
	case INSERT:
		return p.parseInsertStatement()
	case CREATE:
		return p.parseCreateStatement()
	case DELETE:
		return p.parseDeleteStatement()
	case UPDATE:
		return p.parseUpdateStatement()
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

func (p *Parser) parseSelectStatement() (*ast.SelectStatement, error) {
	stmt := &ast.SelectStatement{}

	if !p.expectPeek(IDENT) {
		if !p.expectPeek(MULTIPLY) {
			return nil, fmt.Errorf("expected identifier or *, got %s", p.curToken.Literal)
		}
		// In SELECT statements, * is treated as ALL
		p.curToken.Type = ALL
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

func (p *Parser) parseInsertStatement() (*ast.InsertStatement, error) {
	stmt := &ast.InsertStatement{}

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

func (p *Parser) parseUpdateStatement() (ast.Statement, error) {
	stmt := &ast.UpdateStatement{}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt.TableName = p.curToken.Literal

	if !p.expectPeek(SET) {
		return nil, fmt.Errorf("expected set, got %s", p.curToken.Literal)
	}

	stmt.Values = p.parseValueListWithoutBrackets()

	if !p.expectPeek(WHERE) {
		return nil, fmt.Errorf("expected where, got %s", p.curToken.Literal)
	}

	p.nextToken()

	stmt.Where = p.parseExpression()

	if !p.expectPeek(SEMICOLON) && !p.expectPeek(EOF) {
		return nil, fmt.Errorf("expected semicolon or eof, got %s", p.curToken.Literal)
	}

	return stmt, nil
}

func (p *Parser) parseCreateStatement() (ast.Statement, error) {
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

func (p *Parser) parseCreateIndexStatement(isUnique bool) (*ast.CreateIndexStatement, error) {
	stmt := &ast.CreateIndexStatement{
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

func (p *Parser) parseCreateTableStatement() (*ast.CreateTableStatement, error) {
	stmt := &ast.CreateTableStatement{}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}
	stmt.TableName = p.curToken.Literal

	if !p.expectPeek(LPAREN) {
		return nil, fmt.Errorf("expected left parenthesis, got %s", p.curToken.Literal)
	}

	columns, err := p.parseColumnDefinitions()

	if err != nil {
		return nil, err
	}

	stmt.Columns = columns

	if p.peekTokenIs(FOREIGN) {
		p.nextToken()
		if !p.expectPeek(KEY) {
			return nil, fmt.Errorf("expected key, got %s", p.curToken.Literal)
		}

		p.nextToken()
		p.nextToken()

		columns := p.parseIdentifierList()
		p.nextToken()

		if !p.expectPeek(REFERENCES) {
			return nil, fmt.Errorf("expected references, got %s", p.curToken.Literal)
		}
		p.nextToken()

		referencedTable := p.curToken.Literal

		if !p.expectPeek(LPAREN) {
			return nil, fmt.Errorf("expected left parenthesis, got %s", p.curToken.Literal)
		}

		p.nextToken()
		referencedColumns := p.parseIdentifierList()

		stmt.ForeignKeys = append(stmt.ForeignKeys, database.ForeignKeyConstraint{
			ReferencedTable: referencedTable,
			ReferencedColumns: []database.ForeignKeyReference{
				{
					ColumnName:           columns[0],
					ReferencedColumnName: referencedColumns[0],
				},
			},
		})
	}

	if !p.expectPeek(RPAREN) {
		return nil, fmt.Errorf("expected right parenthesis, got %s", p.curToken.Literal)
	}

	return stmt, nil
}

func (p *Parser) parseDeleteStatement() (*ast.DeleteStatement, error) {
	stmt := &ast.DeleteStatement{}

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

func (p *Parser) parseDropStatement() (ast.Statement, error) {
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

func (p *Parser) parseDropTableStatement() (*ast.DropTableStatement, error) {
	stmt := &ast.DropTableStatement{}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt.TableName = p.curToken.Literal

	return stmt, nil
}

func (p *Parser) parseDropIndexStatement() (*ast.DropIndexStatement, error) {
	stmt := &ast.DropIndexStatement{}

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

func (p *Parser) parseShowStatement() (ast.Statement, error) {
	if !p.expectPeek(INDEXES) {
		return nil, fmt.Errorf("expected INDEXES, got %s", p.curToken.Literal)
	}

	if !p.expectPeek(FROM) {
		return nil, fmt.Errorf("expected FROM, got %s", p.curToken.Literal)
	}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt := &ast.ShowIndexesStatement{
		TableName: p.curToken.Literal,
	}

	return stmt, nil
}

func (p *Parser) parseDescribeTableStatement() (*ast.DescribeTableStatement, error) {
	stmt := &ast.DescribeTableStatement{}

	if !p.expectPeek(TABLE) {
		return nil, fmt.Errorf("expected table, got %s", p.curToken.Literal)
	}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt.TableName = p.curToken.Literal

	return stmt, nil
}

func (p *Parser) parseAlterStatement() (ast.Statement, error) {
	switch p.curToken.Type {
	case PROCEDURE:
		return p.parseAlterProcedureStatement()
	// case TABLE:
	// 	return p.parseAlterTableStatement()
	default:
		return nil, fmt.Errorf("expected procedure or table, got %s", p.curToken.Literal)
	}
}

func (p *Parser) parseCreateProcedureStatement() (*ast.CreateProcedureStatement, error) {
	stmt := &ast.CreateProcedureStatement{}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}
	stmt.Name = p.curToken.Literal

	if p.peekTokenIs(LPAREN) {
		p.nextToken()

		parameters, err := p.parseColumnDefinitions()
		if err != nil {
			return nil, err
		}
		stmt.Parameters = parameters

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

// func (p *Parser) parseAlterTableStatement() (*ast.AlterTableStatement, error) {
// 	stmt := &ast.AlterTableStatement{}

// 	if !p.expectPeek(IDENT) {
// 		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
// 	}
// 	stmt.TableName = p.curToken.Literal

// 	if p.peekTokenIs(DROP) {

// 		if !p.expectPeek(CONSTRAINT) {

// 		}

// 	}

// 	return stmt, nil
// }

func (p *Parser) parseAlterProcedureStatement() (*ast.AlterProcedureStatement, error) {
	stmt := &ast.AlterProcedureStatement{}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}
	stmt.Name = p.curToken.Literal

	if p.peekTokenIs(LPAREN) {
		p.nextToken()
		p.nextToken()

		parameters, err := p.parseColumnDefinitions()
		if err != nil {
			return nil, err
		}
		stmt.Parameters = parameters

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

		bodyBuilder.WriteString(p.curToken.Literal)
		bodyBuilder.WriteString(" ")

		if p.peekTokenIs(SEMICOLON) {
			p.nextToken()
			bodyBuilder.WriteString("; ")
		}
	}

	return bodyBuilder.String(), true, nil
}

func (p *Parser) parseColumnDefinitions() ([]database.Column, error) {
	columns := []database.Column{}

	if !p.expectPeek(IDENT) && !p.expectPeek(VARIABLE) {
		return nil, fmt.Errorf("expected identifier or variable, got %s", p.curToken.Literal)
	}

	for {
		col := p.parseColumnDefinition()
		if col == nil {
			return nil, fmt.Errorf("expected column definition, got %s", p.curToken.Literal)
		}
		columns = append(columns, *col)

		if !p.peekTokenIs(COMMA) {
			break
		}

		p.nextToken()
		if !p.expectPeek(IDENT) {
			if p.peekTokenIs(FOREIGN) {
				break
			}

			return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
		}
	}

	return columns, nil
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
		col.IsNullable = false
	}

	return col
}

func (p *Parser) parseExecStatement() (*ast.ExecStatement, error) {
	stmt := &ast.ExecStatement{}

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

// Precedence levels
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

var precedences = map[TokenType]int{
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
			p.nextToken()
			leftExpr = p.parseBinaryExpression(leftExpr)
		case ASSIGN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN, GREATER_THAN_OR_EQ:
			if precedence >= EQUALS {
				return leftExpr
			}
			p.nextToken()
			leftExpr = p.parseComparisonExpression(leftExpr)
		case AND, OR:
			if precedence >= LOGICAL {
				return leftExpr
			}
			p.nextToken()
			leftExpr = p.parseLogicalExpression(leftExpr)
		default:
			return leftExpr
		}
	}

	return leftExpr
}

func (p *Parser) parseLiteral() ast.Expression {
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

func (p *Parser) parseIdentifier() ast.Expression {
	return &ast.Identifier{Value: p.curToken.Literal}
}

func (p *Parser) parseVariable() ast.Expression {
	name := p.curToken.Literal[1:]
	return &ast.VariableExpression{Name: name}
}

func (p *Parser) parseAssignment() ast.Expression {
	expr := &ast.AssignmentExpression{
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

func (p *Parser) parseBinaryExpression(left ast.Expression) ast.Expression {
	operator := p.curToken.Literal
	precedence := p.curPrecedence()

	p.nextToken()
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

	p.nextToken()
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

	p.nextToken()
	right := p.parseExpressionWithPrecedence(precedence)

	return &ast.AssignmentExpression{
		Left:  left,
		Op:    operator,
		Right: right,
	}
}

func (p *Parser) parseAllExpression() ast.Expression {
	return &ast.AllExpression{}
}

func (p *Parser) parseValueLists() [][]ast.Expression {
	valueLists := [][]ast.Expression{}

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
