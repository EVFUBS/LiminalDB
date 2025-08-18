package parser

import (
	"LiminalDb/internal/ast"
	. "LiminalDb/internal/common"
	"LiminalDb/internal/database"
	"fmt"
)

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
		p.NextToken()
		p.NextToken()
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

	p.NextToken()
	stmt.Columns = p.parseIdentifierList()

	if !p.expectPeek(RPAREN) {
		return nil, fmt.Errorf("expected right parenthesis, got %s", p.curToken.Literal)
	}

	if !p.expectPeek(VALUES) {
		return nil, fmt.Errorf("expected values, got %s", p.curToken.Literal)
	}

	p.NextToken()
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

	p.NextToken()

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

	p.NextToken()
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
		p.NextToken()
		if !p.expectPeek(KEY) {
			return nil, fmt.Errorf("expected key, got %s", p.curToken.Literal)
		}

		p.NextToken()
		p.NextToken()

		columns := p.parseIdentifierList()
		p.NextToken()

		if !p.expectPeek(REFERENCES) {
			return nil, fmt.Errorf("expected references, got %s", p.curToken.Literal)
		}
		p.NextToken()

		referencedTable := p.curToken.Literal

		if !p.expectPeek(LPAREN) {
			return nil, fmt.Errorf("expected left parenthesis, got %s", p.curToken.Literal)
		}

		p.NextToken()
		referencedColumns := p.parseIdentifierList()

		stmt.ForeignKeys = append(stmt.ForeignKeys, database.ForeignKeyConstraint{
			Name:            fmt.Sprintf("FK_%s_%s", stmt.TableName, columns[0]),
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
		p.NextToken()
		p.NextToken()
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
	p.NextToken()
	switch p.curToken.Type {
	case PROCEDURE:
		return p.parseAlterProcedureStatement()
	case TABLE:
		return p.parseAlterTableStatement()
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
		p.NextToken()

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

func (p *Parser) parseAlterTableStatement() (*ast.AlterTableStatement, error) {
	stmt := &ast.AlterTableStatement{}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}
	stmt.TableName = p.curToken.Literal

	if p.peekTokenIs(DROP) {
		p.NextToken()
		stmt.DropConstraint = true
		if !p.expectPeek(CONSTRAINT) {
			return nil, fmt.Errorf("expected constraint, got %s", p.curToken.Literal)
		}

		if !p.expectPeek(IDENT) {
			return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
		}

		stmt.ConstraintName = p.curToken.Literal
	}

	if p.peekTokenIs(ADD) {
		p.NextToken()

		if !p.expectPeek(COLUMN) {
			return nil, fmt.Errorf("expected column, got %s", p.curToken.Literal)
		}

		p.NextToken()

		stmt.AddColumn = true
		columnToAdd := p.parseColumnDefinition()
		stmt.Columns = append(stmt.Columns, *columnToAdd)
	}

	// TODO: Support more than drop constraint and add constraint

	return stmt, nil
}

func (p *Parser) parseAlterProcedureStatement() (*ast.AlterProcedureStatement, error) {
	stmt := &ast.AlterProcedureStatement{}

	if !p.expectPeek(IDENT) {
		return nil, fmt.Errorf("expected identifier, got %s", p.curToken.Literal)
	}

	stmt.Name = p.curToken.Literal

	if p.peekTokenIs(LPAREN) {
		p.NextToken()
		p.NextToken()

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
