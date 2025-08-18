package parser

import (
	. "LiminalDb/internal/common"
	"LiminalDb/internal/database"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func (p *Parser) NextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.Lexer.NextToken()
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
		p.NextToken()
		if p.curTokenIs(END) {
			break
		}
		if p.curToken.Type == EOF {
			return "", false, fmt.Errorf("expected end, got %s", p.curToken.Literal)
		}

		bodyBuilder.WriteString(p.curToken.Literal)
		bodyBuilder.WriteString(" ")

		if p.peekTokenIs(SEMICOLON) {
			p.NextToken()
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

		p.NextToken()
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

	if !p.expectPeek(INT) && !p.expectPeek(FLOAT) &&
		!p.expectPeek(STRING) && !p.expectPeek(BOOL) && !p.expectPeek(DATETIME) {
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
		p.NextToken()
		p.NextToken()

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

	if p.peekTokenIs(DEFAULT) {
		p.NextToken()
		p.NextToken()
		switch p.curToken.Type {
		case STRING:
			col.DefaultValue = p.curToken.Literal
		case INT:
			val, err := strconv.Atoi(p.curToken.Literal)
			if err == nil {
				col.DefaultValue = val
			}
		case FLOAT:
			val, err := strconv.ParseFloat(p.curToken.Literal, 64)
			if err == nil {
				col.DefaultValue = val
			}
		case BOOL:
			val, err := strconv.ParseBool(p.curToken.Literal)
			if err == nil {
				col.DefaultValue = val
			}
		case DATETIME:
			// TODO: Support more date formats
			val, err := time.Parse("2006-01-02 15:04:05", p.curToken.Literal)
			if err == nil {
				col.DefaultValue = val
			}
		default:
			col.DefaultValue = p.curToken.Literal
		}
	}

	if p.peekTokenIs(NOT) {
		p.NextToken()
		if !p.expectPeek(NULL) {
			return nil
		}
		col.IsNullable = false
	} else if p.peekTokenIs(NULL) {
		p.NextToken()
		col.IsNullable = true
	}

	if p.peekTokenIs(PRIMARY) {
		p.NextToken()
		if !p.expectPeek(KEY) {
			return nil
		}
		col.IsPrimaryKey = true
		col.IsNullable = false
	}

	return col
}

func (p *Parser) parseIdentifierList() []string {
	identifiers := []string{p.curToken.Literal}

	for p.peekTokenIs(COMMA) {
		p.NextToken()
		p.NextToken()
		identifiers = append(identifiers, p.curToken.Literal)
	}

	return identifiers
}