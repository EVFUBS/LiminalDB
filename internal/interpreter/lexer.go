package interpreter

import "strings"

type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           byte
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
}

func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	if l.ch == 0 {
		tok.Type = EOF
		tok.Literal = ""
		return tok
	}

	switch l.ch {
	case '@':
		l.readChar()
		tok.Type = VARIABLE
		tok.Literal = "@" + l.readIdentifier()
		return tok
	case '=':
		tok = newToken(ASSIGN, l.ch)
	case ';':
		tok = newToken(SEMICOLON, l.ch)
	case '(':
		tok = newToken(LPAREN, l.ch)
	case ')':
		tok = newToken(RPAREN, l.ch)
	case ',':
		tok = newToken(COMMA, l.ch)
	case '+':
		tok = newToken(PLUS, l.ch)
	case '*':
		tok = newToken(ALL, l.ch)
	case '\'':
		tok.Type = STRING
		tok.Literal = l.readString()
		return tok
	case '<':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok.Type = LESS_THAN_OR_EQ
			tok.Literal = string(ch) + string(l.ch)
			l.readChar()
			return tok
		}
		tok = newToken(LESS_THAN, l.ch)
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok.Type = GREATER_THAN_OR_EQ
			tok.Literal = string(ch) + string(l.ch)
			l.readChar()
			return tok
		}
		tok = newToken(GREATER_THAN, l.ch)
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return l.readNumberToken()
	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			if tok.Literal == "true" || tok.Literal == "false" {
				tok.Type = BOOL
				return tok
			}
			tok.Type = LookupIdent(tok.Literal)
			return tok
		} else {
			tok = newToken(ILLEGAL, l.ch)
		}
	}

	l.readChar()
	return tok
}

func (l *Lexer) readIdentifier() string {
	position := l.position
	for isAlphanumeric(l.ch) {
		l.readChar()
	}
	return l.input[position:l.position]
}

func (l *Lexer) readNumber() string {
	position := l.position
	for isDigit(l.ch) {
		l.readChar()
	}
	return l.input[position:l.position]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func newToken(tokenType TokenType, ch byte) Token {
	return Token{Type: tokenType, Literal: string(ch)}
}

func isLetter(ch byte) bool {
	return ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') || ch == '_'
}

func isAlphanumeric(ch byte) bool {
	return isLetter(ch) || isDigit(ch)
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

type TokenType string

type Token struct {
	Type    TokenType
	Literal string
}

const (
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"

	// Identifiers + literals
	IDENT  = "IDENT"  // add, foobar, x, y, ...
	INT    = "INT"    // 1343456
	STRING = "STRING" // "foo bar"
	FLOAT  = "FLOAT"  // 123.456
	BOOL   = "BOOL"   // true, false
	ALL    = "*"

	// Types
	INTTYPE    = "INT"
	FLOATTYPE  = "FLOAT"
	BOOLTYPE   = "BOOL"
	STRINGTYPE = "STRING"

	// Operators
	ASSIGN = "="
	PLUS   = "+"

	// Comparison Operators
	LESS_THAN          = "<"
	LESS_THAN_OR_EQ    = "<="
	GREATER_THAN       = ">"
	GREATER_THAN_OR_EQ = ">="

	// Delimiters
	COMMA     = ","
	SEMICOLON = ";"
	LPAREN    = "("
	RPAREN    = ")"

	// Keywords
	SELECT     = "SELECT"
	FROM       = "FROM"
	WHERE      = "WHERE"
	INSERT     = "INSERT"
	INTO       = "INTO"
	VALUES     = "VALUES"
	CREATE     = "CREATE"
	TABLE      = "TABLE"
	DROP       = "DROP"
	NULL       = "NULL"
	NOT        = "NOT"
	DELETE     = "DELETE"
	DESC       = "DESC"
	PRIMARY    = "PRIMARY"
	KEY        = "KEY"
	FOREIGN    = "FOREIGN"
	REFERENCES = "REFERENCES"
	ON         = "ON"

	// Stored Procedure Keywords
	PROCEDURE = "PROCEDURE"
	ALTER     = "ALTER"
	AS        = "AS"
	BEGIN     = "BEGIN"
	END       = "END"
	EXEC      = "EXEC"

	// Variables
	VARIABLE = "@" // For variables like @user_id
)

var keywords = map[string]TokenType{
	"select":     SELECT,
	"from":       FROM,
	"where":      WHERE,
	"insert":     INSERT,
	"into":       INTO,
	"values":     VALUES,
	"true":       BOOL,
	"false":      BOOL,
	"create":     CREATE,
	"table":      TABLE,
	"drop":       DROP,
	"int":        INTTYPE,
	"float":      FLOATTYPE,
	"bool":       BOOLTYPE,
	"string":     STRINGTYPE,
	"null":       NULL,
	"not":        NOT,
	"delete":     DELETE,
	"desc":       DESC,
	"*":          ALL,
	"primary":    PRIMARY,
	"key":        KEY,
	"foreign":    FOREIGN,
	"references": REFERENCES,
	"on":         ON,
	"procedure":  PROCEDURE,
	"alter":      ALTER,
	"as":         AS,
	"begin":      BEGIN,
	"end":        END,
	"exec":       EXEC,
	"variable":   VARIABLE,
	"<":          LESS_THAN,
	"<=":         LESS_THAN_OR_EQ,
	">":          GREATER_THAN,
	">=":         GREATER_THAN_OR_EQ,
}

func LookupIdent(ident string) TokenType {
	identLower := strings.ToLower(ident)
	if tok, ok := keywords[identLower]; ok {
		return tok
	}
	return IDENT
}

func (l *Lexer) readString() string {
	position := l.position + 1 // Skip the opening quote
	for {
		l.readChar()
		if l.ch == '\'' || l.ch == 0 {
			break
		}
	}

	value := l.input[position:l.position]
	l.readChar()

	return value
}

func (l *Lexer) readNumberToken() Token {
	var tok Token
	startPos := l.position
	isFloat := false

	// Read the integer part
	for isDigit(l.ch) {
		l.readChar()
	}

	// Check for decimal point
	if l.ch == '.' {
		isFloat = true
		l.readChar()
		// Read decimal places
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	if isFloat {
		tok.Type = FLOAT
	} else {
		tok.Type = INT
	}
	tok.Literal = l.input[startPos:l.position]
	return tok
}

func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}
