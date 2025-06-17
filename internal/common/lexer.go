package common

const (
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"

	// Identifiers + literals
	IDENT  = "IDENT"  // add, foobar, x, y, ...
	INT    = "INT"    // 1343456
	STRING = "STRING" // "foo bar"
	FLOAT  = "FLOAT"  // 123.456
	BOOL   = "BOOL"   // true, false
	ALL    = "ALL"    // For SELECT * queries

	// Types
	INTTYPE    = "INT"
	FLOATTYPE  = "FLOAT"
	BOOLTYPE   = "BOOL"
	STRINGTYPE = "STRING"

	// Operators
	ASSIGN   = "="
	PLUS     = "+"
	MINUS    = "-"
	MULTIPLY = "*"
	DIVIDE   = "/"

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
	UPDATE     = "UPDATE"
	SET        = "SET"
	NULL       = "NULL"
	NOT        = "NOT"
	DELETE     = "DELETE"
	DESC       = "DESC"
	PRIMARY    = "PRIMARY"
	KEY        = "KEY"
	FOREIGN    = "FOREIGN"
	REFERENCES = "REFERENCES"
	ON         = "ON"
	INDEX      = "INDEX"
	UNIQUE     = "UNIQUE"
	SHOW       = "SHOW"
	INDEXES    = "INDEXES"
	AND        = "AND"
	OR         = "OR"
	CONSTRAINT = "CONSTRAINT"
	ADD        = "ADD"
	COLUMN     = "COLUMN"

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

var LogicalOperators = map[string]bool{
	AND: true,
	OR:  true,
}
