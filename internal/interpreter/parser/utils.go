package parser

import (
	. "LiminalDb/internal/common"
	"LiminalDb/internal/database"
	l "LiminalDb/internal/interpreter/lexer"
	"fmt"
)

func convertTokenTypeToColumnType(tokenType l.TokenType) (database.ColumnType, error) {
	switch tokenType {
	case INT:
		return database.TypeInteger64, nil
	case FLOAT:
		return database.TypeFloat64, nil
	case STRING:
		return database.TypeString, nil
	case BOOL:
		return database.TypeBoolean, nil
	case DATETIME:
		return database.TypeDatetime, nil
	}

	return 0, fmt.Errorf("unsupported token type: %s", tokenType)
}
