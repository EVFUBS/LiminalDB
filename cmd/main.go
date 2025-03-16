package main

import (
	"LiminalDb/internal/sqlparser"
	"fmt"
)

func main() {
	sqlparser.Repl()
	//test()
}

func test() {
	sql := "select name from test"

	lexer := sqlparser.NewLexer(sql)
	parser := sqlparser.NewParser(lexer)
	evaluator := sqlparser.NewEvaluator(parser)

	result, err := evaluator.Execute(sql)
	if err != nil {
		panic(err)
	}

	fmt.Println(result)
}
