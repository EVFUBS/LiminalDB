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
	sql := "Create table pkeytest (id int primary key, name string(100))"

	lexer := sqlparser.NewLexer(sql)
	parser := sqlparser.NewParser(lexer)
	evaluator := sqlparser.NewEvaluator(parser)

	result, err := evaluator.Execute(sql)
	if err != nil {
		panic(err)
	}

	fmt.Println(result)

	sql = "DESC TABLE pkeytest"

	lexer = sqlparser.NewLexer(sql)
	parser = sqlparser.NewParser(lexer)
	evaluator = sqlparser.NewEvaluator(parser)

	result, err = evaluator.Execute(sql)

	fmt.Println(result)
}
