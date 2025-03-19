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
	//sql := "CREATE PROCEDURE get_user(@user_id int) AS BEGIN SELECT * FROM users WHERE id = @user_id; END"
	sql := "exec get_user(1)"
	//sql := "create table users (id int primary key, name string(100))"
	//sql := "INSERT INTO users (id, name) VALUES (1, 'John Doe')"

	lexer := sqlparser.NewLexer(sql)
	parser := sqlparser.NewParser(lexer)
	evaluator := sqlparser.NewEvaluator(parser)

	result, err := evaluator.Execute(sql)
	if err != nil {
		panic(err)
	}

	fmt.Println(result)
}
