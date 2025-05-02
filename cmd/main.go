package main

import (
	"LiminalDb/internal/interpreter"
	"LiminalDb/internal/logger"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	setupLogger()
	logger.Info("Starting LiminalDB server")
	interpreter.Repl()
	logger.Info("Shutting down LiminalDB server")

	//test()
}

func test() {
	//sql := "CREATE PROCEDURE get_user(@user_id int) AS BEGIN SELECT * FROM users WHERE id = @user_id; END"
	//sql := "exec get_user(1)"
	//sql := "create table users (id int primary key, name string(100))"
	//sql := "INSERT INTO users (id, name) VALUES (1, 'John Doe')"

	sql := "select * from test where id < 2"

	lexer := interpreter.NewLexer(sql)
	parser := interpreter.NewParser(lexer)
	evaluator := interpreter.NewEvaluator(parser)
	result, err := evaluator.Execute(sql)
	if err != nil {
		logger.Error("Test execution failed: %v", err)
		panic(err)
	}

	fmt.Println(result)
}

func setupLogger() {
	logDir := filepath.Join("logs")
	if err := logger.Init(logger.INFO, logDir); err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
}
