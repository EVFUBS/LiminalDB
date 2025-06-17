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
}

func test() {
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
