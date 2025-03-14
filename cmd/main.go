package main

import (
	"LiminalDb/internal/db"
	"LiminalDb/internal/sqlparser"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	repl()
}

func repl() {
	fmt.Println("Welcome to LiminalDB")
	fmt.Println("Enter SQL commands, or type 'exit' to quit")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := scanner.Text()

		if strings.ToLower(input) == "exit" {
			break
		}

		if input == "" {
			continue
		}

		lexer := sqlparser.NewLexer(input)
		parser := sqlparser.NewParser(lexer)
		evaluator := sqlparser.NewEvaluator(parser)

		result, err := evaluator.Execute(input)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		switch v := result.(type) {
		case [][]interface{}:
			if len(v) == 0 {
				fmt.Println("No results")
				continue
			}
			for _, row := range v {
				fmt.Println(row)
			}
		case db.TableMetadata:
			fmt.Printf("\nTable: %s\n", v.Name)
			fmt.Println("+-----------------+-----------------+")
			fmt.Printf("| %-15s | %-15s |\n", "Column", "Type")
			fmt.Println("+-----------------+-----------------+")
			for _, column := range v.Columns {
				typeStr := column.DataType.String()
				if column.DataType == db.TypeString {
					typeStr = fmt.Sprintf("%s(%d)", typeStr, column.Length)
				}
				fmt.Printf("| %-15s | %-15s |\n", column.Name, typeStr)
			}
			fmt.Println("+-----------------+-----------------+")
		default:
			fmt.Println(result)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}

// func main() {
// 	sql := "CREATE TABLE users2 (id INT, name string(100))"

// 	lexer := sqlparser.NewLexer(sql)
// 	parser := sqlparser.NewParser(lexer)
// 	evaluator := sqlparser.NewEvaluator(parser)

// 	result, err := evaluator.Execute(sql)
// 	if err != nil {
// 		panic(err)
// 	}

// 	fmt.Println(result)
// }
