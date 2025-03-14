package main

import (
	"LiminalDb/internal/sqlparser"
	"bufio"
	"fmt"
	"os"
	"strings"
)

// func main() {
// 	serializer := db.BinarySerializer{}

// 	table := db.Table{
// 		Header: db.FileHeader{
// 			Magic:          db.MagicNumber,
// 			Version:        db.CurrentVersion,
// 			MetadataLength: 0,
// 		},
// 		Metadata: db.TableMetadata{
// 			Name:        "test",
// 			ColumnCount: 4,
// 			Columns: []db.Column{
// 				{Name: "name", DataType: db.TypeString, Length: 300, IsNullable: false},
// 				{Name: "number", DataType: db.TypeInteger64, Length: 0, IsNullable: false},
// 				{Name: "boolean", DataType: db.TypeBoolean, Length: 0, IsNullable: false},
// 				{Name: "float", DataType: db.TypeFloat64, Length: 0, IsNullable: false},
// 			},
// 			RowCount:   2,
// 			DataOffset: 0,
// 		},
// 		Data: [][]interface{}{
// 			{"test", int64(123), true, float64(123.456)},
// 			{"test2", int64(456), false, float64(789.123)},
// 		},
// 	}

// 	err := serializer.WriteTableToFile(table, "test")
// 	if err != nil {
// 		panic(err)
// 	}

// 	sql := "INSERT INTO test (name, number, boolean, float) VALUES ('test3', 789, true, 123.456)"

// 	lexer := sqlparser.NewLexer(sql)
// 	parser := sqlparser.NewParser(lexer)
// 	evaluator := sqlparser.NewEvaluator(parser)

// 	result, err := evaluator.Execute(sql)
// 	if err != nil {
// 		panic(err)
// 	}

// 	fmt.Println(result)

// 	deserialisedTable, err := serializer.ReadTableFromFile("test")
// 	if err != nil {
// 		panic(err)
// 	}

// 	fmt.Println(deserialisedTable)

// 	selectSql := "SELECT Name, number FROM test WHERE name = 'test3'"

// 	selectLexer := sqlparser.NewLexer(selectSql)
// 	selectParser := sqlparser.NewParser(selectLexer)
// 	selectEvaluator := sqlparser.NewEvaluator(selectParser)

// 	selectResult, err := selectEvaluator.Execute(selectSql)
// 	if err != nil {
// 		panic(err)
// 	}

// 	fmt.Println(selectResult)

// }

func main() {
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
		default:
			fmt.Printf("Query executed successfully\n")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}
