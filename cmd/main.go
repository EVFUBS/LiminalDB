package main

import (
	"LiminalDb/internal/db"
	"LiminalDb/internal/sqlparser"
	"fmt"
)

func main() {
	serializer := db.BinarySerializer{}

	table := db.Table{
		Header: db.FileHeader{
			Magic:          db.MagicNumber,
			Version:        db.CurrentVersion,
			MetadataLength: 0,
		},
		Metadata: db.TableMetadata{
			Name:        "test",
			ColumnCount: 4,
			Columns: []db.Column{
				{Name: "name", DataType: db.TypeString, Length: 300, IsNullable: false},
				{Name: "number", DataType: db.TypeInteger64, Length: 0, IsNullable: false},
				{Name: "boolean", DataType: db.TypeBoolean, Length: 0, IsNullable: false},
				{Name: "float", DataType: db.TypeFloat64, Length: 0, IsNullable: false},
			},
			RowCount:   2,
			DataOffset: 0,
		},
		Data: [][]interface{}{
			{"test", int64(123), true, float64(123.456)},
			{"test2", int64(456), false, float64(789.123)},
		},
	}

	tableBytes, err := serializer.SerializeTable(table)
	if err != nil {
		panic(err)
	}

	deserialisedTable, err := serializer.DeserializeTable(tableBytes)
	if err != nil {
		panic(err)
	}

	err = serializer.WriteTableToFile(table, "test.bin")
	if err != nil {
		panic(err)
	}

	deserialisedTable, err = serializer.ReadTableFromFile("test.bin")

	fmt.Println(deserialisedTable)

	sql := "SELECT Name FROM test WHERE name = 'test'"

	lexer := sqlparser.NewLexer(sql)
	parser := sqlparser.NewParser(lexer)
	evaluator := sqlparser.NewEvaluator(parser)

	result, err := evaluator.Execute(sql)
	if err != nil {
		panic(err)
	}

	fmt.Println(result)
}
