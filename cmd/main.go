package main

import (
	"LiminalDb/db"
	"fmt"
)

func main() {
	serializer := db.BinarySerializer{}

	table := db.Table{
		Header: db.FileHeader{
			Magic:          0x42424242,
			Version:        1,
			MetadataLength: 0,
		},
		Metadata: db.TableMetadata{
			Name:        "test",
			ColumnCount: 2,
			Columns: []db.Column{
				{Name: "name", DataType: db.TypeString, Length: 300, IsNullable: false},
				{Name: "number", DataType: db.TypeInteger, Length: 0, IsNullable: false},
			},
			RowCount:   2,
			DataOffset: 0,
		},
		Data: [][]interface{}{
			{"test", int64(123)},
			{"test2", int64(456)},
		},
	}

	tableBytes, err := serializer.SerializeTable(table)
	if err != nil {
		panic(err)
	}

	fmt.Println(tableBytes)

	deserialisedTable, err := serializer.DeserializeTable(tableBytes)
	if err != nil {
		panic(err)
	}

	fmt.Println(deserialisedTable)

	fmt.Println(deserialisedTable.Header)
	fmt.Println(deserialisedTable.Metadata)
	fmt.Println(deserialisedTable.Data)
}
