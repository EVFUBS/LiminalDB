package main

import (
	"LiminalDb/db"
	"fmt"
)

func main() {

	metadata := db.TableMetadata{
		Name:        "users",
		ColumnCount: 3,
		Columns: []db.Column{
			{Name: "Id", DataType: db.TypeInteger, Length: 0, IsNullable: false},
			{Name: "email", DataType: db.TypeString, Length: 100, IsNullable: false},
			{Name: "password", DataType: db.TypeString, Length: 100, IsNullable: false},
		},
		RowCount:   0,
		DataOffset: 0,
	}

	testFileHeader := db.FileHeader{
		Magic:          db.MagicNumber,
		Version:        db.CurrentVersion,
		MetadataLength: 0,
	}

	serializer := db.BinarySerializer{}

	bytes, err := serializer.SerializeHeader(testFileHeader)

	if err != nil {
		panic(err)
	}

	deserializedHeader, err := serializer.DeserializeHeader(bytes)

	fmt.Println(testFileHeader)
	fmt.Println(bytes)
	fmt.Println(deserializedHeader)

}
