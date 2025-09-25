package integration

import (
	"LiminalDb/internal/database"
	"LiminalDb/internal/database/serializer"
	"bytes"
	"testing"
	"time"
)

func TestSerializeHeader(t *testing.T) {
	header := database.FileHeader{
		Magic:   database.MagicNumber,
		Version: database.CurrentVersion,
	}

	serializer := serializer.BinarySerializer{}
	data, err := serializer.SerializeHeader(header)
	if err != nil {
		t.Fatalf("Failed to serialize header: %v", err)
	}

	reader := bytes.NewReader(data)
	deserialized, err := serializer.DeserializeHeader(reader)
	if err != nil {
		t.Fatalf("Failed to deserialize header: %v", err)
	}

	if deserialized.Magic != header.Magic {
		t.Errorf("Magic number mismatch: got %v, want %v", deserialized.Magic, header.Magic)
	}
	if deserialized.Version != header.Version {
		t.Errorf("Version mismatch: got %v, want %v", deserialized.Version, header.Version)
	}
}

func TestSerializeMetadata(t *testing.T) {
	metadata := database.TableMetadata{
		Name:        "test_table",
		ColumnCount: 3,
		Columns: []database.Column{
			{
				Name:         "id",
				DataType:     database.TypeInteger64,
				IsNullable:   false,
				IsPrimaryKey: true,
			},
			{
				Name:       "name",
				DataType:   database.TypeString,
				Length:     100,
				IsNullable: true,
			},
			{
				Name:       "age",
				DataType:   database.TypeInteger64,
				IsNullable: false,
			},
		},
		Indexes: []database.IndexMetadata{
			{
				Name:      "pk_test_table",
				Columns:   []string{"id"},
				IsUnique:  true,
				IsPrimary: true,
			},
			{
				Name:      "idx_name",
				Columns:   []string{"name"},
				IsUnique:  false,
				IsPrimary: false,
			},
			{
				Name:      "idx_composite",
				Columns:   []string{"name", "age"},
				IsUnique:  true,
				IsPrimary: false,
			},
		},
	}

	serializer := serializer.BinarySerializer{}
	data, _, err := serializer.SerializeMetadata(metadata)
	if err != nil {
		t.Fatalf("Failed to serialize metadata: %v", err)
	}

	reader := bytes.NewReader(data)
	deserialized, err := serializer.DeserializeMetadata(reader)
	if err != nil {
		t.Fatalf("Failed to deserialize metadata: %v", err)
	}

	if deserialized.Name != metadata.Name {
		t.Errorf("Table name mismatch: got %v, want %v", deserialized.Name, metadata.Name)
	}
	if deserialized.ColumnCount != metadata.ColumnCount {
		t.Errorf("Column count mismatch: got %v, want %v", deserialized.ColumnCount, metadata.ColumnCount)
	}
	if len(deserialized.Columns) != len(metadata.Columns) {
		t.Errorf("Columns length mismatch: got %v, want %v", len(deserialized.Columns), len(metadata.Columns))
	}

	// Test column properties
	for i, col := range deserialized.Columns {
		if col.Name != metadata.Columns[i].Name {
			t.Errorf("Column %d name mismatch: got %v, want %v", i, col.Name, metadata.Columns[i].Name)
		}
		if col.DataType != metadata.Columns[i].DataType {
			t.Errorf("Column %d data type mismatch: got %v, want %v", i, col.DataType, metadata.Columns[i].DataType)
		}
		if col.Length != metadata.Columns[i].Length {
			t.Errorf("Column %d length mismatch: got %v, want %v", i, col.Length, metadata.Columns[i].Length)
		}
		if col.IsNullable != metadata.Columns[i].IsNullable {
			t.Errorf("Column %d nullable mismatch: got %v, want %v", i, col.IsNullable, metadata.Columns[i].IsNullable)
		}
		if col.IsPrimaryKey != metadata.Columns[i].IsPrimaryKey {
			t.Errorf("Column %d primary key mismatch: got %v, want %v", i, col.IsPrimaryKey, metadata.Columns[i].IsPrimaryKey)
		}
	}

	// Test indexes
	if len(deserialized.Indexes) != len(metadata.Indexes) {
		t.Errorf("Indexes length mismatch: got %v, want %v", len(deserialized.Indexes), len(metadata.Indexes))
	} else {
		for i, idx := range deserialized.Indexes {
			if idx.Name != metadata.Indexes[i].Name {
				t.Errorf("Index %d name mismatch: got %v, want %v", i, idx.Name, metadata.Indexes[i].Name)
			}
			if idx.IsUnique != metadata.Indexes[i].IsUnique {
				t.Errorf("Index %d unique flag mismatch: got %v, want %v", i, idx.IsUnique, metadata.Indexes[i].IsUnique)
			}
			if idx.IsPrimary != metadata.Indexes[i].IsPrimary {
				t.Errorf("Index %d primary flag mismatch: got %v, want %v", i, idx.IsPrimary, metadata.Indexes[i].IsPrimary)
			}
			if len(idx.Columns) != len(metadata.Indexes[i].Columns) {
				t.Errorf("Index %d columns length mismatch: got %v, want %v", i, len(idx.Columns), len(metadata.Indexes[i].Columns))
			} else {
				for j, col := range idx.Columns {
					if col != metadata.Indexes[i].Columns[j] {
						t.Errorf("Index %d column %d mismatch: got %v, want %v", i, j, col, metadata.Indexes[i].Columns[j])
					}
				}
			}
		}
	}
}

func TestSerializeRow(t *testing.T) {
	columns := []database.Column{
		{Name: "id", DataType: database.TypeInteger64},
		{Name: "name", DataType: database.TypeString, Length: 100},
		{Name: "age", DataType: database.TypeInteger64},
		{Name: "active", DataType: database.TypeBoolean},
		{Name: "created_at", DataType: database.TypeDatetime},
	}

	now := time.Now()
	row := []any{
		int64(1),
		"John Doe",
		int64(30),
		true,
		now,
	}

	serializer := serializer.BinarySerializer{}
	data, err := serializer.SerializeRow(row, columns)
	if err != nil {
		t.Fatalf("Failed to serialize row: %v", err)
	}

	reader := bytes.NewReader(data)
	deserialized, err := serializer.DeserializeRow(reader, columns)
	if err != nil {
		t.Fatalf("Failed to deserialize row: %v", err)
	}

	if len(deserialized) != len(row) {
		t.Fatalf("Row length mismatch: got %v, want %v", len(deserialized), len(row))
	}

	// Test each value
	if deserialized[0].(int64) != row[0].(int64) {
		t.Errorf("ID mismatch: got %v, want %v", deserialized[0], row[0])
	}
	if deserialized[1].(string) != row[1].(string) {
		t.Errorf("Name mismatch: got %v, want %v", deserialized[1], row[1])
	}
	if deserialized[2].(int64) != row[2].(int64) {
		t.Errorf("Age mismatch: got %v, want %v", deserialized[2], row[2])
	}
	if deserialized[3].(bool) != row[3].(bool) {
		t.Errorf("Active mismatch: got %v, want %v", deserialized[3], row[3])
	}
	// Compare timestamps with a small tolerance
	deserializedTime := deserialized[4].(time.Time)
	originalTime := row[4].(time.Time)
	if deserializedTime.Unix() != originalTime.Unix() {
		t.Errorf("Created at mismatch: got %v, want %v", deserializedTime, originalTime)
	}
}

func TestSerializeTable(t *testing.T) {
	table := &database.Table{
		Header: database.FileHeader{
			Magic:   database.MagicNumber,
			Version: database.CurrentVersion,
		},
		Metadata: database.TableMetadata{
			Name:        "test_table",
			ColumnCount: 3,
			Columns: []database.Column{
				{
					Name:         "id",
					DataType:     database.TypeInteger64,
					IsNullable:   false,
					IsPrimaryKey: true,
				},
				{
					Name:       "name",
					DataType:   database.TypeString,
					Length:     100,
					IsNullable: true,
				},
				{
					Name:       "age",
					DataType:   database.TypeInteger64,
					IsNullable: false,
				},
			},
		},
		Data: [][]any{
			{int64(1), "John Doe", int64(30)},
			{int64(2), "Jane Smith", int64(25)},
		},
	}

	serializer := serializer.BinarySerializer{}
	data, err := serializer.SerializeTable(table)
	if err != nil {
		t.Fatalf("Failed to serialize table: %v", err)
	}

	deserialized, err := serializer.DeserializeTable(data)
	if err != nil {
		t.Fatalf("Failed to deserialize table: %v", err)
	}

	// Test header
	if deserialized.Header.Magic != table.Header.Magic {
		t.Errorf("Magic number mismatch: got %v, want %v", deserialized.Header.Magic, table.Header.Magic)
	}
	if deserialized.Header.Version != table.Header.Version {
		t.Errorf("Version mismatch: got %v, want %v", deserialized.Header.Version, table.Header.Version)
	}

	// Test metadata
	if deserialized.Metadata.Name != table.Metadata.Name {
		t.Errorf("Table name mismatch: got %v, want %v", deserialized.Metadata.Name, table.Metadata.Name)
	}
	if deserialized.Metadata.ColumnCount != table.Metadata.ColumnCount {
		t.Errorf("Column count mismatch: got %v, want %v", deserialized.Metadata.ColumnCount, table.Metadata.ColumnCount)
	}

	// Test data
	if len(deserialized.Data) != len(table.Data) {
		t.Fatalf("Data length mismatch: got %v, want %v", len(deserialized.Data), len(table.Data))
	}

	for i, row := range deserialized.Data {
		if len(row) != len(table.Data[i]) {
			t.Fatalf("Row %d length mismatch: got %v, want %v", i, len(row), len(table.Data[i]))
		}

		for j, val := range row {
			switch v := val.(type) {
			case int64:
				if v != table.Data[i][j].(int64) {
					t.Errorf("Row %d, Column %d mismatch: got %v, want %v", i, j, v, table.Data[i][j])
				}
			case string:
				if v != table.Data[i][j].(string) {
					t.Errorf("Row %d, Column %d mismatch: got %v, want %v", i, j, v, table.Data[i][j])
				}
			default:
				t.Errorf("Unexpected type in row %d, column %d: %T", i, j, val)
			}
		}
	}
}

func TestInvalidDataTypes(t *testing.T) {
	columns := []database.Column{
		{Name: "id", DataType: database.TypeInteger64},
	}

	// Test invalid data type
	row := []any{"not an integer"}
	serializer := serializer.BinarySerializer{}
	_, err := serializer.SerializeRow(row, columns)
	if err == nil {
		t.Error("Expected error for invalid data type, got nil")
	}
}

func TestStringLengthExceeded(t *testing.T) {
	columns := []database.Column{
		{Name: "name", DataType: database.TypeString, Length: 5},
	}

	// Test string exceeding length
	row := []any{"too long string"}
	serializer := serializer.BinarySerializer{}
	_, err := serializer.SerializeRow(row, columns)
	if err == nil {
		t.Error("Expected error for string exceeding length, got nil")
	}
}
