package db

import (
	"os"
	"testing"
	"time"
)

func setupTestEnvironment(t *testing.T) {
	// Clean up any existing test files before each test
	if err := os.RemoveAll(DatabaseDir); err != nil {
		t.Fatalf("Failed to clean up test environment: %v", err)
	}
}

func createSampleTableMetadata() TableMetadata {
	return TableMetadata{
		Name: "test_table",
		Columns: []Column{
			{Name: "id", DataType: TypeInteger64, IsNullable: false},
			{Name: "name", DataType: TypeString, Length: 50, IsNullable: true},
			{Name: "age", DataType: TypeInteger64, IsNullable: true},
			{Name: "active", DataType: TypeBoolean, IsNullable: false},
			{Name: "created_at", DataType: TypeTimestamp, IsNullable: false},
		},
	}
}

func TestCreateTable(t *testing.T) {
	setupTestEnvironment(t)
	ops := &OperationsImpl{serializer: BinarySerializer{}}

	metadata := createSampleTableMetadata()
	err := ops.CreateTable(metadata)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table exists
	if _, err := os.Stat(getTableFilePath("test_table")); os.IsNotExist(err) {
		t.Error("Table file was not created")
	}

	// Read back metadata and verify
	readMetadata, err := ops.ReadMetadata("test_table")
	if err != nil {
		t.Fatalf("Failed to read metadata: %v", err)
	}

	if readMetadata.Name != metadata.Name {
		t.Errorf("Expected table name %s, got %s", metadata.Name, readMetadata.Name)
	}

	if len(readMetadata.Columns) != len(metadata.Columns) {
		t.Errorf("Expected %d columns, got %d", len(metadata.Columns), len(readMetadata.Columns))
	}
}

func TestWriteAndReadRows(t *testing.T) {
	setupTestEnvironment(t)
	ops := &OperationsImpl{serializer: BinarySerializer{}}

	// Create table
	metadata := createSampleTableMetadata()
	err := ops.CreateTable(metadata)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test data
	now := time.Now()
	testData := [][]interface{}{
		{int64(1), "John Doe", int64(30), true, now},
		{int64(2), "Jane Smith", int64(25), false, now},
	}

	// Write rows
	err = ops.WriteRows("test_table", testData)
	if err != nil {
		t.Fatalf("Failed to write rows: %v", err)
	}

	// Read all rows
	result, err := ops.ReadRows("test_table", []string{"*"}, nil)
	if err != nil {
		t.Fatalf("Failed to read rows: %v", err)
	}

	if len(result.Rows) != len(testData) {
		t.Errorf("Expected %d rows, got %d", len(testData), len(result.Rows))
	}

	// Test filter
	filterResult, err := ops.ReadRows("test_table", []string{"name", "age"},
		func(row []interface{}, cols []Column) (bool, error) {
			age := row[2].(int64)
			return age > 25, nil
		})
	if err != nil {
		t.Fatalf("Failed to read filtered rows: %v", err)
	}

	if len(filterResult.Rows) != 1 {
		t.Errorf("Expected 1 filtered row, got %d", len(filterResult.Rows))
	}
}

func TestDeleteRows(t *testing.T) {
	setupTestEnvironment(t)
	ops := &OperationsImpl{serializer: BinarySerializer{}}

	// Create table and insert test data
	metadata := createSampleTableMetadata()
	err := ops.CreateTable(metadata)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	now := time.Now()
	testData := [][]interface{}{
		{int64(1), "John Doe", int64(30), true, now},
		{int64(2), "Jane Smith", int64(25), false, now},
		{int64(3), "Bob Johnson", int64(35), true, now},
	}

	err = ops.WriteRows("test_table", testData)
	if err != nil {
		t.Fatalf("Failed to write rows: %v", err)
	}

	// Delete rows where age > 30
	deletedCount, err := ops.DeleteRows("test_table", func(row []interface{}, cols []Column) (bool, error) {
		age := row[2].(int64)
		return age > 30, nil
	})
	if err != nil {
		t.Fatalf("Failed to delete rows: %v", err)
	}

	if deletedCount != 1 {
		t.Errorf("Expected to delete 1 row, deleted %d", deletedCount)
	}

	// Verify remaining rows
	result, err := ops.ReadRows("test_table", []string{"*"}, nil)
	if err != nil {
		t.Fatalf("Failed to read rows after delete: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows after delete, got %d", len(result.Rows))
	}
}

func TestDropTable(t *testing.T) {
	setupTestEnvironment(t)
	ops := &OperationsImpl{serializer: BinarySerializer{}}

	// Create table
	metadata := createSampleTableMetadata()
	err := ops.CreateTable(metadata)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Drop table
	err = ops.DropTable("test_table")
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	// Verify table doesn't exist
	if _, err := os.Stat(getTableFilePath("test_table")); !os.IsNotExist(err) {
		t.Error("Table file still exists after drop")
	}
}

func TestReadMetadata(t *testing.T) {
	setupTestEnvironment(t)
	ops := &OperationsImpl{serializer: BinarySerializer{}}

	// Create table
	metadata := createSampleTableMetadata()
	err := ops.CreateTable(metadata)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Read metadata
	readMetadata, err := ops.ReadMetadata("test_table")
	if err != nil {
		t.Fatalf("Failed to read metadata: %v", err)
	}

	// Verify metadata fields
	if readMetadata.Name != metadata.Name {
		t.Errorf("Expected table name %s, got %s", metadata.Name, readMetadata.Name)
	}

	if len(readMetadata.Columns) != len(metadata.Columns) {
		t.Errorf("Expected %d columns, got %d", len(metadata.Columns), len(readMetadata.Columns))
	}

	// Verify column details
	for i, col := range metadata.Columns {
		readCol := readMetadata.Columns[i]
		if readCol.Name != col.Name {
			t.Errorf("Column %d: expected name %s, got %s", i, col.Name, readCol.Name)
		}
		if readCol.DataType != col.DataType {
			t.Errorf("Column %d: expected type %v, got %v", i, col.DataType, readCol.DataType)
		}
		if readCol.IsNullable != col.IsNullable {
			t.Errorf("Column %d: expected nullable %v, got %v", i, col.IsNullable, readCol.IsNullable)
		}
	}
}
