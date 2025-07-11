package integration

import (
	"LiminalDb/internal/database"
	"testing"
)

func TestExpressionPrecedence(t *testing.T) {
	defer cleanupDB(t)

	_, err := execute("CREATE TABLE math_test (id int primary key, value int)")
	if err != nil {
		t.Fatalf("Failed to create table for expression test: %v", err)
	}

	_, err = execute("INSERT INTO math_test (id, value) VALUES (1, 10)")
	if err != nil {
		t.Fatalf("Failed to insert row for expression test: %v", err)
	}

	selectSQL1 := "SELECT id, value FROM math_test WHERE value = 2 + 3 * 4"
	result0, err1 := execute(selectSQL1)
	if err1 != nil {
		t.Fatalf("Failed to execute expression query 0: %v", err1)
	}

	resultSlice0, ok := result0.(*database.QueryResult)
	if !ok {
		t.Fatalf("SELECT result is not of expected type *database.QueryResult, got %T", result0)
	}

	if len(resultSlice0.Rows) != 0 {
		t.Errorf("Expected no rows for value = 14, got %v", resultSlice0.Rows)
	}

	_, err = execute("INSERT INTO math_test (id, value) VALUES (2, 14)")
	if err != nil {
		t.Fatalf("Failed to insert row with value 14: %v", err)
	}

	result1, err1 := execute(selectSQL1)
	if err1 != nil {
		t.Fatalf("Failed to execute expression query 1: %v", err1)
	}

	resultSlice1, ok := result1.(*database.QueryResult)
	if !ok {
		t.Fatalf("SELECT result is not of expected type *database.QueryResult, got %T", result1)
	}

	if len(resultSlice1.Rows) != 1 || resultSlice1.Rows[0][0] != int64(2) {
		t.Errorf("Expected row with id 2, got %v", resultSlice1.Rows)
	}

	selectSQL2 := "SELECT id FROM math_test WHERE value = 10 + 20 / 5"
	result2, err2 := execute(selectSQL2)
	if err2 != nil {
		t.Fatalf("Failed to execute expression query 2: %v", err2)
	}

	resultSlice2, ok := result2.(*database.QueryResult)
	if !ok {
		t.Fatalf("SELECT result is not of expected type *database.QueryResult, got %T", result2)
	}

	if len(resultSlice2.Rows) != 1 || resultSlice2.Rows[0][0] != int64(2) {
		t.Errorf("Expected row with id 2, got %v", resultSlice2.Rows)
	}

	_, err = execute("INSERT INTO math_test (id, value) VALUES (3, 26)")
	if err != nil {
		t.Fatalf("Failed to insert row with value 26: %v", err)
	}

	selectSQL3 := "SELECT id FROM math_test WHERE value = 2 * 3 + 4 * 5"
	result3, err3 := execute(selectSQL3)
	if err3 != nil {
		t.Fatalf("Failed to execute expression query 3: %v", err3)
	}

	resultSlice3, ok := result3.(*database.QueryResult)
	if !ok {
		t.Fatalf("SELECT result is not of expected type *database.QueryResult, got %T", result3)
	}

	if len(resultSlice3.Rows) != 1 || resultSlice3.Rows[0][0] != int64(3) {
		t.Errorf("Expected row with id 3, got %v", resultSlice3.Rows)
	}
}
