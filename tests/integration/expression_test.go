package integration

import (
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

	if result0.Err != nil {
		t.Fatalf("SELECT result is not expected to have error, got %v", result0.Err)
	}

	if len(result0.Data.Rows) != 0 {
		t.Errorf("Expected no rows for value = 14, got %v", result0.Data.Rows)
	}

	_, err = execute("INSERT INTO math_test (id, value) VALUES (2, 14)")
	if err != nil {
		t.Fatalf("Failed to insert row with value 14: %v", err)
	}

	result1, err1 := execute(selectSQL1)
	if err1 != nil {
		t.Fatalf("Failed to execute expression query 1: %v", err1)
	}

	if result1.Err != nil {
		t.Fatalf("SELECT result is not expected to have error, got %v", result1.Err)
	}

	if len(result0.Data.Rows) != 0 {
		t.Errorf("Expected no rows for value = 14, got %v", result0.Data.Rows)
	}

	selectSQL2 := "SELECT id FROM math_test WHERE value = 10 + 20 / 5"
	result2, err2 := execute(selectSQL2)
	if err2 != nil {
		t.Fatalf("Failed to execute expression query 2: %v", err2)
	}

	if result2.Err != nil {
		t.Fatalf("SELECT result is not expected to have error, got %v", result2.Err)
	}

	if len(result0.Data.Rows) != 0 {
		t.Errorf("Expected no rows for value = 14, got %v", result0.Data.Rows)
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

	if result3.Err != nil {
		t.Fatalf("SELECT result is not expected to have error, got %v", result3.Err)
	}

	if len(result0.Data.Rows) != 0 {
		t.Errorf("Expected no rows for value = 14, got %v", result0.Data.Rows)
	}
}
