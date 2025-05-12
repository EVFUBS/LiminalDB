package integration

import (
	"LiminalDb/internal/database"
	"LiminalDb/internal/interpreter"
	"LiminalDb/internal/logger"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func execute(sql string) (any, error) {
	logger.SetupLogger()
	return interpreter.Execute(sql)
}

func cleanupDB(t *testing.T) {
	dbPath := "./db"
	if err := os.RemoveAll(dbPath); err != nil {
		t.Logf("Failed to clean up db directory: %v", err) // Use Logf for non-fatal errors in cleanup
	}
}

func TestCreateTable(t *testing.T) {
	defer cleanupDB(t)
	sql := "CREATE TABLE users (id int primary key, name string(100), active bool)"
	_, err := execute(sql)
	if err != nil {
		t.Fatalf("Failed to execute CREATE TABLE: %v", err)
	}

	tablePath := filepath.Join("./db", "users"+database.FileExtension)
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		t.Errorf("Table file was not created at %s", tablePath)
	}
}

func TestInsertRow(t *testing.T) {
	defer cleanupDB(t)
	_, err := execute("CREATE TABLE products (pid int primary key, pname string(50))")
	if err != nil {
		t.Fatalf("Failed to create table for insert test: %v", err)
	}

	insertSQL := "INSERT INTO products (pid, pname) VALUES (1, 'Test Product')"
	_, err = execute(insertSQL)
	if err != nil {
		t.Fatalf("Failed to execute INSERT: %v", err)
	}
}

func TestSelectRow(t *testing.T) {
	defer cleanupDB(t)
	_, err := execute("CREATE TABLE customers (cid int primary key, email string(100))")
	if err != nil {
		t.Fatalf("Failed to create table for select test: %v", err)
	}

	_, err = execute("INSERT INTO customers (cid, email) VALUES (101, 'test@example.com')")
	if err != nil {
		t.Fatalf("Failed to insert row for select test: %v", err)
	}

	selectSQL := "SELECT cid, email FROM customers WHERE cid = 101"
	result, err := execute(selectSQL)
	if err != nil {
		t.Fatalf("Failed to execute SELECT: %v", err)
	}

	expected := &database.QueryResult{
		Columns: []database.Column{
			{
				Name:         "cid",
				DataType:     database.TypeInteger64,
				Length:       0,
				IsNullable:   false,
				IsPrimaryKey: true,
			},
			{
				Name:         "email",
				DataType:     database.TypeString,
				Length:       100,
				IsNullable:   true,
				IsPrimaryKey: false,
			},
		},
		Rows: [][]any{
			{int64(101), "test@example.com"},
		},
	}

	resultSlice, ok := result.(*database.QueryResult)
	if !ok {
		t.Fatalf("SELECT result is not of expected type []map[string]any, got %T", result)
	}

	if !reflect.DeepEqual(resultSlice, expected) {
		t.Errorf("SELECT result mismatch. Expected %v, got %v", expected, resultSlice)
	}
}

func TestDropRow(t *testing.T) {
	defer cleanupDB(t)
	_, err := execute("CREATE TABLE orders (oid int primary key, item string(30))")
	if err != nil {
		t.Fatalf("Failed to create table for drop row test: %v", err)
	}

	_, err = execute("INSERT INTO orders (oid, item) VALUES (201, 'Sample Item')")
	if err != nil {
		t.Fatalf("Failed to insert row for drop row test: %v", err)
	}

	deleteSQL := "DELETE FROM orders WHERE oid = 201"
	_, err = execute(deleteSQL)
	if err != nil {
		t.Fatalf("Failed to execute DELETE: %v", err)
	}

	selectSQL := "SELECT oid FROM orders WHERE oid = 201"
	result, err := execute(selectSQL)
	if err != nil {
		t.Logf("SELECT after DELETE failed as expected (or returned no results): %v", err)
	} else {
		resultSlice, ok := result.(*database.QueryResult)
		if !ok {
			t.Fatalf("SELECT result after delete is not of expected type []map[string]any, got %T", result)
		}
		if len(resultSlice.Rows) > 0 {
			t.Errorf("DELETE operation failed. Row still exists: %v", resultSlice)
		}
	}
}

func TestDropTable(t *testing.T) {
	defer cleanupDB(t) // Ensure cleanup even if table creation fails for some reason
	_, err := execute("CREATE TABLE temp_table (id int primary key)")
	if err != nil {
		t.Fatalf("Failed to create table for drop table test: %v", err)
	}

	tablePath := filepath.Join("./db", "temp_table"+database.FileExtension)
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		t.Fatalf("Table file was not created at %s before drop", tablePath)
	}

	dropSQL := "DROP TABLE temp_table"
	_, err = execute(dropSQL)
	if err != nil {
		t.Fatalf("Failed to execute DROP TABLE: %v", err)
	}

	if _, err := os.Stat(tablePath); !os.IsNotExist(err) {
		t.Errorf("Table file %s still exists after DROP TABLE operation", tablePath)
	}
}

func TestSelectNonExistentTable(t *testing.T) {
	defer cleanupDB(t)
	selectSQL := "SELECT id FROM non_existent_table"
	_, err := execute(selectSQL)
	if err == nil {
		t.Errorf("Expected error when selecting from non-existent table, but got nil")
	}
	// Add more specific error checking if your interpreter returns typed errors
	// For example: if !strings.Contains(err.Error(), "table not found") { ... }
}

func TestInsertIntoNonExistentTable(t *testing.T) {
	defer cleanupDB(t)
	insertSQL := "INSERT INTO another_non_existent_table (id) VALUES (1)"
	_, err := execute(insertSQL)
	if err == nil {
		t.Errorf("Expected error when inserting into non-existent table, but got nil")
	}
}

func TestDropNonExistentTable(t *testing.T) {
	defer cleanupDB(t)
	dropSQL := "DROP TABLE yet_another_non_existent_table"
	_, err := execute(dropSQL)
	if err == nil {
		t.Errorf("Expected error when dropping a non-existent table, but got nil")
	}
}

func TestDeleteFromNonExistentTable(t *testing.T) {
	defer cleanupDB(t)
	deleteSQL := "DELETE FROM table_does_not_exist WHERE id = 1"
	_, err := execute(deleteSQL)
	if err == nil {
		t.Errorf("Expected error when deleting from a non-existent table, but got nil")
	}
}

func TestComplexQuery(t *testing.T) {
	defer cleanupDB(t)
	_, err := execute("CREATE TABLE employees (eid int primary key, name string(100), department string(50), salary int)")
	if err != nil {
		t.Fatalf("Failed to create employees table: %v", err)
	}

	employeesData := []struct {
		eid        int
		name       string
		department string
		salary     int
	}{
		{1, "Alice", "Engineering", 70000},
		{2, "Bob", "Engineering", 80000},
		{3, "Charlie", "HR", 60000},
		{4, "David", "Marketing", 75000},
		{5, "Eve", "Engineering", 90000},
	}

	for _, emp := range employeesData {
		sql := fmt.Sprintf("INSERT INTO employees (eid, name, department, salary) VALUES (%d, '%s', '%s', %d)", emp.eid, emp.name, emp.department, emp.salary)
		_, err := execute(sql)
		if err != nil {
			t.Fatalf("Failed to insert employee data: %v", err)
		}
	}

	// Test 1: Select engineers with salary > 75000
	selectSQL1 := "SELECT name, salary FROM employees WHERE department = 'Engineering' AND salary > 75000"
	result1, err1 := execute(selectSQL1)
	if err1 != nil {
		t.Fatalf("Failed to execute complex SELECT query 1: %v", err1)
	}

	expected1 := []map[string]any{
		{"name": "Bob", "salary": int64(80000)},
		{"name": "Eve", "salary": int64(90000)},
	}
	assertSelectResult(t, result1, expected1, "Query 1")

	// Test 2: Select all from HR or Marketing
	selectSQL2 := "SELECT name, department FROM employees WHERE department = 'HR' OR department = 'Marketing'"
	result2, err2 := execute(selectSQL2)
	if err2 != nil {
		t.Fatalf("Failed to execute complex SELECT query 2: %v", err2)
	}
	expected2 := []map[string]any{
		{"name": "Charlie", "department": "HR"},
		{"name": "David", "department": "Marketing"},
	}
	assertSelectResult(t, result2, expected2, "Query 2")

	// Test 3: Delete HR department
	_, err = execute("DELETE FROM employees WHERE department = 'HR'")
	if err != nil {
		t.Fatalf("Failed to delete HR department: %v", err)
	}

	// Verify deletion
	selectSQL3 := "SELECT name FROM employees WHERE department = 'HR'"
	result3, err3 := execute(selectSQL3)
	if err3 != nil {
		t.Logf("SELECT after delete HR failed as expected or returned no results: %v", err3)
	} else {
		assertSelectResult(t, result3, []map[string]any{}, "Query 3 (Post HR Deletion)")
	}

}

// assertSelectResult is a helper to reduce boilerplate in SELECT tests
func assertSelectResult(t *testing.T, actualResult any, expectedResult []map[string]any, queryName string) {
	t.Helper() // Marks this function as a test helper
	actualSlice, ok := actualResult.([]map[string]any)
	if !ok {
		t.Fatalf("%s: SELECT result is not of expected type []map[string]any, got %T", queryName, actualResult)
	}

	// Normalize int types for comparison (e.g., int vs int64)
	normalizedActual := make([]map[string]any, len(actualSlice))
	for i, row := range actualSlice {
		normalizedActual[i] = make(map[string]any)
		for k, v := range row {
			if num, isNum := v.(int); isNum {
				normalizedActual[i][k] = int64(num)
			} else if num, isNum := v.(int32); isNum {
				normalizedActual[i][k] = int64(num)
			} else {
				normalizedActual[i][k] = v
			}
		}
	}

	if len(normalizedActual) != len(expectedResult) {
		t.Errorf("%s: Result length mismatch. Expected %d rows, got %d rows. Expected %v, got %v", queryName, len(expectedResult), len(normalizedActual), expectedResult, normalizedActual)
		return
	}

	// For more robust comparison, consider sorting or comparing sets if row order is not guaranteed
	// This simple DeepEqual works if order is guaranteed and types match exactly.
	// Adding a check for individual rows to provide more detailed error messages
	for i := range expectedResult {
		found := false
		for j := range normalizedActual {
			if reflect.DeepEqual(expectedResult[i], normalizedActual[j]) {
				found = true
				// Optionally, remove found items from normalizedActual to handle duplicates correctly if needed,
				// but that makes the logic more complex if order doesn't matter.
				// For now, we assume this simple check is okay if counts match.
				break
			}
		}
		if !found {
			t.Errorf("%s: Expected row %v not found in actual results %v", queryName, expectedResult[i], normalizedActual)
		}
	}

	// Double check the other way
	for i := range normalizedActual {
		found := false
		for j := range expectedResult {
			if reflect.DeepEqual(normalizedActual[i], expectedResult[j]) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s: Actual row %v not found in expected results %v", queryName, normalizedActual[i], expectedResult)
		}
	}
}
