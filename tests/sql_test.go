package tests

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

	tablePath := filepath.Join("./db/tables", "users"+database.FileExtension)
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
	defer cleanupDB(t)
	_, err := execute("CREATE TABLE temp_table (id int primary key)")
	if err != nil {
		t.Fatalf("Failed to create table for drop table test: %v", err)
	}

	tablePath := filepath.Join("./db/tables", "temp_table"+database.FileExtension)
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

	expected1 := &database.QueryResult{
		Columns: []database.Column{
			{
				Name:         "name",
				DataType:     database.TypeString,
				Length:       100,
				IsNullable:   true,
				IsPrimaryKey: false,
			},
			{
				Name:         "salary",
				DataType:     database.TypeInteger64,
				Length:       0,
				IsNullable:   true,
				IsPrimaryKey: false,
			},
		},
		Rows: [][]any{
			{"Bob", int64(80000)},
			{"Eve", int64(90000)},
		},
	}

	assertSelectResult(t, result1, expected1, "Query 1")

	selectSQL2 := "SELECT name, department FROM employees WHERE department = 'HR' OR department = 'Marketing'"
	result2, err2 := execute(selectSQL2)
	if err2 != nil {
		t.Fatalf("Failed to execute complex SELECT query 2: %v", err2)
	}
	expected2 := &database.QueryResult{
		Columns: []database.Column{
			{
				Name:         "name",
				DataType:     database.TypeString,
				Length:       100,
				IsNullable:   true,
				IsPrimaryKey: false,
			},
			{
				Name:       "department",
				DataType:   database.TypeString,
				Length:     50,
				IsNullable: true,
			},
		},
		Rows: [][]any{
			{"Charlie", "HR"},
			{"David", "Marketing"},
		},
	}
	assertSelectResult(t, result2, expected2, "Query 2")

	_, err = execute("DELETE FROM employees WHERE department = 'HR'")
	if err != nil {
		t.Fatalf("Failed to delete HR department: %v", err)
	}

	selectSQL3 := "SELECT name FROM employees WHERE department = 'HR'"
	result3, err3 := execute(selectSQL3)
	if err3 != nil {
		t.Logf("SELECT after delete HR failed as expected or returned no results: %v", err3)
	} else {
		assertSelectResult(t, result3, &database.QueryResult{
			Columns: []database.Column{
				{
					Name:         "name",
					DataType:     database.TypeString,
					Length:       100,
					IsNullable:   true,
					IsPrimaryKey: false,
				},
			},
		}, "Query 3 (Post HR Deletion)")
	}
}

func TestSproc(t *testing.T) {
	defer cleanupDB(t)
	_, err := execute("CREATE TABLE users (id int primary key, name string(100), active bool)")
	if err != nil {
		t.Fatalf("Failed to create table for sproc test: %v", err)
	}

	_, err = execute("INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)")
	if err != nil {
		t.Fatalf("Failed to insert row for sproc test: %v", err)
	}

	_, err = execute("CREATE PROCEDURE get_user_by_id(@id int) AS BEGIN SELECT name, active FROM users WHERE id = @id; END")
	if err != nil {
		t.Fatalf("Failed to create sproc: %v", err)
	}

	result, err := execute("EXEC get_user_by_id(1)")
	if err != nil {
		t.Fatalf("Failed to execute sproc: %v", err)
	}

	expected := &database.QueryResult{
		Columns: []database.Column{
			{
				Name:         "name",
				DataType:     database.TypeString,
				Length:       100,
				IsNullable:   true,
				IsPrimaryKey: false,
			},
			{
				Name:       "active",
				DataType:   database.TypeBoolean,
				Length:     0,
				IsNullable: true,
			},
		},
		Rows: [][]any{
			{"Alice", true},
		},
	}

	assertSelectResult(t, result, expected, "Sproc Result")
}

func TestForeignKey(t *testing.T) {
	defer cleanupDB(t)
	_, err := execute("CREATE TABLE customers (cid int primary key, name string(100))")
	if err != nil {
		t.Fatalf("Failed to create customers table: %v", err)
	}

	_, err = execute("CREATE TABLE orders (oid int primary key, customer_id int, FOREIGN KEY (customer_id) REFERENCES customers(cid))")
	if err != nil {
		t.Fatalf("Failed to create orders table with foreign key: %v", err)
	}

	_, err = execute("INSERT INTO customers (cid, name) VALUES (1, 'John Doe')")
	if err != nil {
		t.Fatalf("Failed to insert customer: %v", err)
	}

	_, err = execute("INSERT INTO orders (oid, customer_id) VALUES (1, 1)")
	if err != nil {
		t.Fatalf("Failed to insert order with valid foreign key: %v", err)
	}

	_, err = execute("INSERT INTO orders (oid, customer_id) VALUES (2, 999)")
	if err == nil {
		t.Errorf("Expected error when inserting order with invalid foreign key, got nil")
	}
}

func TestDropForeignKey(t *testing.T) {
	defer cleanupDB(t)
	_, err := execute("CREATE TABLE customers (cid int primary key, name string(100))")
	if err != nil {
		t.Fatalf("Failed to create customers table: %v", err)
	}

	_, err = execute("CREATE TABLE orders (oid int primary key, customer_id int, FOREIGN KEY (customer_id) REFERENCES customers(cid))")
	if err != nil {
		t.Fatalf("Failed to create orders table with foreign key: %v", err)
	}

	_, err = execute("ALTER TABLE orders DROP CONSTRAINT FK_orders_customer_id")
	if err != nil {
		t.Fatalf("Failed to drop foreign key: %v", err)
	}

	_, err = execute("INSERT INTO orders (oid, customer_id) VALUES (3, 1)")
	if err != nil {
		t.Errorf("Expected success when inserting order after dropping foreign key, got error: %v", err)
	}
}

func assertSelectResult(t *testing.T, actualResult any, expected *database.QueryResult, queryName string) {
	t.Helper()
	actual, ok := actualResult.(*database.QueryResult)
	if !ok {
		t.Fatalf("%s: Result is not of type *database.QueryResult, got %T", queryName, actualResult)
	}

	if !reflect.DeepEqual(actual.Columns, expected.Columns) {
		t.Errorf("%s: Column definitions mismatch.\nExpected: %+v\nGot: %+v",
			queryName, expected.Columns, actual.Columns)
		return
	}

	if len(actual.Rows) != len(expected.Rows) {
		t.Errorf("%s: Row count mismatch. Expected %d rows, got %d rows",
			queryName, len(expected.Rows), len(actual.Rows))
		return
	}

	for i := range expected.Rows {
		if !reflect.DeepEqual(actual.Rows[i], expected.Rows[i]) {
			t.Errorf("%s: Row %d mismatch.\nExpected: %v\nGot: %v",
				queryName, i, expected.Rows[i], actual.Rows[i])
		}
	}
}
