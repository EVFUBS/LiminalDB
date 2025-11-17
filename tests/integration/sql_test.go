package integration

import (
	"LiminalDb/internal/database"
	"LiminalDb/internal/database/engine"
	ops "LiminalDb/internal/database/operations"
	"LiminalDb/internal/interpreter"
	l "LiminalDb/internal/logger"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

var requestChannel chan *engine.Request

func execute(sql string) (ops.Result, error) {
	setupLogging()
	operations, err := interpreter.Evaluate(sql)

	if err != nil {
		return ops.Result{}, err
	}
	requestChannel = make(chan *engine.Request, 100)
	stopChannel := make(chan any)

	dbEngine := engine.NewEngine()
	go dbEngine.StartEngine(requestChannel, stopChannel)

	responseCh := make(chan []ops.Result, 1)

	requestChannel <- &engine.Request{
		Operations: operations,
		ResponseCh: responseCh,
	}

	result := <-responseCh

	return result[len(result)-1], nil
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

func TestUpdateRow(t *testing.T) {
	defer cleanupDB(t)
	_, err := execute("CREATE TABLE items (id int primary key, description string(200))")
	if err != nil {
		t.Fatalf("Failed to create table for update test: %v", err)
	}

	_, err = execute("INSERT INTO items (id, description) VALUES (1, 'Old Description')")
	if err != nil {
		t.Fatalf("Failed to insert row for update test: %v", err)
	}

	updateSQL := "UPDATE items SET description = 'Updated Description' WHERE id = 1"
	_, err = execute(updateSQL)
	if err != nil {
		t.Fatalf("Failed to execute UPDATE: %v", err)
	}

	selectSQL := "SELECT description FROM items WHERE id = 1"
	result, err := execute(selectSQL)
	if err != nil {
		t.Fatalf("Failed to execute SELECT after UPDATE: %v", err)
	}

	expected := &database.QueryResult{
		Columns: []database.Column{
			{
				Name:         "description",
				DataType:     database.TypeString,
				Length:       200,
				IsNullable:   true,
				IsPrimaryKey: false,
			},
		},
		Rows: [][]any{
			{"Updated Description"},
		},
	}

	if result.Err != nil {
		t.Fatalf("SELECT result has error: %v", result.Err)
	}

	if !reflect.DeepEqual(result.Data, expected) {
		t.Errorf("SELECT result mismatch. Expected %v, got %v", expected, result.Data)
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

	if result.Err != nil {
		t.Fatalf("SELECT result is not expected to have error, got %v", result.Err)
	}

	if !reflect.DeepEqual(result.Data, expected) {
		t.Errorf("SELECT result mismatch. Expected %v, got %v", expected, result.Data)
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
		if result.Err != nil {
			t.Fatalf("SELECT result after delete is not of expected type []map[string]any, got %T", result)
		}
		if len(result.Data.Rows) > 0 {
			t.Errorf("DELETE operation failed. Row still exists: %v", result.Data)
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
	result, err := execute(selectSQL)

	if err == nil && result.Err == nil {
		t.Errorf("Expected error when selecting from non-existent table, but got nil")
	}
}

func TestInsertIntoNonExistentTable(t *testing.T) {
	defer cleanupDB(t)
	insertSQL := "INSERT INTO another_non_existent_table (id) VALUES (1)"
	result, err := execute(insertSQL)
	if err == nil && result.Err == nil {
		t.Errorf("Expected error when inserting into non-existent table, but got nil")
	}
}

func TestDropNonExistentTable(t *testing.T) {
	defer cleanupDB(t)
	dropSQL := "DROP TABLE yet_another_non_existent_table"
	result, err := execute(dropSQL)
	if err == nil && result.Err == nil {
		t.Errorf("Expected error when dropping a non-existent table, but got nil")
	}
}

func TestDeleteFromNonExistentTable(t *testing.T) {
	defer cleanupDB(t)
	deleteSQL := "DELETE FROM table_does_not_exist WHERE id = 1"
	result, err := execute(deleteSQL)
	if err == nil && result.Err == nil {
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

	assertSelectResult(t, result1.Data, expected1, "Query 1")

	selectSQL2 := "SELECT name, department FROM employees WHERE department = 'HR' OR department = 'Marketing'"
	result2, err2 := execute(selectSQL2)
	if err2 != nil {
		t.Fatalf("Failed to execute complex SELECT query 2: %v", err2)
	}
	if result2.Err != nil {
		t.Fatalf("SELECT result has error: %v", result2.Err)
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
	assertSelectResult(t, result2.Data, expected2, "Query 2")

	deleteResult, err := execute("DELETE FROM employees WHERE department = 'HR'")
	if err != nil {
		t.Fatalf("Failed to delete HR department: %v", err)
	}
	if deleteResult.Err != nil {
		t.Fatalf("DELETE result has error: %v", deleteResult.Err)
	}

	selectSQL3 := "SELECT name FROM employees WHERE department = 'HR'"
	result3, err3 := execute(selectSQL3)
	if err3 != nil {
		t.Logf("SELECT after delete HR failed as expected or returned no results: %v", err3)
	} else if result3.Err != nil {
		t.Logf("SELECT result after delete HR has error: %v", result3.Err)
	} else {
		assertSelectResult(t, result3.Data, &database.QueryResult{
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

// func TestSproc(t *testing.T) {
// 	defer cleanupDB(t)
// 	_, err := execute("CREATE TABLE users (id int primary key, name string(100), active bool)")
// 	if err != nil {
// 		t.Fatalf("Failed to create table for sproc test: %v", err)
// 	}

// 	_, err = execute("INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)")
// 	if err != nil {
// 		t.Fatalf("Failed to insert row for sproc test: %v", err)
// 	}

// 	_, err = execute("CREATE PROCEDURE get_user_by_id(@id int) AS BEGIN SELECT name, active FROM users WHERE id = @id; END")
// 	if err != nil {
// 		t.Fatalf("Failed to create sproc: %v", err)
// 	}

// 	result, err := execute("EXEC get_user_by_id(1)")
// 	if err != nil {
// 		t.Fatalf("Failed to execute sproc: %v", err)
// 	}

// 	expected := &database.QueryResult{
// 		Columns: []database.Column{
// 			{
// 				Name:         "name",
// 				DataType:     database.TypeString,
// 				Length:       100,
// 				IsNullable:   true,
// 				IsPrimaryKey: false,
// 			},
// 			{
// 				Name:       "active",
// 				DataType:   database.TypeBoolean,
// 				Length:     0,
// 				IsNullable: true,
// 			},
// 		},
// 		Rows: [][]any{
// 			{"Alice", true},
// 		},
// 	}

// 	assertSelectResult(t, result, expected, "Sproc Result")
// }

func TestForeignKey(t *testing.T) {
	defer cleanupDB(t)
	result1, err := execute("CREATE TABLE customers (cid int primary key, name string(100))")
	if err != nil {
		t.Fatalf("Failed to create customers table: %v", err)
	}
	if result1.Err != nil {
		t.Fatalf("CREATE TABLE customers result has error: %v", result1.Err)
	}

	result2, err := execute("CREATE TABLE orders (oid int primary key, customer_id int, FOREIGN KEY (customer_id) REFERENCES customers(cid))")
	if err != nil {
		t.Fatalf("Failed to create orders table with foreign key: %v", err)
	}
	if result2.Err != nil {
		t.Fatalf("CREATE TABLE orders result has error: %v", result2.Err)
	}

	result3, err := execute("INSERT INTO customers (cid, name) VALUES (1, 'John Doe')")
	if err != nil {
		t.Fatalf("Failed to insert customer: %v", err)
	}
	if result3.Err != nil {
		t.Fatalf("INSERT customer result has error: %v", result3.Err)
	}

	result4, err := execute("INSERT INTO orders (oid, customer_id) VALUES (1, 1)")
	if err != nil {
		t.Fatalf("Failed to insert order with valid foreign key: %v", err)
	}
	if result4.Err != nil {
		t.Fatalf("INSERT order result has error: %v", result4.Err)
	}

	result5, err := execute("INSERT INTO orders (oid, customer_id) VALUES (2, 999)")
	if err == nil && result5.Err == nil {
		t.Errorf("Expected error when inserting order with invalid foreign key, got nil")
	}
}

func TestDropForeignKey(t *testing.T) {
	defer cleanupDB(t)
	result1, err := execute("CREATE TABLE customers (cid int primary key, name string(100))")
	if err != nil {
		t.Fatalf("Failed to create customers table: %v", err)
	}
	if result1.Err != nil {
		t.Fatalf("CREATE TABLE customers result has error: %v", result1.Err)
	}

	result2, err := execute("CREATE TABLE orders (oid int primary key, customer_id int, FOREIGN KEY (customer_id) REFERENCES customers(cid))")
	if err != nil {
		t.Fatalf("Failed to create orders table with foreign key: %v", err)
	}
	if result2.Err != nil {
		t.Fatalf("CREATE TABLE orders result has error: %v", result2.Err)
	}

	result3, err := execute("ALTER TABLE orders DROP CONSTRAINT FK_orders_customer_id")
	if err != nil {
		t.Fatalf("Failed to drop foreign key: %v", err)
	}
	if result3.Err != nil {
		t.Fatalf("DROP CONSTRAINT result has error: %v", result3.Err)
	}

	result4, err := execute("INSERT INTO orders (oid, customer_id) VALUES (3, 1)")
	if err != nil {
		t.Errorf("Expected success when inserting order after dropping foreign key, got error: %v", err)
	}
	if result4.Err != nil {
		t.Errorf("Expected success when inserting order after dropping foreign key, got error: %v", result4.Err)
	}
}

func TestAddColumnNoData(t *testing.T) {
	defer cleanupDB(t)
	result1, err := execute("CREATE TABLE products (pid int primary key, pname string(50))")
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}
	if result1.Err != nil {
		t.Fatalf("CREATE TABLE result has error: %v", result1.Err)
	}

	// Add a new column without any data
	alterSQL := "ALTER TABLE products ADD COLUMN price float"
	result2, err := execute(alterSQL)
	if err != nil {
		t.Fatalf("Failed to add column: %v", err)
	}
	if result2.Err != nil {
		t.Fatalf("ALTER TABLE result has error: %v", result2.Err)
	}

	selectSQL := "SELECT pid, pname, price FROM products"
	result, err := execute(selectSQL)
	if err != nil {
		t.Fatalf("Failed to execute SELECT after adding column: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("SELECT result has error: %v", result.Err)
	}

	expected := &database.QueryResult{
		Columns: []database.Column{
			{
				Name:         "pid",
				DataType:     database.TypeInteger64,
				Length:       0,
				IsNullable:   false,
				IsPrimaryKey: true,
			},
			{
				Name:         "pname",
				DataType:     database.TypeString,
				Length:       50,
				IsNullable:   true,
				IsPrimaryKey: false,
			},
			{
				Name:       "price",
				DataType:   database.TypeFloat64,
				Length:     0,
				IsNullable: true,
			},
		},
		Rows: [][]any{},
	}

	assertSelectResult(t, result.Data, expected, "Add Column No Data Result")
}

func TestAddColumnWithData(t *testing.T) {
	defer cleanupDB(t)
	result1, err := execute("CREATE TABLE products (pid int primary key, pname string(50))")
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}
	if result1.Err != nil {
		t.Fatalf("CREATE TABLE result has error: %v", result1.Err)
	}

	result2, err := execute("INSERT INTO products (pid, pname) VALUES (1, 'Product A')")
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}
	if result2.Err != nil {
		t.Fatalf("INSERT result has error: %v", result2.Err)
	}

	alterSQL := "ALTER TABLE products ADD COLUMN price float DEFAULT 9.99"
	result3, err := execute(alterSQL)
	if err != nil {
		t.Fatalf("Failed to add column with default value: %v", err)
	}
	if result3.Err != nil {
		t.Fatalf("ALTER TABLE result has error: %v", result3.Err)
	}

	selectSQL := "SELECT pid, pname, price FROM products"
	result, err := execute(selectSQL)
	if err != nil {
		t.Fatalf("Failed to execute SELECT after adding column: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("SELECT result has error: %v", result.Err)
	}

	expected := &database.QueryResult{
		Columns: []database.Column{
			{
				Name:         "pid",
				DataType:     database.TypeInteger64,
				Length:       0,
				IsNullable:   false,
				IsPrimaryKey: true,
			},
			{
				Name:         "pname",
				DataType:     database.TypeString,
				Length:       50,
				IsNullable:   true,
				IsPrimaryKey: false,
			},
			{
				Name:       "price",
				DataType:   database.TypeFloat64,
				Length:     0,
				IsNullable: true,
			},
		},
		Rows: [][]any{
			{int64(1), "Product A", 9.99},
		},
	}

	assertSelectResult(t, result.Data, expected, "Add Column With Data Result")
}

func TestAddColumnWithExistingData(t *testing.T) {
	defer cleanupDB(t)
	result1, err := execute("CREATE TABLE products (pid int primary key, pname string(50))")
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}
	if result1.Err != nil {
		t.Fatalf("CREATE TABLE result has error: %v", result1.Err)
	}

	result2, err := execute("INSERT INTO products (pid, pname) VALUES (1, 'Product A')")
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}
	if result2.Err != nil {
		t.Fatalf("INSERT result has error: %v", result2.Err)
	}

	alterSQL := "ALTER TABLE products ADD COLUMN price float NULL"
	result3, err := execute(alterSQL)
	if err != nil {
		t.Fatalf("Failed to add column without default value: %v", err)
	}
	if result3.Err != nil {
		t.Fatalf("ALTER TABLE result has error: %v", result3.Err)
	}

	selectSQL := "SELECT pid, pname, price FROM products"
	result, err := execute(selectSQL)
	if err != nil {
		t.Fatalf("Failed to execute SELECT after adding column: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("SELECT result has error: %v", result.Err)
	}

	expected := &database.QueryResult{
		Columns: []database.Column{
			{
				Name:         "pid",
				DataType:     database.TypeInteger64,
				Length:       0,
				IsNullable:   false,
				IsPrimaryKey: true,
			},
			{
				Name:         "pname",
				DataType:     database.TypeString,
				Length:       50,
				IsNullable:   true,
				IsPrimaryKey: false,
			},
			{
				Name:       "price",
				DataType:   database.TypeFloat64,
				Length:     0,
				IsNullable: true,
			},
		},
		Rows: [][]any{
			{int64(1), "Product A", nil},
		},
	}

	assertSelectResult(t, result.Data, expected, "Add Column With Existing Data Result")
}

func TestAddColumnWithExistingDataNoDefault(t *testing.T) {
	defer cleanupDB(t)
	result1, err := execute("CREATE TABLE products (pid int primary key, pname string(50))")
	if err != nil {
		t.Fatalf("Failed to create products table: %v", err)
	}
	if result1.Err != nil {
		t.Fatalf("CREATE TABLE result has error: %v", result1.Err)
	}

	result2, err := execute("INSERT INTO products (pid, pname) VALUES (1, 'Product A')")
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}
	if result2.Err != nil {
		t.Fatalf("INSERT result has error: %v", result2.Err)
	}

	alterSQL := "ALTER TABLE products ADD COLUMN price float NOT NULL"
	result3, err := execute(alterSQL)
	if err == nil && result3.Err == nil {
		t.Errorf("Expected error when adding non-nullable column without default value, got nil")
	}
}

func TestTimestamp(t *testing.T) {
	defer cleanupDB(t)
	result1, err := execute("CREATE TABLE events (id int primary key, event_time datetime)")
	if err != nil {
		t.Fatalf("Failed to create events table: %v", err)
	}
	if result1.Err != nil {
		t.Fatalf("CREATE TABLE result has error: %v", result1.Err)
	}

	insertSQL := "INSERT INTO events (id, event_time) VALUES (1, '2023-10-01 12:00:00')"
	result2, err := execute(insertSQL)
	if err != nil {
		t.Fatalf("Failed to insert row with timestamp: %v", err)
	}
	if result2.Err != nil {
		t.Fatalf("INSERT result has error: %v", result2.Err)
	}

	selectSQL := "SELECT id, event_time FROM events WHERE id = 1"
	result, err := execute(selectSQL)
	if err != nil {
		t.Fatalf("Failed to execute SELECT with timestamp: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("SELECT result has error: %v", result.Err)
	}

	expected := &database.QueryResult{
		Columns: []database.Column{
			{
				Name:         "id",
				DataType:     database.TypeInteger64,
				Length:       0,
				IsNullable:   false,
				IsPrimaryKey: true,
			},
			{
				Name:       "event_time",
				DataType:   database.TypeDatetime,
				Length:     0,
				IsNullable: true,
			},
		},
		Rows: [][]any{
			{int64(1), time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)},
		},
	}

	assertSelectResult(t, result.Data, expected, "Timestamp Result")
}

func setupLogging() {
	logDir := filepath.Join("logs")
	l.New("repl", logDir, l.ERROR)
	l.New("interpreter", logDir, l.ERROR)
	l.New("sql", logDir, l.ERROR)
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
