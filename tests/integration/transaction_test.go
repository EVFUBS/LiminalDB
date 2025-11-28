package integration

import (
	"LiminalDb/helpers"
	"LiminalDb/internal/database/operations"
	"LiminalDb/internal/database/server"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type execReq struct {
	SQL string `json:"sql"`
}

type execResp struct {
	Success bool              `json:"success"`
	Result  operations.Result `json:"result"`
}

func execRemote(sql string) (operations.Result, error) {
	buf := new(bytes.Buffer)
	_ = json.NewEncoder(buf).Encode(&execReq{SQL: sql})
	resp, err := http.Post("http://localhost:8080/exec", "application/json", buf)
	if err != nil {
		return operations.Result{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return operations.Result{}, &httpError{msg: strings.TrimSpace(string(b))}
	}
	var r execResp
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return operations.Result{}, err
	}
	return r.Result, nil
}

type httpError struct{ msg string }

func (e *httpError) Error() string { return e.msg }

// Helper: Extract row count from result
func getRowCount(result operations.Result) (int, error) {
	if result.Err != nil {
		return 0, result.Err
	}
	if result.Data == nil {
		return 0, fmt.Errorf("result data is nil")
	}
	return len(result.Data.Rows), nil
}

// Helper: Get string value from result at row and column
func getStringValue(result operations.Result, row, col int) (string, error) {
	if result.Err != nil {
		return "", result.Err
	}
	if result.Data == nil || len(result.Data.Rows) == 0 {
		return "", fmt.Errorf("no rows in result")
	}
	if row >= len(result.Data.Rows) || col >= len(result.Data.Rows[row]) {
		return "", fmt.Errorf("row or column index out of bounds")
	}
	val, ok := result.Data.Rows[row][col].(string)
	if !ok {
		return "", fmt.Errorf("value at [%d][%d] is not a string", row, col)
	}
	return val, nil
}

// Helper: Get int value from result at row and column
func getIntValue(result operations.Result, row, col int) (int64, error) {
	if result.Err != nil {
		return 0, result.Err
	}
	if result.Data == nil || len(result.Data.Rows) == 0 {
		return 0, fmt.Errorf("no rows in result")
	}
	if row >= len(result.Data.Rows) || col >= len(result.Data.Rows[row]) {
		return 0, fmt.Errorf("row or column index out of bounds")
	}
	val, ok := result.Data.Rows[row][col].(int64)
	if !ok {
		return 0, fmt.Errorf("value at [%d][%d] is not an int64", row, col)
	}
	return val, nil
}

// Helper: Check if string value exists in result column
func hasStringInColumn(result operations.Result, colIndex int, searchStr string) bool {
	if result.Data == nil {
		return false
	}
	for _, row := range result.Data.Rows {
		if colIndex < len(row) {
			if val, ok := row[colIndex].(string); ok && strings.Contains(val, searchStr) {
				return true
			}
		}
	}
	return false
}

func cleanupDBDir() {
	_ = os.RemoveAll("./db")
}

func TestMain(m *testing.M) {
	//defer cleanupDBDir()
	go server.StartServer()
	helpers.WaitForServer()
	code := m.Run()
	os.Exit(code)
}

func TestSingleTransaction(t *testing.T) {
	sql := strings.Join([]string{
		"BEGIN TRAN",
		"CREATE TABLE single_tx (id int primary key, value string(50))",
		"INSERT INTO single_tx (id, value) VALUES (1, 'test')",
		"COMMIT",
	}, "\n")
	_, err := execRemote(sql)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}

	result, err := execRemote("SELECT * FROM single_tx")
	if err != nil {
		t.Fatalf("failed to query after transaction: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("SELECT result has error: %v", result.Err)
	}

	rowValue, err := getStringValue(result, 0, 1)
	if err != nil {
		t.Fatalf("failed to get string value: %v", err)
	}
	if !strings.Contains(rowValue, "test") {
		t.Fatalf("expected result to contain 'test', got: %s", rowValue)
	}

	path := filepath.Join("./db/tables/single_tx/", "single_tx.bin")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected table file to exist: %v", err)
	}
}

func TestTransactionCommit(t *testing.T) {
	cleanupDBDir()
	sql := strings.Join([]string{
		"BEGIN TRAN",
		"CREATE TABLE tx_users (id int primary key, name string(50))",
		"INSERT INTO tx_users (id, name) VALUES (1, 'Alice')",
		"COMMIT",
	}, "\n")
	_, err := execRemote(sql)
	if err != nil {
		t.Fatalf("failed to run transaction: %v", err)
	}
	result, err := execRemote("SELECT id, name FROM tx_users WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to select after commit: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("SELECT result has error: %v", result.Err)
	}

	name, err := getStringValue(result, 0, 1)
	if err != nil {
		t.Fatalf("failed to get name value: %v", err)
	}
	if !strings.Contains(name, "Alice") {
		t.Fatalf("expected result to contain 'Alice', got: %s", name)
	}

	rowCount, err := getRowCount(result)
	if err != nil {
		t.Fatalf("failed to get row count: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected 1 row, got %d", rowCount)
	}

	path := filepath.Join("./db/tables/tx_users", "tx_users.bin")
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("expected table file to exist: %v", statErr)
	}
}

func TestTransactionRollback(t *testing.T) {
	cleanupDBDir()
	sql := strings.Join([]string{
		"BEGIN TRAN",
		"CREATE TABLE tx_roll (id int primary key, name string(50))",
		"INSERT INTO tx_roll (id, name) VALUES (1, 'Alice')",
		"ROLLBACK",
	}, "\n")
	_, err := execRemote(sql)
	if err != nil {
		t.Fatalf("failed to run rollback transaction: %v", err)
	}
	result, selErr := execRemote("SELECT 1 FROM tx_roll")
	if selErr == nil && result.Err == nil {
		t.Fatalf("expected error when selecting from non-existent table after rollback")
	}
	path := filepath.Join("./db/tables", "tx_roll.bin")
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		if statErr == nil {
			t.Fatalf("expected no table file to exist after rollback")
		}
	}
}

func TestConcurrentInsertsSameTable(t *testing.T) {
	cleanupDBDir()

	var wg sync.WaitGroup
	_, err := execRemote(strings.Join([]string{
		"CREATE TABLE concurrent_inserts (id int primary key, name string(50))",
	}, "\n"))
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	const writers = 100
	errCh := make(chan error, writers)
	successes := make(chan int, writers)

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sql := "INSERT INTO concurrent_inserts (id, name) VALUES (" + strconv.Itoa(id) + ", 'User" + strconv.Itoa(id) + "')"
			_, e := execRemote(sql)
			if e != nil {
				errCh <- e
				return
			}
			successes <- 1
		}(i + 1)
	}

	wg.Wait()
	close(errCh)
	close(successes)

	// count successful inserts
	successCount := 0
	for range successes {
		successCount++
	}

	if len(errCh) > 0 {
		errSamples := []string{}
		for e := range errCh {
			errSamples = append(errSamples, e.Error())
			if len(errSamples) >= 5 {
				break
			}
		}
		t.Logf("writers returned %d errors (sample): %v", len(errCh), errSamples)
	}

	time.Sleep(15 * time.Second) // brief wait to ensure all inserts are finalized

	result, err := execRemote("SELECT * FROM concurrent_inserts")
	if err != nil {
		t.Fatalf("failed to select count after concurrent inserts: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("SELECT result has error: %v", result.Err)
	}

	resultCount, err := getRowCount(result)
	if err != nil {
		t.Fatalf("failed to get row count: %v", err)
	}

	if resultCount != writers {
		t.Fatalf("expected %d successful inserts, got %d", writers, resultCount)
	}
}

func TestConcurrentInsertsDifferentTables(t *testing.T) {
	cleanupDBDir()

	const numTables = 10
	const insertsPerTable = 10
	var wg sync.WaitGroup

	// Create 10 different tables
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("table_%d", i)
		sql := fmt.Sprintf("CREATE TABLE %s (id int primary key, value string(50))", tableName)
		_, err := execRemote(sql)
		if err != nil {
			t.Fatalf("failed to create table %s: %v", tableName, err)
		}
	}

	errCh := make(chan error, numTables*insertsPerTable)
	successes := make(chan int, numTables*insertsPerTable)

	// Insert 10 rows into each table concurrently (100 total inserts)
	for tableIdx := 0; tableIdx < numTables; tableIdx++ {
		for rowIdx := 0; rowIdx < insertsPerTable; rowIdx++ {
			wg.Add(1)
			go func(tIdx, rIdx int) {
				defer wg.Done()
				tableName := fmt.Sprintf("table_%d", tIdx)
				sql := fmt.Sprintf("INSERT INTO %s (id, value) VALUES (%d, 'value_%d_%d')",
					tableName, rIdx, tIdx, rIdx)
				result, _ := execRemote(sql)
				if result.Err != nil {
					errCh <- result.Err
					return
				}
				successes <- 1
			}(tableIdx, rowIdx)
		}
	}

	wg.Wait()
	close(errCh)
	close(successes)

	successCount := 0
	for range successes {
		successCount++
	}

	if len(errCh) > 0 {
		errSamples := []string{}
		for e := range errCh {
			errSamples = append(errSamples, e.Error())
			if len(errSamples) >= 5 {
				break
			}
		}
		t.Fatalf("inserts returned %d errors (sample): %v", len(errCh), errSamples)
	}

	// Verify each table has the correct number of rows
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("table_%d", i)
		result, err := execRemote(fmt.Sprintf("SELECT * FROM %s", tableName))
		if err != nil {
			t.Fatalf("failed to select from %s: %v", tableName, err)
		}
		if result.Err != nil {
			t.Fatalf("SELECT result has error for %s: %v", tableName, result.Err)
		}

		rowCount, err := getRowCount(result)
		if err != nil {
			t.Fatalf("failed to get row count for %s: %v", tableName, err)
		}

		if rowCount != insertsPerTable {
			t.Fatalf("expected %d rows in %s, got %d", insertsPerTable, tableName, rowCount)
		}
	}
}

func TestConcurrentReadersDuringWrites(t *testing.T) {
	cleanupDBDir()
	_, err := execRemote(strings.Join([]string{
		"CREATE TABLE cr_table (id int primary key, val int)",
	}, "\n"))
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	const writers = 30
	const readers = 10
	var wg sync.WaitGroup
	writerErrors := make(chan error, writers)
	readerErrors := make(chan error, readers)
	successes := make(chan int, writers)

	// writers
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sql := "INSERT INTO cr_table (id, val) VALUES (" + strconv.Itoa(id+1) + ", 1)"
			_, e := execRemote(sql)
			if e != nil {
				writerErrors <- e
				return
			}
			successes <- 1
		}(i)
		// small stagger to increase interleaving
		time.Sleep(5 * time.Millisecond)
	}

	// readers concurrently polling counts
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// poll a few times while writers are running
			for j := 0; j < 20; j++ {
				_, e := execRemote("SELECT * FROM cr_table")
				if e != nil {
					readerErrors <- e
					return
				}
				time.Sleep(3 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	close(writerErrors)
	close(readerErrors)
	close(successes)

	successCount := 0
	for range successes {
		successCount++
	}

	if len(writerErrors) > 0 {
		errSamples := []string{}
		for e := range writerErrors {
			errSamples = append(errSamples, e.Error())
			if len(errSamples) >= 5 {
				break
			}
		}
		t.Fatalf("writers returned errors (sample): %v", errSamples)
	}

	if len(readerErrors) > 0 {
		errSamples := []string{}
		for e := range readerErrors {
			errSamples = append(errSamples, e.Error())
			if len(errSamples) >= 5 {
				break
			}
		}
		t.Fatalf("readers returned errors (sample): %v", errSamples)
	}

	// final verification
	result, err := execRemote("SELECT * FROM cr_table")
	if err != nil {
		t.Fatalf("failed to select count after concurrent readers/writers: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("SELECT result has error: %v", result.Err)
	}

	resultCount, err := getRowCount(result)
	if err != nil {
		t.Fatalf("failed to get row count: %v", err)
	}

	if resultCount != writers {
		t.Fatalf("expected %d successful inserts, got %d", writers, resultCount)
	}
}

func extractCount(result string) (int, error) {
	re := regexp.MustCompile(`\d+`)
	matches := re.FindAllString(result, -1)
	if len(matches) == 0 {
		return 0, fmt.Errorf("failed to extract count from result: %s", result)
	}
	last := matches[len(matches)-1]
	count, err := strconv.Atoi(last)
	if err != nil {
		return 0, fmt.Errorf("failed to parse count %q from result: %w", last, err)
	}
	return count, nil
}

func TestTransactionMixedOperations(t *testing.T) {
	cleanupDBDir()

	sql := strings.Join([]string{
		"BEGIN TRAN",
		"CREATE TABLE mix (id int primary key, name string(50), cnt int)",
		"INSERT INTO mix (id, name, cnt) VALUES (1, 'Alice', 10)",
		"INSERT INTO mix (id, name, cnt) VALUES (2, 'Bob', 20)",
		"UPDATE mix SET cnt = cnt + 5 WHERE id = 1",
		"DELETE FROM mix WHERE id = 2",
		"ALTER TABLE mix ADD COLUMN extra string(10) NULL",
		"COMMIT",
	}, "\n")
	if _, err := execRemote(sql); err != nil {
		t.Fatalf("failed to execute mixed transaction: %v", err)
	}

	result, err := execRemote("SELECT id, name, cnt, extra FROM mix WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to select from committed table: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("SELECT result has error: %v", result.Err)
	}

	name, err := getStringValue(result, 0, 1)
	if err != nil {
		t.Fatalf("failed to get name value: %v", err)
	}
	if !strings.Contains(name, "Alice") {
		t.Fatalf("expected name Alice in result, got: %s", name)
	}

	cnt, err := getIntValue(result, 0, 2)
	if err != nil {
		t.Fatalf("failed to get cnt value: %v", err)
	}
	if cnt != 15 {
		t.Fatalf("expected updated cnt 15 in result, got: %d", cnt)
	}

	rowCount, err := getRowCount(result)
	if err != nil {
		t.Fatalf("failed to get row count: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected 1 row for id=1, got %d", rowCount)
	}

	// ensure table file exists on disk
	path := filepath.Join("./db/tables", "mix.bin")
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("expected table file to exist after commit: %v", statErr)
	}

	// Rollback scenario: create a table and insert, then rollback; table should not exist
	sqlRB := strings.Join([]string{
		"BEGIN TRAN",
		"CREATE TABLE mix_rb (id int primary key, val int)",
		"INSERT INTO mix_rb (id, val) VALUES (1, 100)",
		"ROLLBACK",
	}, "\n")
	if _, err := execRemote(sqlRB); err != nil {
		t.Fatalf("failed to execute rollback transaction: %v", err)
	}
	// selecting from rolled-back table should produce an error
	rbResult, selErr := execRemote("SELECT 1 FROM mix_rb")
	if selErr == nil && rbResult.Err == nil {
		t.Fatalf("expected selecting from rolled-back table to return an error")
	}
}
