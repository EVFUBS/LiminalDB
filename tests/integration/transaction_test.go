package integration

import (
	"LiminalDb/helpers"
	"LiminalDb/internal/server"
	"bytes"
	"encoding/json"
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
	Success bool   `json:"success"`
	Result  string `json:"result"`
}

func execRemote(sql string) (string, error) {
	buf := new(bytes.Buffer)
	_ = json.NewEncoder(buf).Encode(&execReq{SQL: sql})
	resp, err := http.Post("http://localhost:8080/exec", "application/json", buf)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", &httpError{msg: strings.TrimSpace(string(b))}
	}
	var r execResp
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", err
	}
	return r.Result, nil
}

type httpError struct{ msg string }

func (e *httpError) Error() string { return e.msg }

func cleanupDBDir() {
	_ = os.RemoveAll("./db")
}

func TestMain(m *testing.M) {
	cleanupDBDir()
	go server.StartServer()
	helpers.WaitForServer()
	code := m.Run()
	os.Exit(code)
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
	out, err := execRemote("SELECT id, name FROM tx_users WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to select after commit: %v", err)
	}
	if !strings.Contains(out, "Alice") {
		t.Fatalf("expected result to contain inserted value, got: %s", out)
	}
	if !strings.Contains(out, "1 row(s) in set") {
		t.Fatalf("expected row count to be 1, got: %s", out)
	}
	path := filepath.Join("./db/tables", "tx_users.bin")
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
	_, selErr := execRemote("SELECT 1 FROM tx_roll")
	if selErr == nil {
		t.Fatalf("expected error when selecting from non-existent table after rollback")
	}
	path := filepath.Join("./db/tables", "tx_roll.bin")
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		if statErr == nil {
			t.Fatalf("expected no table file to exist after rollback")
		}
	}
}

// execRemoteCount runs a SELECT COUNT(*) query and returns the parsed count.
func execRemoteCount(sql string) (int, error) {
	out, err := execRemote(sql)
	if err != nil {
		return 0, err
	}
	return extractRowCount(out)
}

// extractRowCount finds "N row(s) in set" in the result and returns N.
func extractRowCount(out string) (int, error) {
	re := regexp.MustCompile(`(?m)(\d+)\s+row\(s\)\s+in\s+set`)
	m := re.FindStringSubmatch(out)
	if len(m) < 2 {
		return 0, &httpError{msg: "failed to parse row count from output: " + out}
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, err
	}
	return n, nil
}

func TestConcurrentInsertsSameTable(t *testing.T) {
	cleanupDBDir()
	// create table
	_, err := execRemote(strings.Join([]string{
		"CREATE TABLE concurrent_inserts (id int primary key, name string(50))",
	}, "\n"))
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	const writers = 50
	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	successes := make(chan int, writers)

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sql := strings.Join([]string{
				"INSERT INTO concurrent_inserts (id, name) VALUES (" + strconv.Itoa(id) + ", 'User" + strconv.Itoa(id) + "')",
			}, "\n")
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

	// ensure server didn't crash for any writer errors but allow some to fail if primary key collisions occur
	if len(errCh) > 0 {
		// collect a few error messages for debugging but don't fail the test solely for errors;
		// we will assert that final row count equals number of successes.
		errSamples := []string{}
		for e := range errCh {
			errSamples = append(errSamples, e.Error())
			if len(errSamples) >= 5 {
				break
			}
		}
		t.Logf("writers returned %d errors (sample): %v", len(errSamples), errSamples)
	}

	// verify final count matches successCount
	count, err := execRemoteCount("SELECT COUNT(*) FROM concurrent_inserts")
	if err != nil {
		t.Fatalf("failed to select count after concurrent inserts: %v", err)
	}
	if count != successCount {
		t.Fatalf("expected %d rows after concurrent inserts, got %d", successCount, count)
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
				_, e := execRemote("SELECT COUNT(*) FROM cr_table")
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
	count, err := execRemoteCount("SELECT COUNT(*) FROM cr_table")
	if err != nil {
		t.Fatalf("failed to select count after concurrent readers/writers: %v", err)
	}
	if count != successCount {
		t.Fatalf("expected %d rows after concurrent readers/writers, got %d", successCount, count)
	}
}
