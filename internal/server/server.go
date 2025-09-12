package server

import (
	"LiminalDb/internal/interpreter"
	e "LiminalDb/internal/interpreter/eval"
	l "LiminalDb/internal/logger"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
)

var logger *l.Logger
var eval *e.Evaluator

type sqlRequest struct {
	SQL string `json:"sql"`
}

type sqlResponse struct {
	Success bool   `json:"success"`
	Result  string `json:"result"`
}

func StartServer() {
	filepath.Join("logs/server")
	logger = l.New("server", "logs", l.ERROR)
	l.New("interpreter", "logs", l.ERROR)
	l.New("sql", "logs", l.ERROR)
	eval = interpreter.SetupEvaluator()

	mux := http.NewServeMux()

	// Health & readiness
	mux.HandleFunc("/health", health)

	// Transactions
	// POST /tx           -> begin a new transaction
	// GET  /tx           -> list active transactions
	mux.HandleFunc("/tx", txCollectionHandler)

	// Resource-style handlers for tx id and subpaths:
	// GET  /tx/{id}                -> inspect tx
	// POST /tx/{id}/exec           -> exec statement in tx
	// POST /tx/{id}/commit         -> commit tx
	// POST /tx/{id}/rollback       -> rollback tx
	// POST /tx/{id}/kill           -> force abort tx
	mux.HandleFunc("/tx/", txResourceHandler)

	// Execution
	// POST /exec -> execute a single statement in autocommit mode
	mux.HandleFunc("/exec", execHandler)

	// Administrative / diagnostics
	// GET /locks -> show lock table / wait queues
	mux.HandleFunc("/locks", locksHandler)

	// GET/POST/PATCH/DELETE /tables/{name}/rows -> CRUD rows
	mux.HandleFunc("/tables/", tableResourceHandler)

	// Optional metrics endpoint
	mux.HandleFunc("/metrics", metricsHandler)

	server := &http.Server{Addr: ":8080", Handler: mux}
	err := server.ListenAndServe()
	if err != nil {
		panic(err)
	}
}

// health returns 200 OK for liveness checks
func health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// txCollectionHandler handles /tx for creating/listing transactions
func txCollectionHandler(w http.ResponseWriter, r *http.Request) {
	// POST /tx -> begin a new transaction (return txID)
	// GET  /tx -> list active transactions (admin/debug)

	if r.Method == http.MethodGet {
		activeTransactions := eval.TransactionManager.ActiveTransactions
		activeTransactionsBytes, err := json.Marshal(activeTransactions)
		if err != nil {
			logger.Error("Failed to marshal active transactions: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(activeTransactionsBytes)
		return
	}

	if r.Method == http.MethodPost {

	}

	http.Error(w, "Not Implemented", http.StatusNotImplemented)
}

// txResourceHandler handles /tx/{txID} and subpaths like /commit, /rollback, /exec, /kill
func txResourceHandler(w http.ResponseWriter, r *http.Request) {
	// Expect paths of the form:
	//   /tx/{txID}
	//   /tx/{txID}/commit
	//   /tx/{txID}/rollback
	//   /tx/{txID}/exec
	//   /tx/{txID}/kill
	path := strings.TrimPrefix(r.URL.Path, "/tx/")
	_ = path // parse txID and subpath from path, then dispatch
	http.Error(w, "Not Implemented", http.StatusNotImplemented)
}

// execHandler handles autocommit single-statement execution
func execHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		logger.Error("Invalid method used: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req sqlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Error("Failed to decode request body: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	tranSql := wrapSqlInCommitTransaction(req.SQL)
	result, err := eval.Execute(tranSql)
	if err != nil {
		logger.Error("Failed to execute SQL: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	formatted := interpreter.FormatResult(result)
	response := sqlResponse{Success: true, Result: formatted}
	responseBytes, err := json.Marshal(response)
	if err != nil {
		logger.Error("Failed to marshal response: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(responseBytes)
}

// locksHandler returns lock table / wait queues for diagnostics
func locksHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		logger.Error("Invalid method used: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	lockSnapshot := eval.TransactionManager.LockManager.GetLockQueueSnapshot()
	lockSnapshotBytes, err := json.Marshal(lockSnapshot)
	if err != nil {
		logger.Error("Failed to marshal lock snapshot: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(lockSnapshotBytes)
}

// tableResourceHandler handles /tables/{name} and subpaths like /rows
func tableResourceHandler(w http.ResponseWriter, r *http.Request) {
	// Paths:
	//  /tables/{name}
	//  /tables/{name}/rows
	// Dispatch based on method and subpath for read/insert/update/delete
	http.Error(w, "Not Implemented", http.StatusNotImplemented)
}

// metricsHandler is a placeholder for Prometheus or other metrics
func metricsHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not Implemented", http.StatusNotImplemented)
}

// wrapSqlInCommitTransaction wraps a SQL statement in a transaction
func wrapSqlInCommitTransaction(sql string) string {
	if isWrappedInTransaction(sql) {
		return sql
	}
	tranSql := "BEGIN TRAN \n" + sql + "\n COMMIT"
	return tranSql
}

// isWrappedInTransaction checks if SQL is already wrapped in transaction statements
func isWrappedInTransaction(sql string) bool {
	sql = strings.ToUpper(strings.TrimSpace(sql))
	if !strings.HasPrefix(sql, "BEGIN TRAN") {
		return false
	}
	return strings.HasSuffix(sql, "COMMIT") || strings.HasSuffix(sql, "ROLLBACK")
}
