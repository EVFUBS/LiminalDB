package transaction

import (
	"LiminalDb/internal/common"
	ops "LiminalDb/internal/database/operations"
	log "LiminalDb/internal/logger"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Status int

const (
	Active Status = iota
	Committed
	RolledBack
)

const (
	TranTimeout = 60
)

func (s Status) String() string {
	switch s {
	case Active:
		return "active"
	case Committed:
		return "committed"
	case RolledBack:
		return "rolled back"
	default:
		return "unknown"
	}
}

type Transaction struct {
	ID            string
	Status        Status
	Changes       []*Change
	Timestamp     int64
	Locks         map[string]Lock
	ShadowManager *ShadowManager
}

type TransactionManager struct {
	mu                 sync.Mutex
	ActiveTransactions map[string]*Transaction
	LockManager        *LockManager
}

var logger *log.Logger

func NewTransactionManager() *TransactionManager {
	logger = log.Get("sql")

	return &TransactionManager{
		ActiveTransactions: make(map[string]*Transaction),
		LockManager:        NewLockManager(),
	}
}

func (tm *TransactionManager) NewTransaction(operations *[]ops.Operation) *Transaction {
	transactionId := uuid.NewString()
	changes := tm.operationsToChanges(transactionId, operations)
	allLocks := make(map[string]Lock)
	for _, change := range changes {
		for _, lock := range change.Locks {
			allLocks[lock.ResourceID] = lock
		}
	}

	tx := &Transaction{
		ID:            transactionId,
		Status:        Active,
		Changes:       changes,
		Timestamp:     time.Now().Unix(),
		Locks:         allLocks,
		ShadowManager: NewShadowManager(transactionId),
	}

	tm.mu.Lock()
	tm.ActiveTransactions[tx.ID] = tx
	tm.mu.Unlock()

	return tx
}

// Execute executes the transaction by acquiring necessary locks and performing all operations
func (tm *TransactionManager) Execute(tx *Transaction) []ops.Result {
	logger.Info("Executing transaction %s with %d changes", tx.ID, len(tx.Changes))

	var results []ops.Result

	if tx.Status != Active {
		results = append(results, ops.Result{
			Err: fmt.Errorf("transaction is not active"),
		})
		return results
	}

	// Step 1: Acquire ALL unique locks upfront (not per-change)
	logger.Debug("Acquiring all locks for transaction %s", tx.ID)
	acquiredLocks := make(map[string]bool)
	for _, change := range tx.Changes {
		for resourceID, lock := range change.Locks {
			if acquiredLocks[resourceID] {
				continue // Already acquired this lock
			}
			logger.Debug("Requesting lock on resource %s", lock.ResourceID)
			if !tm.LockManager.RequestAndWait(lock.ResourceID, lock, 60*time.Second) {
				logger.Debug("Failed to acquire lock on resource %s", lock.ResourceID)
				results = append(results, ops.Result{
					Err: fmt.Errorf("transaction %s failed to acquire lock on resource %s within timeout",
						tx.ID, lock.ResourceID),
				})
				// Release any locks we did acquire
				tm.releaseLocksForTransaction(tx)
				tx.Status = RolledBack
				return results
			}
			acquiredLocks[resourceID] = true
		}
	}
	logger.Debug("Acquired all locks for transaction %s", tx.ID)

	// Step 2: Create shadow copies for all affected tables
	logger.Debug("Creating shadow copies for transaction %s", tx.ID)
	tablesProcessed := make(map[string]bool)
	for _, change := range tx.Changes {
		tableName := change.Operation.TableName
		if tableName == "" {
			tableName = change.Operation.Metadata.Name
		}
		if tableName != "" && !tablesProcessed[tableName] {
			if err := tx.ShadowManager.CreateShadowForTable(tableName); err != nil {
				logger.Error("Failed to create shadow for table %s: %v", tableName, err)
				results = append(results, ops.Result{Err: fmt.Errorf("failed to create shadow for table %s: %w", tableName, err)})
				tm.releaseLocksForTransaction(tx)

				err := tx.ShadowManager.CleanupShadows()
				if err != nil {
					return nil
				}

				tx.Status = RolledBack
				return results
			}
			tablesProcessed[tableName] = true
		}
	}

	// Step 3: Execute all operations on shadow files
	var rollback bool
	var commit bool
	for _, change := range tx.Changes {
		if change.Rollback {
			rollback = true
			break
		}

		if change.Commit {
			commit = true
			break
		}

		change.Operation.ShadowManager = tx.ShadowManager

		logger.Debug("Executing operation on shadow files")
		changeResult := change.Operation.Execute()

		if changeResult.Err != nil {
			logger.Error("Operation failed: %v", changeResult.Err)
			results = append(results, ops.Result{Err: changeResult.Err})
			rollback = true
			break
		}

		change.Ran = true
		results = append(results, *changeResult)
	}

	// Step 4: Commit or rollback based on results
	if rollback || !commit {
		logger.Info("Rolling back transaction %s", tx.ID)
		tx.Status = RolledBack
		if err := tx.ShadowManager.CleanupShadows(); err != nil {
			logger.Error("Failed to cleanup shadows during rollback: %v", err)
		}
	} else if commit {
		logger.Info("Committing transaction %s", tx.ID)
		if err := tx.ShadowManager.CommitShadows(); err != nil {
			logger.Error("Failed to commit shadows: %v", err)
			results = append(results, ops.Result{Err: fmt.Errorf("failed to commit transaction: %w", err)})
			tx.Status = RolledBack
			tx.ShadowManager.CleanupShadows()
		} else {
			tx.Status = Committed
		}
	}

	// Step 5: Release all locks
	tm.releaseLocksForTransaction(tx)
	logger.Info("Transaction %s completed with status: %s", tx.ID, tx.Status)

	return results
}

// releaseLocksForTransaction releases all locks held by a transaction
func (tm *TransactionManager) releaseLocksForTransaction(tx *Transaction) {
	for _, change := range tx.Changes {
		for _, lock := range change.Locks {
			tm.LockManager.ReleaseLock(lock.ResourceID, lock.TransactionID, lock.Type)
		}
	}
}

// operationsToChanges converts a slice of Operations to a slice of Changes
func (tm *TransactionManager) operationsToChanges(transactionId string, operations *[]ops.Operation) []*Change {
	var changes []*Change
	for i := range *operations {
		op := &(*operations)[i]
		change := &Change{
			Operation: op,
			Locks:     tm.determineNecessaryLocks(transactionId, operations),
		}

		// Set commit/rollback flags based on operation type
		if op.Type == common.Commit {
			change.Commit = true
		} else if op.Type == common.Rollback {
			change.Rollback = true
		}

		changes = append(changes, change)
	}
	return changes
}

// determineNecessaryLocks analyzes operations and returns the necessary locks for the transaction
func (tm *TransactionManager) determineNecessaryLocks(transactionId string, operations *[]ops.Operation) map[string]Lock {
	locks := make(map[string]Lock)
	lockType := DetermineLockType(operations)

	for i := range *operations {
		op := &(*operations)[i]
		resourceID := op.TableName
		if resourceID == "" {
			resourceID = op.Metadata.Name
		}
		if resourceID != "" {
			lock := Lock{
				ResourceID:    resourceID,
				TransactionID: transactionId,
				Type:          lockType,
				Timestamp:     time.Now().Unix(),
			}
			locks[resourceID] = lock
		}
	}
	return locks
}
