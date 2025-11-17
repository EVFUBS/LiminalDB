package transaction

import (
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
	ID        string
	Status    Status
	Changes   []*Change
	Timestamp int64
	Locks     map[string]Lock
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
		ID:        transactionId,
		Status:    Active,
		Changes:   changes,
		Timestamp: time.Now().Unix(),
		Locks:     allLocks,
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

	var rollback bool
	var commit bool
	for _, change := range tx.Changes {

		for _, lock := range change.Locks {
			logger.Debug("Requesting lock on resource %s", lock.ResourceID)
			if !tm.LockManager.RequestAndWait(lock.ResourceID, lock, 60*time.Second) {
				logger.Debug("Failed to acquire lock on resource %s", lock.ResourceID)
				results = append(results, ops.Result{
					Err: fmt.Errorf("transaction %s failed to acquire lock on resource %s within timeout",
						tx.ID, lock.ResourceID),
				})
				tm.releaseLocksForChange(change)
				tx.Status = RolledBack
				return results
			}
		}

		logger.Debug("Acquired locks for %d resources", len(change.Locks))

		if change.Rollback {
			rollback = true
			break
		}

		if change.Commit {
			commit = true
			break
		}

		changeResult := change.Operation.Execute()

		if changeResult.Err != nil {
			results = append(results, ops.Result{Err: changeResult.Err})
			tx.Status = RolledBack
			return results
		}

		results = append(results, *changeResult)
		tm.releaseLocksForChange(change)
	}

	if rollback || !commit {
		tx.Status = RolledBack
	} else if commit {
		tx.Status = Committed
	}

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

// releaseLocksForTransaction releases all locks held by a change
func (tm *TransactionManager) releaseLocksForChange(change *Change) {
	for _, lock := range change.Locks {
		tm.LockManager.ReleaseLock(lock.ResourceID, lock.TransactionID, lock.Type)
	}
}

// operationsToChanges converts a slice of Operations to a slice of Changes
func (tm *TransactionManager) operationsToChanges(transactionId string, operations *[]ops.Operation) []*Change {
	var changes []*Change
	for i := range *operations {
		op := &(*operations)[i]
		changes = append(changes, &Change{
			Operation: op,
			Locks:     tm.determineNecessaryLocks(transactionId, operations),
		})
	}
	return changes
}

// determineNecessaryLocks analyzes operations and returns the necessary locks for the transaction
func (tm *TransactionManager) determineNecessaryLocks(transactionId string, operations *[]ops.Operation) map[string]Lock {
	locks := make(map[string]Lock)
	lockType := DetermineLockType(operations)

	for i := range *operations {
		op := &(*operations)[i]
		if op.TableName != "" || op.Metadata.Name != "" {
			lock := Lock{
				ResourceID:    op.TableName,
				TransactionID: transactionId,
				Type:          lockType,
				Timestamp:     time.Now().Unix(),
			}
			locks[op.TableName] = lock
		}
	}
	return locks
}
