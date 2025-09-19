package transaction

import (
	"LiminalDb/internal/ast"
	db "LiminalDb/internal/database"
	ops "LiminalDb/internal/database/operations"
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
	Changes   []Change
	Locks     map[string]Lock
	Timestamp int64
}

type TransactionManager struct {
	mu                 sync.Mutex
	ActiveTransactions map[string]*Transaction
	LockManager        *LockManager
}

func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		ActiveTransactions: make(map[string]*Transaction),
		LockManager:        NewLockManager(),
	}
}

func (tm *TransactionManager) Begin() *Transaction {
	tx := &Transaction{
		ID:        uuid.NewString(),
		Status:    Active,
		Changes:   []Change{},
		Locks:     map[string]Lock{},
		Timestamp: time.Now().Unix(),
	}
	tm.mu.Lock()
	tm.ActiveTransactions[tx.ID] = tx
	tm.mu.Unlock()

	return tx
}

func (tm *TransactionManager) AddChange(tx *Transaction, change Change) error {
	tm.mu.Lock()
	tx.Changes = append(tx.Changes, change)
	tm.ActiveTransactions[tx.ID] = tx
	// TODO: Need a big figure out what needs to lock when certain stuff is done this is far to basic
	if change.Operation.TableName != "" || change.Operation.Metadata.Name != "" {
		lock := Lock{
			ResourceID:    change.Operation.TableName, // TODO: Find a way to get the rows it needs as well
			TransactionID: tx.ID,
			Type:          Exclusive, // TODO: Need a way to determine if its a read or write lock
			Timestamp:     time.Now().Unix(),
		}
		tx.Locks[change.Operation.TableName] = lock
	}
	tm.mu.Unlock()

	return nil
}

func (tm *TransactionManager) Execute(tx *Transaction, execFunc func(ast.Statement) (any, error)) ([]any, error) {
	if tx.Status != Active {
		return nil, fmt.Errorf("committed transaction is not active, transaction: %s", tx.ID)
	}

	for _, lock := range tx.Locks {
		tm.LockManager.requestLock(lock.ResourceID, lock, time.Now().Unix())
	}

	// TODO: This is very basic, need to implement wait timeouts and deadlock detection
	for _, lock := range tx.Locks {
		for {
			hasLock := tm.LockManager.checkLock(lock.ResourceID, lock)
			if hasLock {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	var results []any
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
		changeResult, err := execFunc(change.Statement)
		if err != nil {
			return nil, err
		}
		results = append(results, changeResult)
	}

	if rollback || !commit {
		tm.Rollback(tx)
		tx.Status = RolledBack
	}

	if commit {
		tm.Commit(tx)
		tx.Status = Committed
	}

	return results, nil
}

// Commit - deletes the transaction from the active transactions map
func (tm *TransactionManager) Commit(tx *Transaction) error {
	tm.releaseLocksForTransaction(tx)
	tm.mu.Lock()
	delete(tm.ActiveTransactions, tx.ID)
	tm.mu.Unlock()
	return nil
}

// Rollback - rolls back the transaction by deleting it from the active transactions map and rolling back all changes
func (tm *TransactionManager) Rollback(tx *Transaction) error {
	op := ops.NewOperationsImpl()
	for i := len(tx.Changes) - 1; i >= 0; i-- {
		ch := tx.Changes[i]
		if ch.Commit || ch.Rollback {
			continue
		}
		switch s := ch.Statement.(type) {
		case *ast.CreateTableStatement:
			operation := &ops.Operation{TableName: s.TableName}
			_ = op.DropTable(operation)
		case *ast.InsertStatement:
			rows := ch.Operation.Data.Insert
			if len(rows) == 0 {
				continue
			}
			filter := func(row []any, cols []db.Column) (bool, error) {
				for _, ins := range rows {
					if len(ins) != len(row) {
						continue
					}
					equal := true
					for i := range ins {
						if row[i] != ins[i] {
							equal = false
							break
						}
					}
					if equal {
						return true, nil
					}
				}
				return false, nil
			}
			_ = op.DeleteRows(&ops.Operation{TableName: s.TableName, Filter: filter})
		}
	}
	tm.releaseLocksForTransaction(tx)
	tm.mu.Lock()
	delete(tm.ActiveTransactions, tx.ID)
	tm.mu.Unlock()
	return nil
}

// RollbackByID provides a safe way to rollback a transaction by its id.
func (tm *TransactionManager) RollbackByID(txID string) error {
	tm.mu.Lock()
	tx, ok := tm.ActiveTransactions[txID]
	tm.mu.Unlock()

	if !ok {
		return nil
	}

	return tm.Rollback(tx)
}

// ExpireTransactionAfterTimeout starts a background worker that periodically scans
// ActiveTransactions for transactions whose last activity timestamp is older than
// TranTimeout seconds. Expired transactions are rolled back via RollbackByID.
func (tm *TransactionManager) ExpireTransactionAfterTimeout() {
	go func() {
		ticker := time.NewTicker(time.Second * TranTimeout)
		defer ticker.Stop()

		for range ticker.C {
			now := time.Now().Unix()

			tm.mu.Lock()
			var expired []string
			for id, tx := range tm.ActiveTransactions {
				if now-tx.Timestamp > int64(TranTimeout) {
					expired = append(expired, id)
				}
			}
			tm.mu.Unlock()

			for _, id := range expired {
				_ = tm.RollbackByID(id)
			}
		}
	}()
}

// releaseLocksForTransaction releases all locks for a transaction
func (tm *TransactionManager) releaseLocksForTransaction(tx *Transaction) {
	for _, lock := range tx.Locks {
		tm.LockManager.releaseLock(lock.ResourceID, lock)
	}
}
