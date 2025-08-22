package transaction

import (
	"LiminalDb/internal/database/operations"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Status int

const (
	Active Status = iota
	Committed
)

func (s Status) String() string {
	switch s {
	case Active:
		return "active"
	case Committed:
		return "committed"
	default:
		return "unknown"
	}
}

type Transaction struct {
	ID      string
	Status  Status
	Changes []Change
	Locks   map[string]Lock
}

type TransactionManager struct {
	ActiveTransactions map[string]*Transaction
	LockManager        *LockManager
}

func (tm *TransactionManager) Begin() (*Transaction, error) {
	tx := &Transaction{
		ID:      uuid.NewString(),
		Status:  Active,
		Changes: []Change{},
		Locks:   map[string]Lock{},
	}
	tm.ActiveTransactions[tx.ID] = tx
	return tx, nil
}

func (tm *TransactionManager) AddChange(tx *Transaction, change Change) error {
	tx.Changes = append(tx.Changes, change)
	tm.ActiveTransactions[tx.ID] = tx

	if change.Operation.TableName != "" {
		lock := Lock{
			ResourceID:    change.Operation.TableName, // TODO: Find a way to get the rows it needs as well
			TransactionID: tx.ID,
			Type:          Exclusive, // TODO: Need a way to determine if its a read or write lock
			Timestamp:     time.Now().Unix(),
		}
		tx.Locks[change.Operation.TableName] = lock
	}

	return nil
}

func (tm *TransactionManager) Commit(tx *Transaction) ([]operations.Result, error) {
	// if not active return error
	if tx.Status != Active {
		return nil, fmt.Errorf("commited transaction is not active, transaction: %s", tx.ID)
	}

	for _, lock := range tx.Locks {
		tm.LockManager.requestLock(lock.ResourceID, lock, time.Now().Unix())
	}

	var results []operations.Result
	for _, change := range tx.Changes {
		hasLock := tm.LockManager.checkLock(change.Operation.TableName, tx.Locks[change.Operation.TableName])

		if hasLock {
			changeResult := change.execute(change.Operation)

			if changeResult.Err != nil {
				return nil, changeResult.Err
			}

			results = append(results, changeResult)
		}

		tm.LockManager.releaseLock(change.Operation.TableName, tx.Locks[change.Operation.TableName])
	}

	tx.Status = Committed
	delete(tm.ActiveTransactions, tx.ID)

	return results, nil
}

func (tm *TransactionManager) Rollback(tx *Transaction) error {
	delete(tm.ActiveTransactions, tx.ID)
	return nil
}
