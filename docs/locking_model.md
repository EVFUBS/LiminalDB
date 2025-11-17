# LiminalDB Transaction Locking Model

## Overview
LiminalDB uses a centralized lock manager to coordinate concurrent access to database resources (tables, rows) during transactions. This ensures ACID properties and prevents race conditions and deadlocks.

## Lock Types
- **Shared Lock**: Allows multiple transactions to read a resource concurrently. No transaction may write while shared locks are held.
- **Exclusive Lock**: Allows a single transaction to write to a resource. No other transaction may read or write while an exclusive lock is held.

## Lock Acquisition
- All lock requests go through `LockManager.RequestAndWait(resourceID, lock, timeout)`.
- Locks are requested before executing any transaction changes.
- If a lock cannot be acquired within the timeout, the transaction is rolled back and all acquired locks are released.

## Lock Release
- All locks are released via `LockManager.ReleaseLock(resourceID, transactionID, lockType)`.
- Locks are released after transaction completion (commit or rollback).
- The helper `TransactionManager.releaseLocksForTransaction(tx)` releases all locks held by a transaction.

## Transaction Flow
1. **Begin Transaction**: TransactionManager creates a new transaction and determines required locks.
2. **Acquire Locks**: For each resource, RequestAndWait is called. If any lock fails, the transaction is rolled back.
3. **Execute Operations**: Transaction changes are executed.
4. **Complete Transaction**: On commit or rollback, all locks are released.

## Deadlock Avoidance
- The lock manager uses a queue per resource. Locks are granted in order, and only when safe (see canGrantLock logic).
- Shared locks are granted if no exclusive lock is held or pending.
- Exclusive locks are granted only if no other transaction holds a lock on the resource.

## API Summary
- `LockManager.RequestAndWait(resourceID, lock, timeout)` — Request a lock and wait for it to be granted.
- `LockManager.ReleaseLock(resourceID, transactionID, lockType)` — Release a lock.
- `TransactionManager.releaseLocksForTransaction(tx)` — Release all locks for a transaction.

## Best Practices
- Always acquire all required locks before executing transaction changes.
- Always release all locks after transaction completion.
- Avoid ad-hoc lock checks or manual queue manipulation.

## Example
```go
// Acquire locks
for _, lock := range tx.Locks {
    if !tm.LockManager.RequestAndWait(lock.ResourceID, lock, 60*time.Second) {
        // Handle lock acquisition failure
    }
}
// Release locks
tm.releaseLocksForTransaction(tx)
```

---
For further details, see `internal/database/transaction/lock.go` and `transaction.go`.
