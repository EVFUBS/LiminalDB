package transaction

import (
	"LiminalDb/internal/common"
	"LiminalDb/internal/database/operations"
	"sync"
	"time"
)

type LockType int

const (
	Shared LockType = iota
	Exclusive
)

type Lock struct {
	ResourceID    string // TableName:Row
	Type          LockType
	TransactionID string
	Timestamp     int64
}

type LockRequest struct {
	Lock      Lock
	Timestamp int64
	Granted   bool // Track whether this lock has been granted
}

type LockManager struct {
	mu        sync.Mutex               // Changed from RWMutex to regular Mutex for simpler, safer semantics
	LockQueue map[string][]LockRequest // ResourceID -> List of lock requests
}

// NewLockManager creates a new lock manager.
func NewLockManager() *LockManager {
	return &LockManager{
		LockQueue: make(map[string][]LockRequest),
	}
}

// RequestAndWait atomically requests a lock and waits for it to be granted.
// This consolidates requestLock + checkLock logic and prevents deadlocks.
// Returns false if the lock cannot be acquired within the timeout.
func (lm *LockManager) RequestAndWait(resourceID string, lock Lock, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	lm.mu.Lock()
	request := LockRequest{
		Lock:      lock,
		Timestamp: time.Now().Unix(),
		Granted:   false,
	}
	lm.LockQueue[resourceID] = append(lm.LockQueue[resourceID], request)
	lm.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		lm.mu.Lock()
		requests := lm.LockQueue[resourceID]

		requestIndex := -1
		for i, req := range requests {
			if req.Lock.TransactionID == lock.TransactionID && req.Lock.ResourceID == lock.ResourceID && req.Lock.Type == lock.Type {
				requestIndex = i
				break
			}
		}

		if requestIndex == -1 {
			lm.mu.Unlock()
			return false
		}

		if lm.canGrantLock(requests, requestIndex) {
			lm.LockQueue[resourceID][requestIndex].Granted = true
			lm.mu.Unlock()
			return true
		}

		lm.mu.Unlock()

		if time.Now().After(deadline) {
			lm.mu.Lock()
			for i, req := range lm.LockQueue[resourceID] {
				if req.Lock.TransactionID == lock.TransactionID && req.Lock.ResourceID == lock.ResourceID && req.Lock.Type == lock.Type {
					lm.LockQueue[resourceID] = append(
						lm.LockQueue[resourceID][:i],
						lm.LockQueue[resourceID][i+1:]...,
					)
					break
				}
			}
			if len(lm.LockQueue[resourceID]) == 0 {
				delete(lm.LockQueue, resourceID)
			}
			lm.mu.Unlock()
			return false
		}
	}

	return false
}

// canGrantLock checks if the lock at index can be granted given current queue state.
// Must be called while holding the mutex.
func (lm *LockManager) canGrantLock(requests []LockRequest, index int) bool {
	if index >= len(requests) || requests[index].Granted {
		return false
	}

	lockReq := requests[index]

	if lockReq.Lock.Type == Exclusive {
		// For exclusive locks: no other locks can be granted (even from earlier in the queue)
		for i := 0; i < len(requests); i++ {
			if i != index && requests[i].Granted {
				return false
			}
		}
		return true
	}

	// For shared locks: can be granted if no exclusive lock is granted or waiting ahead
	for i := 0; i <= index; i++ {
		req := requests[i]
		if req.Lock.Type == Exclusive && (req.Granted || i < index) {
			return false
		}
	}

	// Check if any exclusive lock is currently granted
	for i := 0; i < len(requests); i++ {
		if requests[i].Granted && requests[i].Lock.Type == Exclusive {
			return false
		}
	}
	return true
}

// ReleaseLock removes a granted lock for a transaction and resource.
// This triggers the lock manager to potentially grant waiting locks.
func (lm *LockManager) ReleaseLock(resourceID string, transactionID string, lockType LockType) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if requests, ok := lm.LockQueue[resourceID]; ok {
		for i, req := range requests {
			if req.Lock.TransactionID == transactionID && req.Lock.Type == lockType && req.Granted {
				// Remove the released lock
				lm.LockQueue[resourceID] = append(requests[:i], requests[i+1:]...)
				break
			}
		}
		if len(lm.LockQueue[resourceID]) == 0 {
			delete(lm.LockQueue, resourceID)
		}
	}
}

// GetLockQueueSnapshot returns a snapshot of the current lock queue state.
// Useful for debugging and monitoring.
func (lm *LockManager) GetLockQueueSnapshot() map[string][]LockRequest {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	snapshot := make(map[string][]LockRequest)
	for resourceID, requests := range lm.LockQueue {
		requestsCopy := make([]LockRequest, len(requests))
		copy(requestsCopy, requests)
		snapshot[resourceID] = requestsCopy
	}
	return snapshot
}

// DetermineLockType analyzes operations and returns the appropriate lock type.
// Write operations and DDL require Exclusive locks; reads use Shared locks.
func DetermineLockType(operations *[]operations.Operation) LockType {
	for _, op := range *operations {
		switch op.Type {
		case common.Read:
			return Shared
		case common.Write, common.Insert, common.Delete, common.Alter, common.CreateTable, common.DropTable,
			common.CreateProcedure, common.AlterProcedure, common.ExecuteProcedure,
			common.CreateIndex, common.DropIndex, common.Transaction:
			return Exclusive
		default:
			return Exclusive
		}
	}
	return Shared
}
