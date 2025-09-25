package transaction

import "sync"

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
}

type LockManager struct {
	mu        sync.RWMutex
	LockQueue map[string][]LockRequest // ResourceID -> List of lock requests
}

// NewLockManager creates a new lock manager.
func NewLockManager() *LockManager {
	return &LockManager{
		LockQueue: make(map[string][]LockRequest),
	}
}

// requestLock appends a lock request to the queue for the resource in a thread-safe way.
func (lm *LockManager) requestLock(resourceID string, lock Lock, timestamp int64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	request := LockRequest{
		Lock:      lock,
		Timestamp: timestamp,
	}
	lm.LockQueue[resourceID] = append(lm.LockQueue[resourceID], request)
}

// releaseLock removes the matching lock request for the transaction and resource.
// It matches by TransactionID and Type (so callers need not hold the exact struct pointer).
func (lm *LockManager) releaseLock(resourceID string, lock Lock) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if requests, ok := lm.LockQueue[resourceID]; ok {
		for i, req := range requests {
			if req.Lock.TransactionID == lock.TransactionID && req.Lock.Type == lock.Type {
				lm.LockQueue[resourceID] = append(requests[:i], requests[i+1:]...)
				break
			}
		}
		// Clean up empty slice to avoid growth of map with empty slices
		if len(lm.LockQueue[resourceID]) == 0 {
			delete(lm.LockQueue, resourceID)
		}
	}
}

// checkLock returns true when the provided lock request is allowed to proceed.
// For Exclusive locks the request must be the first in the queue.
// For Shared locks the request is allowed when there are no earlier Exclusive requests owned by other transactions.
func (lm *LockManager) checkLock(resourceID string, lock Lock) bool {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	requests, ok := lm.LockQueue[resourceID]
	if !ok || len(requests) == 0 {
		return false
	}

	var idx int = -1
	for i, req := range requests {
		if req.Lock.TransactionID == lock.TransactionID && req.Lock.Type == lock.Type {
			idx = i
			break
		}
	}
	if idx == -1 {
		return false
	}

	if lock.Type == Exclusive {
		return idx == 0
	}

	for i := 0; i < idx; i++ {
		if requests[i].Lock.Type == Exclusive && requests[i].Lock.TransactionID != lock.TransactionID {
			return false
		}
	}
	return true
}

func (lm *LockManager) GetLockQueueSnapshot() map[string][]LockRequest {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	snapshot := make(map[string][]LockRequest)
	for resourceID, requests := range lm.LockQueue {
		requestsCopy := make([]LockRequest, len(requests))
		copy(requestsCopy, requests)
		snapshot[resourceID] = requestsCopy
	}
	return snapshot
}
