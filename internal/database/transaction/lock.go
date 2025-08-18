package transaction

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
	LockQueue map[string][]LockRequest // ResourceID -> List of lock requests
}

func (lm *LockManager) requestLock(resourceID string, lock Lock, timestamp int64) {
	request := LockRequest{
		Lock:      lock,
		Timestamp: timestamp,
	}
	lm.LockQueue[resourceID] = append(lm.LockQueue[resourceID], request)
}

func (lm *LockManager) releaseLock(resourceID string, lock Lock) {
	if requests, ok := lm.LockQueue[resourceID]; ok {
		for i, req := range requests {
			if req.Lock == lock {
				lm.LockQueue[resourceID] = append(requests[:i], requests[i+1:]...)
				break
			}
		}
	}
}

func (lm *LockManager) checkLock(resourceID string, lock Lock) bool {
	if requests, ok := lm.LockQueue[resourceID]; ok {
		for _, req := range requests {
			if req.Lock == lock {
				return true
			}
		}
	}
	return false
}
