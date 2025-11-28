package engine

import (
	"LiminalDb/internal/database/operations"
	tran "LiminalDb/internal/database/transaction"
)

type Request struct {
	Operations *[]operations.Operation
	ResponseCh chan []operations.Result
}

type Engine struct {
	TransactionManager *tran.TransactionManager
}

func NewEngine() *Engine {
	return &Engine{
		TransactionManager: tran.NewTransactionManager(),
	}
}

func (e *Engine) StartEngine(requestChannel <-chan *Request, stopCh chan any) {
	e.TransactionManager = tran.NewTransactionManager()

	for {
		select {
		case req := <-requestChannel:
			transaction := e.TransactionManager.NewTransaction(req.Operations)

			go func(req *Request, transaction *tran.Transaction) {
				result := e.TransactionManager.Execute(transaction)
				req.ResponseCh <- result
			}(req, transaction)

		case <-stopCh:
			return
		}
	}
}
