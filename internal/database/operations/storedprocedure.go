package operations

import "fmt"

func (oi *OperationsImpl) CreateStoredProcedure(op *Operation) *Result {
	err := op.StoredProcedureOperation.StoredProcedure.WriteToFile(op.StoredProcedureOperation.StoredProcedure.Name)
	if err != nil {
		return &Result{Err: fmt.Errorf("failed to write stored procedure to file: %v", err)}
	}

	return &Result{}
}

/* func (oi *OperationsImpl) ExecuteStoredProcedure(op *Operation) *Result {
	op.StoredProcedureOperation.StoredProcedure.Execute()
	return &Result{}
}

func (oi *OperationsImpl) AlterStoredProcedure(op *Operation) *Result {
	op.StoredProcedureOperation.StoredProcedure.Alter()
	return &Result{}
}
 */