package operations

import (
	"LiminalDb/internal/database/common"
	"fmt"
	"os"
)

func (o *OperationsImpl) DropTable(op *Operation) *Result {
	logger.Info("Dropping table: %s", op.TableName)

	table, err := o.Serializer.ReadTableFromPath(o.getWorkingTablePath(op, op.TableName))
	if err != nil {
		if os.IsNotExist(err) {
			logger.Info("Table %s does not exist", op.TableName)
			return &Result{Err: fmt.Errorf("table %s does not exist", op.TableName)}
		}
		logger.Error("Failed to read table %s: %v", op.TableName, err)
		return &Result{Err: err}
	}

	// Delete index files from working path (shadow or real)
	for _, idx := range table.Metadata.Indexes {
		workingIndexPath := o.getWorkingIndexPath(op, op.TableName, idx.Name)
		if err := os.Remove(workingIndexPath); err != nil && !os.IsNotExist(err) {
			logger.Error("Failed to remove index file %s: %v", workingIndexPath, err)
		} else {
			logger.Info("Dropped index %s from table %s", idx.Name, op.TableName)
		}
	}

	if sm, ok := op.ShadowManager.(ShadowManagerProvider); ok {
		sm.MarkTableToBeDropped(op.TableName)
	}

	// If not in a transaction (no shadow manager), also delete the table folder
	if op.ShadowManager == nil {
		err = common.DeleteTableFolder(op.TableName)
		if err != nil {
			logger.Error("Failed to drop table folder %s: %v", op.TableName, err)
			return &Result{Err: err}
		}
	}

	logger.Info("Table %s dropped successfully", op.TableName)
	return &Result{Message: fmt.Sprintf("Table %s dropped successfully", op.TableName)}
}

func (o *OperationsImpl) DropConstraint(op *Operation) *Result {
	logger.Info("Dropping constraint: %s", op.ConstraintName)

	table, err := o.Serializer.ReadTableFromPath(o.getWorkingTablePath(op, op.TableName))
	if err != nil {
		logger.Error("Failed to read table %s: %v", op.TableName, err)
		return &Result{Err: err}
	}

	for i := len(table.Metadata.ForeignKeys) - 1; i >= 0; i-- {
		if table.Metadata.ForeignKeys[i].Name == op.ConstraintName {
			table.Metadata.ForeignKeys = append(table.Metadata.ForeignKeys[:i], table.Metadata.ForeignKeys[i+1:]...)
		}
	}

	if err := o.writeTableWithShadow(op, table, op.TableName); err != nil {
		logger.Error("Failed to save table after dropping constraint: %v", err)
		return &Result{Err: err}
	}

	logger.Info("Constraint %s dropped successfully from table %s", op.ConstraintName, op.TableName)
	return &Result{}
}
