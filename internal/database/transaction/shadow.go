package transaction

import (
	"LiminalDb/internal/database"
	"LiminalDb/internal/database/common"
	"fmt"
	"os"
	"path/filepath"
)

// ShadowManager manages shadow copies of files for transaction isolation.
// All operations write to shadow files, then commit atomically renames them to real files.
type ShadowManager struct {
	transactionID string
	shadowDir     string
	shadowFiles   map[string]string // original path → shadow path
	tableNames    map[string]bool   // track tables involved in transaction
	droppedTables map[string]bool   // tracks tables dropped during transaction
}

// NewShadowManager creates a new shadow manager for a transaction.
func NewShadowManager(transactionID string) *ShadowManager {
	shadowDir := filepath.Join(database.DatabaseDir, "shadow", transactionID)
	return &ShadowManager{
		transactionID: transactionID,
		shadowDir:     shadowDir,
		shadowFiles:   make(map[string]string),
		tableNames:    make(map[string]bool),
		droppedTables: make(map[string]bool),
	}
}

// CreateShadowForTable creates shadow copies of all files for a table (table file + all index files).
// If the table doesn't exist yet (CREATE TABLE operation), this is a no-op.
func (sm *ShadowManager) CreateShadowForTable(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	sm.tableNames[tableName] = true

	if err := os.MkdirAll(sm.shadowDir, 0700); err != nil {
		return fmt.Errorf("failed to create shadow directory: %w", err)
	}

	originalTablePath := common.GetTableFilePath(tableName)
	if _, err := os.Stat(originalTablePath); os.IsNotExist(err) {
		return nil
	}

	shadowTablePath := filepath.Join(sm.shadowDir, tableName+database.FileExtension)
	if err := sm.copyFile(originalTablePath, shadowTablePath); err != nil {
		return fmt.Errorf("failed to copy table file to shadow: %w", err)
	}
	sm.shadowFiles[originalTablePath] = shadowTablePath

	tableFolderPath := common.GetTableFolderPath(tableName)
	entries, err := os.ReadDir(tableFolderPath)
	if err != nil {
		return fmt.Errorf("failed to read table folder: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == tableName+database.FileExtension {
			continue
		}

		originalIndexPath := filepath.Join(tableFolderPath, entry.Name())
		shadowIndexPath := filepath.Join(sm.shadowDir, entry.Name())
		if err := sm.copyFile(originalIndexPath, shadowIndexPath); err != nil {
			return fmt.Errorf("failed to copy index file %s to shadow: %w", entry.Name(), err)
		}
		sm.shadowFiles[originalIndexPath] = shadowIndexPath
	}

	return nil
}

// GetWorkingPath returns the path to use for file operations.
// During a transaction, this returns the shadow path if one exists, otherwise the original path.
func (sm *ShadowManager) GetWorkingPath(originalPath string) string {
	if shadowPath, exists := sm.shadowFiles[originalPath]; exists {
		return shadowPath
	}
	return originalPath
}

// GetWorkingTablePath returns the working path for a table's main file.
func (sm *ShadowManager) GetWorkingTablePath(tableName string) string {
	originalPath := common.GetTableFilePath(tableName)
	if shadowPath, exists := sm.shadowFiles[originalPath]; exists {
		return shadowPath
	}

	if sm.tableNames[tableName] {
		return filepath.Join(sm.shadowDir, tableName+database.FileExtension)
	}
	return originalPath
}

// GetWorkingIndexPath returns the working path for an index file.
func (sm *ShadowManager) GetWorkingIndexPath(tableName, indexName string) string {
	originalPath := common.GetIndexFilePath(tableName, indexName)
	if shadowPath, exists := sm.shadowFiles[originalPath]; exists {
		return shadowPath
	}

	if sm.tableNames[tableName] {
		return filepath.Join(sm.shadowDir, fmt.Sprintf("%s_%s.idx", tableName, indexName))
	}
	return originalPath
}

// CommitShadows atomically commits all shadow files by renaming them to their original locations.
func (sm *ShadowManager) CommitShadows() error {
	shadowEntries, err := os.ReadDir(sm.shadowDir)

	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read shadow directory: %w", err)
	}

	for _, entry := range shadowEntries {
		if entry.IsDir() {
			continue
		}

		shadowPath := filepath.Join(sm.shadowDir, entry.Name())

		isNewFile := true
		for _, existingShadowPath := range sm.shadowFiles {
			if existingShadowPath == shadowPath {
				isNewFile = false
				break
			}
		}

		if isNewFile {
			if err := sm.commitNewFile(entry.Name(), shadowPath); err != nil {
				return err
			}
		}
	}

	for originalPath, shadowPath := range sm.shadowFiles {
		if err := os.Rename(shadowPath, originalPath); err != nil {
			return fmt.Errorf("failed to commit shadow file %s to %s: %w", shadowPath, originalPath, err)
		}
	}

	for tableName := range sm.droppedTables {
		if err := common.DeleteTableFolder(tableName); err != nil {
			return fmt.Errorf("failed to delete table folder for dropped table %s: %w", tableName, err)
		}
	}

	return sm.CleanupShadows()
}

// commitNewFile moves a new file from shadow directory to its proper location.
func (sm *ShadowManager) commitNewFile(fileName, shadowPath string) error {
	var targetPath string

	if filepath.Ext(fileName) == database.FileExtension {
		// This is a table file
		tableName := fileName[:len(fileName)-len(database.FileExtension)]
		if _, err := common.CreateTableFolder(tableName); err != nil {
			return fmt.Errorf("failed to create table folder for %s: %w", tableName, err)
		}
		targetPath = common.GetTableFilePath(tableName)
	} else if filepath.Ext(fileName) == ".idx" {
		// This is an index file - extract table name from filename (format: tableName_indexName.idx)
		baseName := filepath.Base(fileName)
		parts := []rune(baseName)
		var tableName string

		// Find the table name by looking for the first underscore
		for i := 0; i < len(parts); i++ {
			if parts[i] == '_' {
				tableName = string(parts[:i])
				break
			}
		}

		if tableName == "" {
			return fmt.Errorf("unable to extract table name from index file: %s", fileName)
		}

		if _, err := common.CreateTableFolder(tableName); err != nil {
			return fmt.Errorf("failed to create table folder for index %s: %w", fileName, err)
		}

		targetPath = filepath.Join(common.GetTableFolderPath(tableName), baseName)
	} else {
		return fmt.Errorf("unknown file type: %s", fileName)
	}

	if targetPath == "" {
		return fmt.Errorf("unable to determine target path for new file: %s", fileName)
	}

	return os.Rename(shadowPath, targetPath)
}

// CleanupShadows removes all shadow files and the shadow directory.
func (sm *ShadowManager) CleanupShadows() error {
	if err := os.RemoveAll(sm.shadowDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to cleanup shadow directory: %w", err)
	}
	return nil
}

// MarkTableToBeDropped marks a table to be dropped during the transaction.
func (sm *ShadowManager) MarkTableToBeDropped(tableName string) {
	if _, ok := sm.droppedTables[tableName]; !ok {
		sm.droppedTables[tableName] = true
	}
}

// copyFile copies a file from src to dst.
func (sm *ShadowManager) copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0666)
}
