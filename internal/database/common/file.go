package common

import (
	"LiminalDb/internal/database"
	"fmt"
	"os"
	"path/filepath"
)

// GetTableFilePath returns the full file path for a table's binary data file.
// The returned path follows the pattern: TableDir/tableName/tableName.bin
func GetTableFilePath(tableName string) string {
	return filepath.Join(database.TableDir, tableName, tableName+database.FileExtension)
}

// GetShadowTableFolderPath returns the path for a shadow table folder.
// Shadow tables are used for transaction isolation and MVCC.
func GetShadowTableFolderPath(tableName string) string {
	return filepath.Join(database.DatabaseDir, "shadows", tableName)
}

// GetTableFolderPath returns the path for a table's folder.
func GetTableFolderPath(tableName string) string {
	return filepath.Join(database.TableDir, tableName)
}

// GetIndexFilePath returns the file path for an index file.
// Index files are stored in the table directory with the pattern: tableName_indexName.idx
func GetIndexFilePath(tableName, indexName string) string {
	return filepath.Join(database.TableDir, tableName, fmt.Sprintf("%s_%s.idx", tableName, indexName))
}

// GetShadowIndexFilePath returns the file path for a shadow index file.
// Shadow index files are stored in the shadow directory.
func GetShadowIndexFilePath(tableName, indexName string) string {
	return filepath.Join(GetShadowTableFolderPath(tableName), fmt.Sprintf("%s_%s.idx", tableName, indexName))
}

// CreateShadowTableFolder creates a shadow table folder if it doesn't exist.
// Shadow tables are used for transaction isolation and MVCC.
// Returns the folder path and any error encountered.
func CreateShadowTableFolder(tableName string) (string, error) {
	if tableName == "" {
		return "", fmt.Errorf("table name cannot be empty")
	}

	path := GetShadowTableFolderPath(tableName)
	if err := ensureDirectoryExists(path); err != nil {
		return "", fmt.Errorf("failed to create shadow table folder for %q: %w", tableName, err)
	}
	return path, nil
}

// CreateTableFolder creates a table folder if it doesn't exist.
// Returns the folder path and any error encountered.
func CreateTableFolder(tableName string) (string, error) {
	if tableName == "" {
		return "", fmt.Errorf("table name cannot be empty")
	}

	path := GetTableFolderPath(tableName)
	if err := ensureDirectoryExists(path); err != nil {
		return "", fmt.Errorf("failed to create table folder for %q: %w", tableName, err)
	}
	return path, nil
}

// CopyTableToShadow copies a table's binary file to a shadow file path.
func CopyTableToShadow(tableName, shadowFolderPath string) error {
	originalFolderPath := GetTableFolderPath(tableName)

	entries, err := os.ReadDir(originalFolderPath)
	if err != nil {
		return fmt.Errorf("failed to read table folder %q: %w", originalFolderPath, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		originalFilePath := filepath.Join(originalFolderPath, entry.Name())
		shadowFilePath := filepath.Join(shadowFolderPath, entry.Name())

		input, err := os.ReadFile(originalFilePath)
		if err != nil {
			return fmt.Errorf("failed to read file %q: %w", originalFilePath, err)
		}

		if err := os.WriteFile(shadowFilePath, input, 0666); err != nil {
			return fmt.Errorf("failed to write shadow file %q: %w", shadowFilePath, err)
		}
	}

	return nil
}

// DeleteTableFolder removes a table's folder and all its contents.
// This operation is irreversible and should be used with caution.
func DeleteTableFolder(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	path := GetTableFolderPath(tableName)
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("failed to delete table folder for %q: %w", tableName, err)
	}
	return nil
}

// SaveIndexToFile writes an index's serialized data to disk.
func SaveIndexToFile(indexBytes []byte, tableName, indexName string) error {
	if tableName == "" || indexName == "" {
		return fmt.Errorf("table name and index name cannot be empty")
	}

	indexFilePath := GetIndexFilePath(tableName, indexName)
	if err := os.WriteFile(indexFilePath, indexBytes, 0666); err != nil {
		return fmt.Errorf("failed to write index file for %q on table %q: %w", indexName, tableName, err)
	}
	return nil
}

// LoadIndexFromFile reads an index's serialized data from disk.
// Returns the index bytes and any error encountered.
func LoadIndexFromFile(tableName, indexName string) ([]byte, error) {
	if tableName == "" || indexName == "" {
		return nil, fmt.Errorf("table name and index name cannot be empty")
	}

	indexFilePath := GetIndexFilePath(tableName, indexName)
	indexBytes, err := os.ReadFile(indexFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read index file for %q on table %q: %w", indexName, tableName, err)
	}
	return indexBytes, nil
}

// DeleteIndexFile removes an index file from disk.
func DeleteIndexFile(tableName, indexName string) error {
	if tableName == "" || indexName == "" {
		return fmt.Errorf("table name and index name cannot be empty")
	}

	indexFilePath := GetIndexFilePath(tableName, indexName)
	if err := os.Remove(indexFilePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete index file for %q on table %q: %w", indexName, tableName, err)
	}
	return nil
}

// IndexFileExists checks if an index file exists on disk.
func IndexFileExists(tableName, indexName string) bool {
	indexFilePath := GetIndexFilePath(tableName, indexName)
	_, err := os.Stat(indexFilePath)
	return err == nil
}

// CreateShadowIndexCopy copies an index file to a shadow location.
// This is used for transaction isolation.
func CreateShadowIndexCopy(tableName, indexName string) error {
	if tableName == "" || indexName == "" {
		return fmt.Errorf("table name and index name cannot be empty")
	}

	if _, err := CreateShadowTableFolder(tableName); err != nil {
		return err
	}

	originalPath := GetIndexFilePath(tableName, indexName)
	shadowPath := GetShadowIndexFilePath(tableName, indexName)

	indexBytes, err := os.ReadFile(originalPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read original index file for %q on table %q: %w", indexName, tableName, err)
	}

	if err := os.WriteFile(shadowPath, indexBytes, 0666); err != nil {
		return fmt.Errorf("failed to write shadow index file for %q on table %q: %w", indexName, tableName, err)
	}

	return nil
}

// DeleteShadowIndexCopy removes a shadow copy of an index file.
func DeleteShadowIndexCopy(tableName, indexName string) error {
	if tableName == "" || indexName == "" {
		return fmt.Errorf("table name and index name cannot be empty")
	}

	shadowPath := GetShadowIndexFilePath(tableName, indexName)
	if err := os.Remove(shadowPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete shadow index file for %q on table %q: %w", indexName, tableName, err)
	}
	return nil
}

// RestoreShadowIndexCopy restores an index from its shadow copy.
// This is used for transaction rollback.
func RestoreShadowIndexCopy(tableName, indexName string) error {
	if tableName == "" || indexName == "" {
		return fmt.Errorf("table name and index name cannot be empty")
	}

	shadowPath := GetShadowIndexFilePath(tableName, indexName)
	originalPath := GetIndexFilePath(tableName, indexName)

	indexBytes, err := os.ReadFile(shadowPath)
	if err != nil {
		if os.IsNotExist(err) {
			return DeleteIndexFile(tableName, indexName)
		}
		return fmt.Errorf("failed to read shadow index file for %q on table %q: %w", indexName, tableName, err)
	}

	if err := os.WriteFile(originalPath, indexBytes, 0666); err != nil {
		return fmt.Errorf("failed to restore index file for %q on table %q: %w", indexName, tableName, err)
	}

	return nil
}

// ensureDirectoryExists creates a directory and any necessary parent directories.
// If the directory already exists, it returns nil.
func ensureDirectoryExists(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0700); err != nil {
			return err
		}
	}
	return nil
}
