package storedproc

import (
	"LiminalDb/internal/database"
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

const (
	StoredProcDir = "db/sproc"
	FileExtension = ".lsql"
)

type StoredProc struct {
	Name        string
	Body        string
	Parameters  []database.Column
	CreatedAt   time.Time
	ModifiedAt  time.Time
	Description string
}

type StoredProcMetadata struct {
	Name        string
	Parameters  []database.Column
	CreatedAt   time.Time
	ModifiedAt  time.Time
	Description string
}

func (s *StoredProc) WriteToFile(filename string) error {
	err := os.MkdirAll(StoredProcDir, 0755)
	if err != nil {
		return err
	}

	// Create metadata file
	metadata := StoredProcMetadata{
		Name:        s.Name,
		Parameters:  s.Parameters,
		CreatedAt:   s.CreatedAt,
		ModifiedAt:  time.Now(),
		Description: s.Description,
	}

	metadataBytes, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}

	metadataPath := filepath.Join(StoredProcDir, filename+".meta.json")
	err = os.WriteFile(metadataPath, metadataBytes, 0644)
	if err != nil {
		return err
	}

	// Create body file
	bodyPath := filepath.Join(StoredProcDir, filename+FileExtension)
	err = os.WriteFile(bodyPath, []byte(s.Body), 0644)
	if err != nil {
		return err
	}

	return nil
}

func (s *StoredProc) ReadFromFile(filename string) error {
	// Read metadata
	metadataPath := filepath.Join(StoredProcDir, filename+".meta.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	if err != nil {
		return err
	}

	var metadata StoredProcMetadata
	err = json.Unmarshal(metadataBytes, &metadata)
	if err != nil {
		return err
	}

	// Read body
	bodyPath := filepath.Join(StoredProcDir, filename+FileExtension)
	bodyBytes, err := os.ReadFile(bodyPath)
	if err != nil {
		return err
	}

	s.Name = metadata.Name
	s.Parameters = metadata.Parameters
	s.CreatedAt = metadata.CreatedAt
	s.ModifiedAt = metadata.ModifiedAt
	s.Description = metadata.Description
	s.Body = string(bodyBytes)

	return nil
}

func NewStoredProc(name string, body string, params []database.Column, description string) *StoredProc {
	now := time.Now()
	return &StoredProc{
		Name:        name,
		Body:        body,
		Parameters:  params,
		CreatedAt:   now,
		ModifiedAt:  now,
		Description: description,
	}
}
