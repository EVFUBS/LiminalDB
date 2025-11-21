package storedprocedure

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

type StoredProcedure struct {
	Name        string
	Body        string
	Parameters  []database.Column
	CreatedAt   time.Time
	ModifiedAt  time.Time
	Description string
}

type StoredProcedureMetadata struct {
	Name        string
	Parameters  []database.Column
	CreatedAt   time.Time
	ModifiedAt  time.Time
	Description string
}

func (s *StoredProcedure) WriteToFile(filename string) error {
	err := os.MkdirAll(StoredProcDir, 0755)
	if err != nil {
		return err
	}

	// Create metadata file
	metadata := StoredProcedureMetadata{
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

	bodyPath := filepath.Join(StoredProcDir, filename+FileExtension)
	err = os.WriteFile(bodyPath, []byte(s.Body), 0644)
	if err != nil {
		return err
	}

	return nil
}

func (s *StoredProcedure) ReadFromFile(filename string) error {
	// Read metadata
	metadataPath := filepath.Join(StoredProcDir, filename+".meta.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	if err != nil {
		return err
	}

	var metadata StoredProcedureMetadata
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

func NewStoredProcedure(name string, body string, params []database.Column, description string) *StoredProcedure {
	now := time.Now()
	return &StoredProcedure{
		Name:        name,
		Body:        body,
		Parameters:  params,
		CreatedAt:   now,
		ModifiedAt:  now,
		Description: description,
	}
}

// need ot pass in params
// func (s *StoredProcedure) Execute() (any, error) {
// 	err := s.ReadFromFile(s.Name)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to read stored procedure: %w", err)
// 	}

// 	if len(s.Parameters) != len(s.Parameters) {
// 		return nil, fmt.Errorf("parameter count mismatch: expected %d, got %d",
// 			len(s.Parameters), len(s.Parameters))
// 	}

// 	paramValues := make(map[string]any)
// 	for i, param := range s.Parameters {
// 		paramValues[param.Name] = stmt.Parameters[i].GetValue()
// 	}

// 	processedBody := s.Body
// 	for name, value := range paramValues {
// 		var valueStr string
// 		switch v := value.(type) {
// 		case string:
// 			valueStr = "'" + v + "'"
// 		default:
// 			valueStr = fmt.Sprintf("%v", v)
// 		}

// 		processedBody = strings.Replace(processedBody, name, valueStr, -1)
// 	}

// 	// Split the body into individual statements
// 	statements := strings.Split(processedBody, ";")
// 	var lastResult any
// 	var lastErr error

// 	// Evaluate each statement
// 	for _, statement := range statements {
// 		statement = strings.TrimSpace(statement)
// 		if statement == "" {
// 			continue
// 		}

// 		lexer := lex.NewLexer(statement)
// 		parser := p.NewParser(lexer)

// 		stmt, err := parser.ParseStatement()
// 		if err != nil || stmt == nil {
// 			return nil, fmt.Errorf("failed to parse statement in stored procedure: %s", statement)
// 		}

// 		lastResult, lastErr = e.evaluateStatement(stmt)
// 		if lastErr != nil {
// 			return nil, fmt.Errorf("failed to execute statement in stored procedure: %w", lastErr)
// 		}
// 	}

// 	return lastResult, nil
// }
