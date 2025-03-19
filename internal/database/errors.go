package database

type DBError struct {
	Code    int
	Message string
	Err     error
}

const (
	ErrTableNotFound = iota
	ErrInvalidData
	ErrIO
	ErrCorruptFile
)
