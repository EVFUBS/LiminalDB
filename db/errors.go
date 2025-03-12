package db

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
