package db

const MagicNumber uint32 = 0x4D444247
const CurrentVersion uint16 = 1

type Operations interface {
	CreateTable(metadata TableMetadata) error
	ReadMetadata(filename string) (TableMetadata, error)
	WriteRow(tableName string, data []interface{}) error
	ReadRows(tableName string, offset int64, limit int) ([][]interface{}, error)
}
