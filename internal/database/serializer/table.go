package serializer

import (
	db "LiminalDb/internal/database"
	"LiminalDb/internal/database/common"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func (b BinarySerializer) SerializeTable(table *db.Table) ([]byte, error) {
	buf := new(bytes.Buffer)

	_, metadataLength, err := b.SerializeMetadata(table.Metadata)
	if err != nil {
		return nil, err
	}

	table.Header.MetadataLength = metadataLength

	headerBytes, err := b.SerializeHeader(table.Header)
	if err != nil {
		return nil, err
	}

	dataOffset := uint32(len(headerBytes)) + metadataLength
	table.Metadata.DataOffset = dataOffset
	table.Metadata.RowCount = int64(len(table.Data))
	table.Metadata.ColumnCount = int64(len(table.Metadata.Columns))

	metadataBytes, _, err := b.SerializeMetadata(table.Metadata)
	if err != nil {
		return nil, err
	}

	dataBytes := make([][]byte, len(table.Data))
	for i, row := range table.Data {
		dataBytes[i], err = b.SerializeRow(row, table.Metadata.Columns)
		if err != nil {
			return nil, err
		}
	}

	buf.Write(headerBytes)
	buf.Write(metadataBytes)
	for _, row := range dataBytes {
		buf.Write(row)
	}

	return buf.Bytes(), nil
}

func (b BinarySerializer) DeserializeTable(data []byte) (*db.Table, error) {
	buf := bytes.NewReader(data)

	header, err := b.DeserializeHeader(buf)
	if err != nil {
		return nil, err
	}

	metadata, err := b.DeserializeMetadata(buf)
	if err != nil {
		return nil, err
	}

	rows := make([][]any, metadata.RowCount)
	for i := range rows {
		rows[i], err = b.DeserializeRow(buf, metadata.Columns)
		if err != nil {
			return nil, err
		}
	}

	return &db.Table{Header: header, Metadata: metadata, Data: rows}, nil
}

func (b BinarySerializer) ReadTableFromPath(path string) (*db.Table, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	serialisedTable, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return b.DeserializeTable(serialisedTable)
}

func (b BinarySerializer) ListTables() ([]string, error) {
	files, err := os.ReadDir(db.TableDir)
	if err != nil {
		return nil, err
	}

	fileNames := make([]string, 0)
	for _, file := range files {
		if !file.IsDir() {
			fileNames = append(fileNames, file.Name())
		}
	}

	tableNames := make([]string, 0)
	for _, tableName := range fileNames {
		if strings.HasSuffix(tableName, ".bin") {
			name := strings.TrimSuffix(tableName, ".bin")
			tableNames = append(tableNames, name)
		}
	}

	return tableNames, nil
}

func (b BinarySerializer) WriteTable(table *db.Table, dbFileName string) error {
	return b.WriteTableToPath(table, dbFileName, "")
}

// WriteTableToPath writes a table to a specific path (used for shadow files)
func (b BinarySerializer) WriteTableToPath(table *db.Table, dbFileName string, targetPath string) error {
	serialisedTable, err := b.SerializeTable(table)
	if err != nil {
		return err
	}

	if _, err := os.Stat(db.TableDir); os.IsNotExist(err) {
		err = os.MkdirAll(db.TableDir, 0700)
		if err != nil {
			return err
		}
	}

	var filePath string
	if targetPath != "" {
		// Use the provided target path (for shadow files)
		filePath = targetPath
	} else {
		// Use the standard path
		path, err := common.CreateTableFolder(dbFileName)
		if err != nil {
			return err
		}
		filePath = filepath.Join(path, dbFileName+db.FileExtension)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(serialisedTable)
	if err != nil {
		return err
	}

	return nil
}
