package serializer

import (
	db "LiminalDb/internal/database"
	"bytes"
)

func (b BinarySerializer) SerializeMetadata(metadata db.TableMetadata) ([]byte, uint32, error) {
	buf := new(bytes.Buffer)

	if err := b.writeString(buf, metadata.Name); err != nil {
		return nil, 0, err
	}

	if err := b.writeData(buf, metadata.ColumnCount); err != nil {
		return nil, 0, err
	}

	for _, col := range metadata.Columns {
		if err := b.writeString(buf, col.Name); err != nil {
			return nil, 0, err
		}

		if err := b.writeData(buf, col.DataType); err != nil {
			return nil, 0, err
		}

		if err := b.writeData(buf, col.Length); err != nil {
			return nil, 0, err
		}

		if err := b.writeData(buf, col.IsNullable); err != nil {
			return nil, 0, err
		}

		if err := b.writeData(buf, col.IsPrimaryKey); err != nil {
			return nil, 0, err
		}
	}

	if err := b.writeData(buf, metadata.RowCount); err != nil {
		return nil, 0, err
	}

	if err := b.writeData(buf, metadata.DataOffset); err != nil {
		return nil, 0, err
	}

	if err := b.writeData(buf, int64(len(metadata.ForeignKeys))); err != nil {
		return nil, 0, err
	}

	for _, foreignKey := range metadata.ForeignKeys {
		if err := b.writeString(buf, foreignKey.Name); err != nil {
			return nil, 0, err
		}

		if err := b.writeString(buf, foreignKey.ReferencedTable); err != nil {
			return nil, 0, err
		}

		if err := b.writeData(buf, int64(len(foreignKey.ReferencedColumns))); err != nil {
			return nil, 0, err
		}

		for _, referencedColumn := range foreignKey.ReferencedColumns {
			if err := b.writeString(buf, referencedColumn.ColumnName); err != nil {
				return nil, 0, err
			}

			if err := b.writeString(buf, referencedColumn.ReferencedColumnName); err != nil {
				return nil, 0, err
			}
		}
	}

	var indexCount int64
	if metadata.Indexes == nil {
		indexCount = 0
	} else {
		indexCount = int64(len(metadata.Indexes))
	}

	if err := b.writeData(buf, indexCount); err != nil {
		return nil, 0, err
	}

	if metadata.Indexes != nil {
		for _, idx := range metadata.Indexes {
			if err := b.writeString(buf, idx.Name); err != nil {
				return nil, 0, err
			}

			if err := b.writeData(buf, int64(len(idx.Columns))); err != nil {
				return nil, 0, err
			}

			for _, col := range idx.Columns {
				if err := b.writeString(buf, col); err != nil {
					return nil, 0, err
				}
			}

			if err := b.writeData(buf, idx.IsUnique); err != nil {
				return nil, 0, err
			}

			if err := b.writeData(buf, idx.IsPrimary); err != nil {
				return nil, 0, err
			}
		}
	}

	return buf.Bytes(), uint32(buf.Len()), nil
}

func (b BinarySerializer) DeserializeMetadata(buf *bytes.Reader) (db.TableMetadata, error) {
	var metadata db.TableMetadata

	tableName, err := b.readString(buf)
	if err != nil {
		return db.TableMetadata{}, err
	}
	metadata.Name = tableName

	if err := b.readData(buf, &metadata.ColumnCount); err != nil {
		return db.TableMetadata{}, err
	}

	metadata.Columns = make([]db.Column, metadata.ColumnCount)
	for i := range metadata.Columns {
		colName, err := b.readString(buf)
		if err != nil {
			return db.TableMetadata{}, err
		}
		metadata.Columns[i].Name = colName

		if err := b.readData(buf, &metadata.Columns[i].DataType); err != nil {
			return db.TableMetadata{}, err
		}

		if err := b.readData(buf, &metadata.Columns[i].Length); err != nil {
			return db.TableMetadata{}, err
		}

		if err := b.readData(buf, &metadata.Columns[i].IsNullable); err != nil {
			return db.TableMetadata{}, err
		}

		if err := b.readData(buf, &metadata.Columns[i].IsPrimaryKey); err != nil {
			return db.TableMetadata{}, err
		}
	}

	if err := b.readData(buf, &metadata.RowCount); err != nil {
		return db.TableMetadata{}, err
	}

	if err := b.readData(buf, &metadata.DataOffset); err != nil {
		return db.TableMetadata{}, err
	}

	var foreignKeyCount int64
	if err := b.readData(buf, &foreignKeyCount); err != nil {
		return metadata, err
	}

	metadata.ForeignKeys = make([]db.ForeignKeyConstraint, foreignKeyCount)
	for i := range metadata.ForeignKeys {
		metadata.ForeignKeys[i].Name, err = b.readString(buf)
		if err != nil {
			return db.TableMetadata{}, err
		}

		metadata.ForeignKeys[i].ReferencedTable, err = b.readString(buf)
		if err != nil {
			return db.TableMetadata{}, err
		}

		var referencedColumnCount int64
		if err := b.readData(buf, &referencedColumnCount); err != nil {
			return db.TableMetadata{}, err
		}

		metadata.ForeignKeys[i].ReferencedColumns = make([]db.ForeignKeyReference, referencedColumnCount)
		for j := range metadata.ForeignKeys[i].ReferencedColumns {
			metadata.ForeignKeys[i].ReferencedColumns[j].ColumnName, err = b.readString(buf)
			if err != nil {
				return db.TableMetadata{}, err
			}

			metadata.ForeignKeys[i].ReferencedColumns[j].ReferencedColumnName, err = b.readString(buf)
			if err != nil {
				return db.TableMetadata{}, err
			}
		}
	}

	var indexCount int64
	if err := b.readData(buf, &indexCount); err != nil {
		return metadata, nil
	}

	metadata.Indexes = make([]db.IndexMetadata, indexCount)
	for i := range metadata.Indexes {
		idxName, err := b.readString(buf)
		if err != nil {
			return db.TableMetadata{}, err
		}
		metadata.Indexes[i].Name = idxName

		var columnCount int64
		if err := b.readData(buf, &columnCount); err != nil {
			return db.TableMetadata{}, err
		}

		metadata.Indexes[i].Columns = make([]string, columnCount)
		for j := range metadata.Indexes[i].Columns {
			colName, err := b.readString(buf)
			if err != nil {
				return db.TableMetadata{}, err
			}
			metadata.Indexes[i].Columns[j] = colName
		}

		if err := b.readData(buf, &metadata.Indexes[i].IsUnique); err != nil {
			return db.TableMetadata{}, err
		}

		if err := b.readData(buf, &metadata.Indexes[i].IsPrimary); err != nil {
			return db.TableMetadata{}, err
		}
	}

	return metadata, nil
}
