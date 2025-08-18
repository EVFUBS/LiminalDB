package serializer

import (
	db "LiminalDb/internal/database"
	"bytes"
	"errors"
	"time"
)

func (b BinarySerializer) serializeValue(buf *bytes.Buffer, val any, col db.Column) error {
	if col.IsNullable {
		if val == nil {
			if err := b.writeData(buf, byte(0)); err != nil {
				return err
			}
			return nil
		} else {
			if err := b.writeData(buf, byte(1)); err != nil {
				return err
			}
		}
	}

	switch v := val.(type) {
	case int64:
		if col.DataType != db.TypeInteger64 {
			return errors.New("data type mismatch for column " + col.Name)
		}
		return b.writeData(buf, v)

	case float64:
		if col.DataType != db.TypeFloat64 {
			return errors.New("data type mismatch for column " + col.Name)
		}
		return b.writeData(buf, v)

	case string:
		if col.DataType != db.TypeString {
			return errors.New("data type mismatch for column " + col.Name)
		}
		strBytes := []byte(v)
		if uint16(len(strBytes)) > col.Length {
			return errors.New("string too long for column " + col.Name)
		}
		if err := b.writeData(buf, uint16(len(strBytes))); err != nil {
			return err
		}
		_, err := buf.Write(strBytes)
		return err

	case bool:
		if col.DataType != db.TypeBoolean {
			return errors.New("data type mismatch for column " + col.Name)
		}
		var boolByte byte
		if v {
			boolByte = 1
		} else {
			boolByte = 0
		}
		return b.writeData(buf, boolByte)

	case time.Time:
		if col.DataType != db.TypeDatetime {
			return errors.New("data type mismatch for column " + col.Name)
		}
		// Serialize as int64 (Unix seconds)
		return b.writeData(buf, v.Unix())

	default:
		return errors.New("unsupported data type for column " + col.Name)
	}
}
