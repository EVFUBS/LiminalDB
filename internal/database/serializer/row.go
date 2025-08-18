package serializer

import (
	db "LiminalDb/internal/database"
	"bytes"
	"encoding/binary"
	"time"
)

func (b BinarySerializer) SerializeRow(data []any, columns []db.Column) ([]byte, error) {
	buf := new(bytes.Buffer)

	for i, val := range data {
		col := columns[i]
		if err := b.serializeValue(buf, val, col); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (b BinarySerializer) DeserializeRow(buf *bytes.Reader, columns []db.Column) ([]any, error) {
	var row []any

	for _, col := range columns {
		if col.IsNullable {
			var nullFlag byte
			if err := binary.Read(buf, binary.LittleEndian, &nullFlag); err != nil {
				return nil, err
			}
			if nullFlag == 0 {
				row = append(row, nil)
				continue
			}
		}
		switch col.DataType {
		case db.TypeInteger64:
			var val int64
			if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
				return nil, err
			}
			row = append(row, val)
		case db.TypeFloat64:
			var val float64
			if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
				return nil, err
			}
			row = append(row, val)
		case db.TypeString:
			var strLen uint16
			if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
				return nil, err
			}

			var strBytes []byte

			if strLen > 0 {
				strBytes = make([]byte, strLen)
				if _, err := buf.Read(strBytes); err != nil {
					return nil, err
				}
			}
			row = append(row, string(strBytes))
		case db.TypeBoolean:
			var boolByte byte
			if err := binary.Read(buf, binary.LittleEndian, &boolByte); err != nil {
				return nil, err
			}
			row = append(row, boolByte == 1)
		case db.TypeDatetime:
			var unixSec int64
			if err := binary.Read(buf, binary.LittleEndian, &unixSec); err != nil {
				return nil, err
			}
			row = append(row, time.Unix(unixSec, 0).UTC())
		default:
			panic("unhandled default case")
		}
	}

	return row, nil
}
