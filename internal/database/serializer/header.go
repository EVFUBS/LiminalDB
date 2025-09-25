package serializer

import (
	db "LiminalDb/internal/database"
	"bytes"
	"errors"
)

func (b BinarySerializer) SerializeHeader(header db.FileHeader) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := b.writeData(buf, header.Magic); err != nil {
		return nil, err
	}

	if err := b.writeData(buf, header.Version); err != nil {
		return nil, err
	}

	if err := b.writeData(buf, header.MetadataLength); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (b BinarySerializer) DeserializeHeader(buf *bytes.Reader) (db.FileHeader, error) {
	var header db.FileHeader

	if err := b.readData(buf, &header.Magic); err != nil {
		return header, err
	}

	if header.Magic != db.MagicNumber {
		return db.FileHeader{}, errors.New("invalid magic number")
	}

	if err := b.readData(buf, &header.Version); err != nil {
		return header, err
	}

	if err := b.readData(buf, &header.MetadataLength); err != nil {
		return header, err
	}

	return header, nil
}
