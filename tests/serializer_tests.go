package tests

import (
	"LiminalDb/internal/db"
	"bytes"
	"encoding/binary"
	"testing"
)

func TestSerializeHeader(t *testing.T) {
	tests := []struct {
		name    string
		header  db.FileHeader
		wantErr bool
	}{
		{
			name: "valid header",
			header: db.FileHeader{
				Magic:          0xAABBCCDD,
				Version:        1,
				MetadataLength: 64,
			},
			wantErr: false,
		},
		{
			name: "zero values",
			header: db.FileHeader{
				Magic:          0,
				Version:        0,
				MetadataLength: 0,
			},
			wantErr: false,
		},
		{
			name: "large header values",
			header: db.FileHeader{
				Magic:          0xFFFFFFFF,
				Version:        0xFFFF,
				MetadataLength: 0xFFFFFFFF,
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			serializer := db.BinarySerializer{}

			data, err := serializer.SerializeHeader(tc.header)
			if (err != nil) != tc.wantErr {
				t.Fatalf("unexpected error status: got %v, wantErr %v", err != nil, tc.wantErr)
			}

			if err == nil {
				var deserializedHeader db.FileHeader
				buf := bytes.NewReader(data)

				if err := binary.Read(buf, binary.LittleEndian, &deserializedHeader.Magic); err != nil {
					t.Errorf("failed to deserialize Magic: %v", err)
				}
				if err := binary.Read(buf, binary.LittleEndian, &deserializedHeader.Version); err != nil {
					t.Errorf("failed to deserialize Version: %v", err)
				}
				if err := binary.Read(buf, binary.LittleEndian, &deserializedHeader.MetadataLength); err != nil {
					t.Errorf("failed to deserialize MetadataLength: %v", err)
				}
				if deserializedHeader != tc.header {
					t.Errorf("deserialized header does not match: got %+v, want %+v", deserializedHeader, tc.header)
				}
			}
		})
	}
}
