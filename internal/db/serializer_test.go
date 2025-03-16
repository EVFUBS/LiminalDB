package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestSerializeHeader(t *testing.T) {
	serializer := BinarySerializer{}

	tests := []struct {
		name        string
		header      FileHeader
		expectErr   bool
		expectedLen int
	}{
		{
			name:        "ValidHeader",
			header:      FileHeader{Magic: 0xAABBCCDD, Version: 1, MetadataLength: 128},
			expectErr:   false,
			expectedLen: 10, // 4 bytes Magic + 2 bytes Version + 4 bytes MetadataLength
		},
		{
			name:        "EmptyHeader",
			header:      FileHeader{},
			expectErr:   false,
			expectedLen: 10, // 4 bytes Magic + 2 bytes Version + 4 bytes MetadataLength
		},
		{
			name:        "LargeMetadata",
			header:      FileHeader{Magic: 0x12345678, Version: 2, MetadataLength: 1 << 24},
			expectErr:   false,
			expectedLen: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := serializer.SerializeHeader(tt.header)

			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("did not expect error but got: %v", err)
				return
			}

			if len(data) != tt.expectedLen {
				t.Errorf("expected serialized header length %d, got %d", tt.expectedLen, len(data))
			}

			// Validate that the serialized data matches the input
			buf := bytes.NewReader(data)
			var deserializedHeader FileHeader
			if binary.Read(buf, binary.LittleEndian, &deserializedHeader.Magic) != nil ||
				binary.Read(buf, binary.LittleEndian, &deserializedHeader.Version) != nil ||
				binary.Read(buf, binary.LittleEndian, &deserializedHeader.MetadataLength) != nil {
				t.Errorf("error deserializing binary data")
			}

			if deserializedHeader != tt.header {
				t.Errorf("expected deserialized header %+v, got %+v", tt.header, deserializedHeader)
			}
		})
	}
}

func TestDeserializeHeader(t *testing.T) {
	serializer := BinarySerializer{}
	const validMagicNumber = MagicNumber

	tests := []struct {
		name          string
		data          []byte
		expected      FileHeader
		expectErr     bool
		expectedError error
	}{
		{
			name: "ValidHeader",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.LittleEndian, validMagicNumber)
				binary.Write(buf, binary.LittleEndian, uint16(1))
				binary.Write(buf, binary.LittleEndian, uint32(128))
				return buf.Bytes()
			}(),
			expected:  FileHeader{Magic: validMagicNumber, Version: 1, MetadataLength: 128},
			expectErr: false,
		},
		{
			name: "InvalidMagicNumber",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.LittleEndian, uint32(0x12345678)) // Invalid Magic
				binary.Write(buf, binary.LittleEndian, uint16(1))
				binary.Write(buf, binary.LittleEndian, uint32(128))
				return buf.Bytes()
			}(),
			expected:      FileHeader{},
			expectErr:     true,
			expectedError: errors.New("invalid magic number"),
		},
		{
			name: "IncompleteHeader",
			data: func() []byte {
				buf := new(bytes.Buffer)
				binary.Write(buf, binary.LittleEndian, validMagicNumber)
				binary.Write(buf, binary.LittleEndian, uint16(1))
				// Missing MetadataLength
				return buf.Bytes()
			}(),
			expected:  FileHeader{},
			expectErr: true,
		},
		{
			name:      "CorruptedData",
			data:      []byte{0x00, 0x01, 0x02}, // Not enough bytes for a valid header
			expected:  FileHeader{},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewReader(tt.data)
			header, err := serializer.DeserializeHeader(buf)

			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error but got nil")
				} else if tt.expectedError != nil && err.Error() != tt.expectedError.Error() {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
				}
				return
			}

			if err != nil {
				t.Errorf("did not expect error, but got: %v", err)
				return
			}

			if header != tt.expected {
				t.Errorf("expected header %+v, got %+v", tt.expected, header)
			}
		})
	}
}
