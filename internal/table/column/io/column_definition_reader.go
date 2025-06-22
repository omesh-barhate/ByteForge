package io

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	parserio "github.com/omesh-barhate/ByteForge/internal/platform/parser/io"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type ColumnDefinitionReader struct {
	reader *parserio.Reader
}

func NewColumnDefinitionReader(reader *parserio.Reader) *ColumnDefinitionReader {
	return &ColumnDefinitionReader{
		reader: reader,
	}
}

func (r *ColumnDefinitionReader) Read(b []byte) (int, error) {
	buf := bytes.Buffer{}
	dataType, err := r.reader.ReadByte()
	if err != nil {
		if err == io.EOF {
			return buf.Len(), io.EOF
		}
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: data type: %w", err)
	}
	if dataType != types.TypeColumnDefinition {
		return buf.Len(), io.EOF
	}
	buf.WriteByte(dataType)

	length, err := r.reader.ReadUint32()
	if err != nil {
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: len: %w", err)
	}
	if err = binary.Write(&buf, binary.LittleEndian, length); err != nil {
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: value: %w", err)
	}

	col := make([]byte, length)
	n, err := r.reader.Read(col)
	if err != nil {
		return n, fmt.Errorf("ColumnDefinitionReader.Read: reading file: %w", err)
	}
	buf.Write(col)
	copy(b, buf.Bytes())
	return buf.Len(), nil
}
