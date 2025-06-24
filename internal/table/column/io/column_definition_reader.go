package io

import (
	"bytes"
	"encoding/binary"
	"fmt"
	stdio "io"
	"os"

	"github.com/omesh-barhate/ByteForge/internal/platform/parser/io"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type ColumnDefinitionReader struct {
	file   *os.File
	reader *io.Reader
}

func NewColumnDefinitionReader(file *os.File, reader *io.Reader) *ColumnDefinitionReader {
	return &ColumnDefinitionReader{
		reader: reader,
		file:   file,
	}
}

func (r *ColumnDefinitionReader) Read(b []byte) (int, error) {
	buf := bytes.Buffer{}
	dataType, err := r.reader.ReadByte()
	if err != nil {
		if err == stdio.EOF {
			return buf.Len(), stdio.EOF
		}
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: %w", err)
	}
	if dataType != types.TypeColumnDefinition {
		return buf.Len(), stdio.EOF
	}
	buf.WriteByte(dataType)

	length, err := r.reader.ReadUint32()
	if err != nil {
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: %w", err)
	}
	if err = binary.Write(&buf, binary.LittleEndian, length); err != nil {
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: %w", err)
	}

	col := make([]byte, length)
	n, err := r.file.Read(col)
	if err != nil {
		if err == stdio.EOF {
			return n, err
		}
		return n, fmt.Errorf("ColumnDefinitionReader.Read: %w", err)
	}
	buf.Write(col)
	copy(b, buf.Bytes())
	return buf.Len(), nil
}
