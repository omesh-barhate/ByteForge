package io

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	parserio "github.com/omesh-barhate/ByteForge/internal/platform/parser/io"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type ColumnDefinitionReader struct {
	file   *os.File
	reader *parserio.Reader
}

func NewColumnDefinitionReader(file *os.File, reader *parserio.Reader) *ColumnDefinitionReader {
	return &ColumnDefinitionReader{
		reader: reader,
		file:   file,
	}
}

func (r *ColumnDefinitionReader) Read(b []byte) (int, error) {
	buf := bytes.Buffer{}
	dataType, err := r.reader.ReadByte()
	if err == io.EOF {
		return buf.Len(), io.EOF
	}
	if err != nil {
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: readByte: %w", err)
	}
	if dataType != types.TypeColumnDefinition {
		return buf.Len(), io.EOF
	}
	buf.WriteByte(dataType)

	length, err := r.reader.ReadUint32()
	if err != nil {
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: readUint32: %w", err)
	}
	if err = binary.Write(&buf, binary.LittleEndian, length); err != nil {
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: binary.Write: %w", err)
	}

	col := make([]byte, length)
	n, err := r.file.Read(col)
	if err != nil {
		if err == io.EOF {
			return n, err
		}
		return 0, fmt.Errorf("ColumnDefinitionReader.Read: file.Read: %w", err)
	}
	buf.Write(col)
	copy(b, buf.Bytes())
	return buf.Len(), nil
}
