package io

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type Reader struct {
	reader io.Reader
}

func NewReader(r io.Reader) (*Reader, error) {
	if r == nil {
		return nil, fmt.Errorf("NewReader: nil reader given")
	}
	return &Reader{
		reader: r,
	}, nil
}

func (r *Reader) ReadUint32() (uint32, error) {
	buf := make([]byte, types.LenInt32)
	if _, err := r.Read(buf); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func (r *Reader) ReadByte() (byte, error) {
	buf := make([]byte, types.LenByte)
	if _, err := r.Read(buf); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (r *Reader) ReadTLV() ([]byte, error) {
	buf := bytes.Buffer{}

	dataType, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Reader.ReadTLV: dataType: %w", err)
	}
	buf.WriteByte(dataType)

	length, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("Reader.ReadTLV: len: %w", err)
	}
	if err = binary.Write(&buf, binary.LittleEndian, length); err != nil {
		return nil, fmt.Errorf("Reader.ReadTLV: len: %w", err)
	}

	valBuf := make([]byte, length)
	if _, err := r.Read(valBuf); err != nil {
		return nil, fmt.Errorf("Reader.ReadTLV: value: %w", err)
	}
	buf.Write(valBuf)

	return buf.Bytes(), nil
}

func (r *Reader) Read(b []byte) (int, error) {
	if b == nil {
		return 0, fmt.Errorf("Reader.Read: nil buffer given")
	}
	n, err := r.reader.Read(b)
	if err != nil {
		return 0, err
	}
	if n != len(b) {
		return n, fmt.Errorf("Reader.Read: %w", &IncompleteReadError{exceptedBytes: len(b), actualBytes: n})
	}
	return n, nil
}
