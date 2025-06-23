package io

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type Reader struct {
	f io.ReadSeeker
}

func NewReader(f io.ReadSeeker) (*Reader, error) {
	if f == nil {
		return nil, fmt.Errorf("NewReader: nil pointer given")
	}
	return &Reader{
		f: f,
	}, nil
}

func (r *Reader) ReadUint32() (uint32, error) {
	buf := make([]byte, types.LenInt32)
	n, err := r.Read(buf)
	if err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("Reader.ReadUint32: %w", err)
	}
	if n != types.LenInt32 {
		return 0, NewIncompleteReadError(types.LenInt32, n)
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func (r *Reader) ReadByte() (byte, error) {
	buf := make([]byte, types.LenByte)
	n, err := r.Read(buf)
	if err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("Reader.ReadByte: %w", err)
	}
	if n != types.LenByte {
		return 0, NewIncompleteReadError(types.LenByte, n)
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
	n, err := r.f.Read(b)
	if err != nil {
		if err == io.EOF {
			return 0, err
		}
		return 0, fmt.Errorf("Reader.Read: %w", err)
	}
	return n, nil
}
