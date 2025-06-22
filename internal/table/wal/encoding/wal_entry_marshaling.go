package encoding

import (
	"bytes"
	"fmt"

	"github.com/omesh-barhate/ByteForge/internal/platform/parser/encoding"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

const (
	OpInsert = "insert"
)

type WALMarshaler struct {
	ID    string
	Table string
	Op    string
	Data  []byte
}

func NewWALMarshaler(id, op, table string, data []byte) *WALMarshaler {
	return &WALMarshaler{
		ID:    id,
		Table: table,
		Op:    op,
		Data:  data,
	}
}

func (m *WALMarshaler) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}

	typeMarshaler := encoding.NewValueMarshaler(types.TypeWALEntry)
	typeBuf, err := typeMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WALMarshaler.Append: %w", err)
	}
	buf.Write(typeBuf)

	length, err := m.len()
	if err != nil {
		return nil, fmt.Errorf("WALMarshaler.Append: %w", err)
	}

	lenMarshaler := encoding.NewValueMarshaler(length)
	lenBuf, err := lenMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WALMarshaler.Append: %w", err)
	}
	buf.Write(lenBuf)

	idMarshaler := encoding.NewTLVMarshaler(m.ID)
	idBuf, err := idMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WAL.Append: %w", err)
	}
	buf.Write(idBuf)

	opMarshaler := encoding.NewTLVMarshaler(m.Op)
	opBuf, err := opMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WAL.Append: %w", err)
	}
	buf.Write(opBuf)

	tableMarshaler := encoding.NewTLVMarshaler(m.Table)
	tableBuf, err := tableMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WAL.Append: %w", err)
	}
	buf.Write(tableBuf)
	buf.Write(m.Data)

	return buf.Bytes(), nil
}

func (m *WALMarshaler) len() (uint32, error) {
	idMarshaler := encoding.NewTLVMarshaler(m.ID)
	opMarshaler := encoding.NewTLVMarshaler(OpInsert)
	tableMarshaler := encoding.NewTLVMarshaler(m.Table)

	idLen, err := idMarshaler.TLVLength()
	if err != nil {
		return 0, fmt.Errorf("WALMarshaler: %w", err)
	}
	opLen, err := opMarshaler.TLVLength()
	if err != nil {
		return 0, fmt.Errorf("WALMarshaler: %w", err)
	}
	tableLen, err := tableMarshaler.TLVLength()
	if err != nil {
		return 0, fmt.Errorf("WALMarshaler: %w", err)
	}

	return idLen + opLen + tableLen + uint32(len(m.Data)), nil
}
