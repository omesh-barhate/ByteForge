package encoding

import (
	"bytes"
	"fmt"

	"github.com/omesh-barhate/ByteForge/internal/platform/parser/encoding"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type (
	LastCommitUnmarshaler struct {
		ID  string
		Len uint32
	}

	LastCommitMarshaler struct {
		ID  string
		Len uint32
	}
)

func NewLastCommitMarshaler(id string, len uint32) *LastCommitMarshaler {
	return &LastCommitMarshaler{
		ID:  id,
		Len: len,
	}
}

func (m *LastCommitMarshaler) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	typeMarshaler := encoding.NewValueMarshaler(types.TypeWALLastIDItem)
	typeBuf, err := typeMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WAL.Commit: %w", err)
	}
	buf.Write(typeBuf)

	// The last commit file contains the length of the last record in the WAL file
	recordLenMarshaler := encoding.NewTLVMarshaler[uint32](m.Len)
	l, err := recordLenMarshaler.TLVLength()
	if err != nil {
		return nil, fmt.Errorf("WAL.Commit: %w", err)
	}

	lenMarshaler := encoding.NewValueMarshaler(uint32(len(m.ID)) + types.LenByte + types.LenInt32 + l)
	lenBuf, err := lenMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WAL.Commit: %w", err)
	}
	buf.Write(lenBuf)

	idMarshaler := encoding.NewTLVMarshaler(m.ID)
	idBuf, err := idMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WAL.Commit: %w", err)
	}
	buf.Write(idBuf)

	recordLenBuf, err := recordLenMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("WAL.Commit: %w", err)
	}
	buf.Write(recordLenBuf)

	return buf.Bytes(), nil
}

func NewLastCommitUnmarshaler() *LastCommitUnmarshaler {
	return &LastCommitUnmarshaler{}
}

func (u *LastCommitUnmarshaler) UnmarshalBinary(data []byte) error {
	var bytesRead uint32 = 0

	byteUnmarshaler := encoding.NewValueUnmarshaler[byte]()
	intUnmarshaler := encoding.NewValueUnmarshaler[uint32]()
	strUnmarshaler := encoding.NewValueUnmarshaler[string]()

	// type
	if err := byteUnmarshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("LastCommitUnmarshaler.UnmarshalBinary: type: %w", err)
	}
	bytesRead += types.LenByte

	// len
	if err := intUnmarshaler.UnmarshalBinary(data[bytesRead:]); err != nil {
		return fmt.Errorf("LastCommitUnmarshaler.UnmarshalBinary: len: %w", err)
	}
	bytesRead += types.LenInt32

	// ID
	idUnmarshaler := encoding.NewTLVUnmarshaler(strUnmarshaler)
	if err := idUnmarshaler.UnmarshalBinary(data[bytesRead:]); err != nil {
		return fmt.Errorf("LastCommitUnmarshaler.UnmarshalBinary: ID: %w", err)
	}
	u.ID = idUnmarshaler.Value
	bytesRead += idUnmarshaler.BytesRead

	intUnmarshaler = encoding.NewValueUnmarshaler[uint32]()
	lenUnmarshaler := encoding.NewTLVUnmarshaler(intUnmarshaler)
	if err := lenUnmarshaler.UnmarshalBinary(data[bytesRead:]); err != nil {
		return fmt.Errorf("LastCommitUnmarshaler.UnmarshalBinary: len of last record: %w", err)
	}
	u.Len = lenUnmarshaler.Value
	bytesRead += lenUnmarshaler.BytesRead

	return nil
}
