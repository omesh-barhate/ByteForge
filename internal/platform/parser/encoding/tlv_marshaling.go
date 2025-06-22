package encoding

import (
	"bytes"
	"fmt"

	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type TLVMarshaler[T any] struct {
	value          T
	valueMarshaler *ValueMarshaler[T]
}

func NewTLVMarshaler[T any](val T) *TLVMarshaler[T] {
	return &TLVMarshaler[T]{
		value:          val,
		valueMarshaler: NewValueMarshaler(val),
	}
}

func (m *TLVMarshaler[T]) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	typeFlag, err := m.typeFlag()
	if err != nil {
		return nil, err
	}
	length, err := m.dataLength()
	if err != nil {
		return nil, err
	}

	byteMarshaler := NewValueMarshaler[byte](typeFlag)
	intMarshaler := NewValueMarshaler[uint32](length)

	// type
	typeBuf, err := byteMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("TLVMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(typeBuf)

	// length
	lenBuf, err := intMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("TLVMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(lenBuf)

	valueBuf, err := m.valueMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("TLVMarshaler.MarshalBinary: %w", err)
	}
	buf.Write(valueBuf)
	return buf.Bytes(), nil
}

func (m *TLVMarshaler[T]) typeFlag() (byte, error) {
	switch v := any(m.value).(type) {
	case byte:
		return types.TypeByte, nil
	case int32, uint32:
		return types.TypeInt32, nil
	case int64:
		return types.TypeInt64, nil
	case bool:
		return types.TypeBool, nil
	case string:
		return types.TypeString, nil
	default:
		return 0, &UnsupportedDataTypeError{dataType: fmt.Sprintf("%T", v)}
	}
}

func (m *TLVMarshaler[T]) dataLength() (uint32, error) {
	switch v := any(m.value).(type) {
	case byte:
		return 1, nil
	case int32, uint32:
		return 4, nil
	case int64:
		return 8, nil
	case bool:
		return 1, nil
	case string:
		return uint32(len(v)), nil
	default:
		return 0, &UnsupportedDataTypeError{dataType: fmt.Sprintf("%T", v)}
	}
}

func (m *TLVMarshaler[T]) TLVLength() (uint32, error) {
	switch v := any(m.value).(type) {
	case byte:
		return types.LenMeta + types.LenByte, nil
	case int32, uint32:
		return types.LenMeta + types.LenInt32, nil
	case int64:
		return types.LenMeta + types.LenInt64, nil
	case bool:
		return types.LenMeta + types.LenByte, nil
	case string:
		return types.LenMeta + uint32(len(v)), nil
	default:
		return 0, &UnsupportedDataTypeError{dataType: fmt.Sprintf("%T", v)}
	}
}

// TLVUnmarshaler unmarshals a piece of data encoded in type-length-value format
// For example, 2 5 0 0 0 104 101 108 108 111 becomes:
//
//	{
//	  dataType: string
//	  length: 5
//	  Value: string("hello")
//	}
type TLVUnmarshaler[T any] struct {
	dataType byte
	length   uint32
	// Value is generic since an TLVUnmarshaler record can store bytes, ints, strings, etc
	Value T
	// Each type has an unmarshaler
	unmarshaler *ValueUnmarshaler[T]
	// BytesRead contains the number of bytes the unmarshal function read from its input
	BytesRead uint32
}

func NewTLVUnmarshaler[T any](unmarshaler *ValueUnmarshaler[T]) *TLVUnmarshaler[T] {
	return &TLVUnmarshaler[T]{
		unmarshaler: unmarshaler,
	}
}

func (u *TLVUnmarshaler[T]) UnmarshalBinary(data []byte) error {
	u.BytesRead = 0

	byteUnmarshaler := NewValueUnmarshaler[byte]()
	intUnmarshaler := NewValueUnmarshaler[uint32]()

	// type
	if err := byteUnmarshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("TLVUnmarshaler.UnmarshalBinary: %w", err)
	}
	u.dataType = byteUnmarshaler.Value
	u.BytesRead += types.LenByte

	// length
	if err := intUnmarshaler.UnmarshalBinary(data[u.BytesRead:]); err != nil {
		return fmt.Errorf("TLVUnmarshaler.UnmarshalBinary: %w", err)
	}
	u.length = intUnmarshaler.Value
	u.BytesRead += types.LenInt32

	// value
	// TODO: WAL előtt nem volt "+ u.length" ez még lehet hogy gondot okoz valahol valamiért
	if err := u.unmarshaler.UnmarshalBinary(data[u.BytesRead:(u.BytesRead + u.length)]); err != nil {
		return fmt.Errorf("TLVUnmarshaler.UnmarshalBinary: %w", err)
	}
	u.Value = u.unmarshaler.Value
	u.BytesRead += u.length

	return nil
}
