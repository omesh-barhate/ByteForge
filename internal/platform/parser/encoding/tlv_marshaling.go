package encoding

import (
	"bytes"
	"encoding/binary"
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

func (m *TLVMarshaler[T]) BinaryLen() uint32 {
	return uint32(binary.Size(m.value))
}

func (m *TLVMarshaler[T]) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	typeFlag, err := m.typeFlag()
	if err != nil {
		return nil, fmt.Errorf("TLVMarshaler.MarshalBinary: %w", err)
	}
	length, err := m.dataLength()
	if err != nil {
		return nil, fmt.Errorf("TLVMarshaler.MarshalBinary: %w", err)
	}

	// type
	if err := binary.Write(&buf, binary.LittleEndian, typeFlag); err != nil {
		return nil, fmt.Errorf("TLVMarshaler.MarshalBinary: type: %w", err)
	}

	// length
	if err := binary.Write(&buf, binary.LittleEndian, length); err != nil {
		return nil, fmt.Errorf("TLVMarshaler.MarshalBinary: len: %w", err)
	}

	valueBuf, err := m.valueMarshaler.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("TLVMarshaler.MarshalBinary: value: %w", err)
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
		return 0, NewUnsupportedDataTypeError(fmt.Sprintf("%T", v))
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
		return 0, NewUnsupportedDataTypeError(fmt.Sprintf("%T", v))
	}
}

func (m *TLVMarshaler[T]) TLVLength() (uint32, error) {
	switch v := any(m.value).(type) {
	case byte:
		return 1 + 4 + 1, nil
	case int32, uint32:
		return 1 + 4 + 4, nil
	case int64:
		return 1 + 4 + 8, nil
	case bool:
		return 1 + 4 + 1, nil
	case string:
		return 1 + 4 + uint32(len(v)), nil
	default:
		return 0, NewUnsupportedDataTypeError(fmt.Sprintf("%T", v))
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

func (t *TLVUnmarshaler[T]) GetValue() interface{} {
	return t.Value
}

func (t *TLVUnmarshaler[T]) UnmarshalBinary(data []byte) error {
	t.BytesRead = 0

	byteUnmarshaler := NewValueUnmarshaler[byte]()
	intUnmarshaler := NewValueUnmarshaler[uint32]()

	// type
	if err := byteUnmarshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("TLVUnmarshaler.UnmarshalBinary: type: %w", err)
	}
	t.dataType = byteUnmarshaler.Value
	t.BytesRead += types.LenByte

	// length
	if err := intUnmarshaler.UnmarshalBinary(data[1:]); err != nil {
		return fmt.Errorf("TLVUnmarshaler.UnmarshalBinary: len: %w", err)
	}
	t.length = uint32(intUnmarshaler.Value)
	t.BytesRead += types.LenInt32

	// value
	if err := t.unmarshaler.UnmarshalBinary(data[5:]); err != nil {
		return fmt.Errorf("TLVUnmarshaler.UnmarshalBinary: value: %w", err)
	}
	t.Value = t.unmarshaler.Value
	t.BytesRead += t.length

	return nil
}
