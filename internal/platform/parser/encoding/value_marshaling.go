package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type ValueMarshaler[T any] struct {
	value T
}

func NewValueMarshaler[T any](val T) *ValueMarshaler[T] {
	return &ValueMarshaler[T]{
		value: val,
	}
}

func (m *ValueMarshaler[T]) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	switch v := any(m.value).(type) {
	case string:
		if err := binary.Write(&buf, binary.LittleEndian, []byte(v)); err != nil {
			return nil, fmt.Errorf("ValueMarshaler.MarshalBinary: %w", err)
		}
	default:
		if err := binary.Write(&buf, binary.LittleEndian, m.value); err != nil {
			return nil, fmt.Errorf("ValueMarshaler.MarshalBinary: %w", err)
		}
	}
	return buf.Bytes(), nil
}

type ValueUnmarshaler[T any] struct {
	Value T
}

func NewValueUnmarshaler[T any]() *ValueUnmarshaler[T] {
	return &ValueUnmarshaler[T]{}
}

func (u *ValueUnmarshaler[T]) UnmarshalBinary(data []byte) error {
	var value T
	switch v := any(&value).(type) {
	case *string:
		*v = string(data)
	default:
		if err := binary.Read(bytes.NewReader(data), binary.LittleEndian, &value); err != nil {
			return fmt.Errorf("ValueUnmarshaler.UnmarsharBinary: %w", err)
		}
	}
	u.Value = value
	return nil
}

type ValueUnmarshalerFactory struct{}

func NewValueUnmarshalerFactory() *ValueUnmarshalerFactory {
	return &ValueUnmarshalerFactory{}
}

func (f *ValueUnmarshalerFactory) CreateInt64() *ValueUnmarshaler[int64] {
	return NewValueUnmarshaler[int64]()
}

func (f *ValueUnmarshalerFactory) CreateInt32() *ValueUnmarshaler[int32] {
	return NewValueUnmarshaler[int32]()
}

func (f *ValueUnmarshalerFactory) CreateByte() *ValueUnmarshaler[byte] {
	return NewValueUnmarshaler[byte]()
}

func (f *ValueUnmarshalerFactory) CreateString() *ValueUnmarshaler[string] {
	return NewValueUnmarshaler[string]()
}
