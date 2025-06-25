package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// ValueMarshaler marshals the V part of TLV so a single value such as an int, byte, string, etc
// It is generic because every type has a different length and meaning, for example:
// 97 becomes 97 0 0 0 if T is int32
// 97 becomes 97 0 0 0 0 0 0 0 if T is int64
// "a" becomes 97 if T is string
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
			return nil, fmt.Errorf("ValueMarshaler.MarshalBinary: string: %w", err)
		}
	default:
		if err := binary.Write(&buf, binary.LittleEndian, m.value); err != nil {
			return nil, fmt.Errorf("ValueMarshaler.MarshalBinary: default: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// ValueUnmarshaler unmarshals the V part of TLV so a single value such as an int, byte, string, etc
// It is generic because every type has a different length and meaning, for example:
// 97 0 0 0 becomes 97 if T is int32
// 97 0 0 0 0 0 0 0 becomes 97 if T is int64
// 97 0 0 0 becomes "a" if T is string
type ValueUnmarshaler[T any] struct {
	Value T
}

func NewValueUnmarshaler[T any]() *ValueUnmarshaler[T] {
	return &ValueUnmarshaler[T]{}
}

func (d *ValueUnmarshaler[T]) GetValue() interface{} {
	return d.Value
}

func (d *ValueUnmarshaler[T]) UnmarshalBinary(data []byte) error {
	var value T
	switch v := any(&value).(type) {
	case *string:
		*v = string(data)
	default:
		if err := binary.Read(bytes.NewReader(data), binary.LittleEndian, &value); err != nil {
			if err == io.EOF {
				return err
			}
			return fmt.Errorf("ValueUnmarshaler.UnmarshalBinary: %w", err)
		}
	}
	d.Value = value
	return nil
}
