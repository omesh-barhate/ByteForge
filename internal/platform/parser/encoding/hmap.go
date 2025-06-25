package encoding

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type HMapMarshaler[T []EmbeddedValueMarshaler | types.Scalar] struct {
	hMap map[string]T
}

func NewHMapMarshaler[T []EmbeddedValueMarshaler | types.Scalar](hMap map[string]T) *HMapMarshaler[T] {
	return &HMapMarshaler[T]{
		hMap: hMap,
	}
}

func (m *HMapMarshaler[T]) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}

	// type
	if err := binary.Write(&buf, binary.LittleEndian, types.TypeHMap); err != nil {
		return nil, fmt.Errorf("hmapMarshaler.MarshalBinary: hmap type: %w", err)
	}

	tmpBuf := bytes.Buffer{}
	for key, value := range m.hMap {
		// type
		if err := binary.Write(&tmpBuf, binary.LittleEndian, types.TypeHMapKey); err != nil {
			return nil, fmt.Errorf("hmapMarshaler.MarshalBinary: hmap key type: %w", err)
		}
		// len
		if err := binary.Write(&tmpBuf, binary.LittleEndian, uint32(len(key))+types.LenMeta); err != nil {
			return nil, fmt.Errorf("hmapMarshaler.MarshalBinary: hmap key len: %w", err)
		}

		// TLV for the key in the hmap
		// A key such as "key" is encoded as 2 3 0 0 0 107 101 121
		keyTLV := NewTLVMarshaler(key)
		keyBuf, err := keyTLV.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("hmapMarshaler.MarshalBinary: key: %w", err)
		}
		tmpBuf.Write(keyBuf)

		// type
		if err := binary.Write(&tmpBuf, binary.LittleEndian, types.TypeHMapVal); err != nil {
			return nil, fmt.Errorf("hmapMarshaler.MarshalBinary: hmap val type: %w", err)
		}

		// TLV for the value under the key
		// A value such as int32 10 encoded as 5 4 0 0 0 10
		// If the value is a list the ListMarshaler struct is used
		valMarshaler := m.createValueMarshaler(value)
		valBytes, err := valMarshaler.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("hmapMarshaler.MarshalBinary: val: %w", err)
		}

		// The value is needed to calculate the length, so we first write the length
		if err := binary.Write(&tmpBuf, binary.LittleEndian, uint32(len(valBytes))); err != nil {
			return nil, fmt.Errorf("hmapMarshaler.MarshalBinary: hmap val len: %w", err)
		}

		tmpBuf.Write(valBytes)
	}

	// len
	if err := binary.Write(&buf, binary.LittleEndian, uint32(tmpBuf.Len())); err != nil {
		return nil, fmt.Errorf("hmapMarshaler.MarshalBinary: hmap len: %w", err)
	}

	buf.Write(tmpBuf.Bytes())
	return buf.Bytes(), nil
}

type HMapUnmarshaler struct {
	createItemFn func() EmbeddedValueUnmarshaler
	Value        map[string]interface{}
}

func NewHMapUnmarshaler(createItmFn func() EmbeddedValueUnmarshaler) *HMapUnmarshaler {
	return &HMapUnmarshaler{
		createItemFn: createItmFn,
		Value:        make(map[string]interface{}),
	}
}

func (u *HMapUnmarshaler) UnmarshalBinary(data []byte) error {
	byteUnmarshaler := NewValueUnmarshaler[byte]()
	int32Unmarshaler := NewValueUnmarshaler[uint32]()
	strUnmarshaler := NewValueUnmarshaler[string]()

	n := 0
	// type
	if err := byteUnmarshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("hmapUnmarshaler.UnmarshalBinary: type: %w", err)
	}
	n++
	// len
	if err := int32Unmarshaler.UnmarshalBinary(data[n:]); err != nil {
		return fmt.Errorf("hmapUnmarshaler.UnmarshalBinary: len: %w", err)
	}
	n += types.LenInt32

	for n <= len(data)-1 {
		// key type
		if err := byteUnmarshaler.UnmarshalBinary(data[n:]); err != nil {
			return fmt.Errorf("hmapUnmarshaler.UnmarshalBinary: key type: %w", err)
		}
		n++
		// key len
		if err := int32Unmarshaler.UnmarshalBinary(data[n:]); err != nil {
			return fmt.Errorf("hmapUnmarshaler.UnmarshalBinary: key len: %w", err)
		}
		n += types.LenInt32

		keyTLV := NewTLVUnmarshaler[string](strUnmarshaler)
		if err := keyTLV.UnmarshalBinary(data[n : n+int(int32Unmarshaler.Value)]); err != nil {
			return fmt.Errorf("hmapUnmarshaler.UnmarshalBinary: ID: %w", err)
		}
		n += int(keyTLV.BytesRead)
		key := keyTLV.Value
		u.Value[key] = nil

		// hMapVal type
		if err := byteUnmarshaler.UnmarshalBinary(data[n:]); err != nil {
			return fmt.Errorf("hmapUnmarshaler.UnmarshalBinary: hmap val type: %w", err)
		}
		n++
		// hMapVal len
		if err := int32Unmarshaler.UnmarshalBinary(data[n:]); err != nil {
			return fmt.Errorf("hmapUnmarshaler.UnmarshalBinary: hmap val len: %w", err)
		}
		n += types.LenInt32

		// val type
		if err := byteUnmarshaler.UnmarshalBinary(data[n:]); err != nil {
			return fmt.Errorf("hmapUnmarshaler.UnmarshalBinary: val type: %w", err)
		}
		n++
		// val len
		if err := int32Unmarshaler.UnmarshalBinary(data[n:]); err != nil {
			return fmt.Errorf("hmapUnmarshaler.UnmarshalBinary: val len: %w", err)
		}
		n += types.LenInt32

		unmarshaler, err := u.createValueUnmarshaler(byteUnmarshaler.Value)
		if err != nil {
			return fmt.Errorf("hmapUnmarshaler.UnmarshalBinary: %w", err)
		}

		// Since we need to determine what value needs to be unmarshaled, for example a list, or a simple int32
		// We need to read the TL bytes beforehand to create a value unmarshaler
		// So now we need to subtract 5 from n because every unmarshaler first read the TL bytes
		start := n - int(types.LenMeta)
		end := n + int(int32Unmarshaler.Value)
		if err = unmarshaler.UnmarshalBinary(data[start:end]); err != nil {
			return fmt.Errorf("hmapUnmarshaler.UnmarshalBinary: item: %w", err)
		}
		// Using the 5 offset again
		n += len(data[start:end]) - int(types.LenMeta)
		val := unmarshaler.GetValue()
		u.Value[key] = val
	}
	return nil
}

func (m *HMapMarshaler[T]) createValueMarshaler(value interface{}) encoding.BinaryMarshaler {
	var marshaler encoding.BinaryMarshaler
	switch v := value.(type) {
	case []EmbeddedValueMarshaler:
		marshaler = NewListMarshaler(v)
	default:
		marshaler = NewTLVMarshaler(v)
	}
	return marshaler
}

func (u *HMapUnmarshaler) createValueUnmarshaler(dataType byte) (interface {
	encoding.BinaryUnmarshaler
	ValueHolder
}, error) {
	switch dataType {
	case types.TypeList:
		return NewListUnmarshaler(u.createItemFn), nil
	case types.TypeInt64:
		return NewTLVUnmarshaler[int64](NewValueUnmarshaler[int64]()), nil
	case types.TypeInt32:
		return NewTLVUnmarshaler[int32](NewValueUnmarshaler[int32]()), nil
	case types.TypeByte:
		return NewTLVUnmarshaler[byte](NewValueUnmarshaler[byte]()), nil
	case types.TypeBool:
		return NewTLVUnmarshaler[bool](NewValueUnmarshaler[bool]()), nil
	case types.TypeString:
		return NewTLVUnmarshaler[string](NewValueUnmarshaler[string]()), nil
	default:
		return nil, fmt.Errorf("hmapUnmarshal.createValueUnmarshaler: %w", NewUnsupportedDataTypeError(strconv.Itoa(int(dataType))))
	}
}
