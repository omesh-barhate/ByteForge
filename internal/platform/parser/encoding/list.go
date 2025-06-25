package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type ListMarshaler struct {
	list []EmbeddedValueMarshaler
}

func NewListMarshaler(list []EmbeddedValueMarshaler) *ListMarshaler {
	return &ListMarshaler{
		list: list,
	}
}

func (m *ListMarshaler) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}
	var lenItems uint32
	for _, item := range m.list {
		lenItems += item.BinaryLen() + types.LenMeta
	}

	// type
	if err := binary.Write(&buf, binary.LittleEndian, types.TypeList); err != nil {
		return nil, fmt.Errorf("listMarshaler.MarshalBinary: type: %w", err)
	}
	// len
	if err := binary.Write(&buf, binary.LittleEndian, lenItems); err != nil {
		return nil, fmt.Errorf("listMarshaler.MarshalBinary: len: %w", err)
	}

	for _, item := range m.list {
		itemBytes, err := item.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("listMarshaler.MarshalBinary: items: %w", err)
		}
		buf.Write(itemBytes)
	}
	return buf.Bytes(), nil
}

type ListUnmarshaler struct {
	createItemFn func() EmbeddedValueUnmarshaler
	List         []EmbeddedValueUnmarshaler
}

func NewListUnmarshaler(createItemFn func() EmbeddedValueUnmarshaler) *ListUnmarshaler {
	return &ListUnmarshaler{createItemFn: createItemFn}
}

func (u *ListUnmarshaler) GetValue() interface{} {
	return u.List
}

func (u *ListUnmarshaler) UnmarshalBinary(data []byte) error {
	byteUnmarshaler := NewValueUnmarshaler[byte]()
	int32Unmarshaler := NewValueUnmarshaler[uint32]()

	n := 0
	// type
	if err := byteUnmarshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("listUnmarshaler.UnmarshalBinary: type: %w", err)
	}
	n++
	// len
	if err := int32Unmarshaler.UnmarshalBinary(data[n:]); err != nil {
		return fmt.Errorf("listUnmarshaler.UnmarshalBinary: len: %w", err)
	}
	n += types.LenInt32

	for n <= len(data)-1 {
		item := u.createItemFn()
		if err := item.UnmarshalBinary(data[n:]); err != nil {
			return fmt.Errorf("listUnmarshaler.UnmarshalBinary: item: %w", err)
		}
		n += int(item.BinaryLen()) + int(types.LenMeta)
		u.List = append(u.List, item)
	}
	return nil
}
