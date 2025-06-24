package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/omesh-barhate/ByteForge/internal/platform/parser/encoding"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type ColumnDefinitionMarshaler struct {
	Name      [64]byte
	DataType  byte
	AllowNull bool
}

func NewColumnDefinitionMarshaler(name [64]byte, dataType byte, allowNull bool) *ColumnDefinitionMarshaler {
	return &ColumnDefinitionMarshaler{
		Name:      name,
		DataType:  dataType,
		AllowNull: allowNull,
	}
}

func (c *ColumnDefinitionMarshaler) Size() uint32 {
	return types.LenByte + // type
		types.LenInt32 + // len
		uint32(len(c.Name)) + // value
		types.LenByte + // type
		types.LenInt32 + // len
		uint32(binary.Size(c.DataType)) + // value
		types.LenByte + // type
		types.LenInt32 + // len
		uint32(binary.Size(c.AllowNull)) // value
}

func (c *ColumnDefinitionMarshaler) MarshalBinary() ([]byte, error) {
	buf := bytes.Buffer{}

	// type
	if err := binary.Write(&buf, binary.LittleEndian, types.TypeColumnDefinition); err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: type: %w", err)
	}

	// length of struct
	if err := binary.Write(&buf, binary.LittleEndian, c.Size()); err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: len: %w", err)
	}

	colName := encoding.NewTLVMarshaler(string(c.Name[:]))
	b, err := colName.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: column name: %w", err)
	}
	buf.Write(b)

	dataType := encoding.NewTLVMarshaler(c.DataType)
	b, err = dataType.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: data type: %w", err)
	}
	buf.Write(b)

	allowNull := encoding.NewTLVMarshaler(c.AllowNull)
	b, err = allowNull.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("ColumnDefinitionMarshaler.MarshalBinary: allow null: %w", err)
	}
	buf.Write(b)

	return buf.Bytes(), nil
}

func (c *ColumnDefinitionMarshaler) UnmarshalBinary(data []byte) error {
	var n uint32 = 0

	byteUnmarshaler := encoding.NewValueUnmarshaler[byte]()
	intUnmarshaler := encoding.NewValueUnmarshaler[uint32]()
	strUnmarshaler := encoding.NewValueUnmarshaler[string]()

	// type
	if err := byteUnmarshaler.UnmarshalBinary(data[n : n+1]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: type: %w", err)
	}
	dataType := byteUnmarshaler.Value
	n++

	if dataType != types.TypeColumnDefinition {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: expected type flag %d received %d", types.TypeColumnDefinition, dataType)
	}

	// length of struct
	if err := intUnmarshaler.UnmarshalBinary(data[n : n+4]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: len: %w", err)
	}
	n += 4

	// unmarshal name
	nameTLV := encoding.NewTLVUnmarshaler[string](strUnmarshaler)
	if err := nameTLV.UnmarshalBinary(data[n:]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: name: %w", err)
	}
	name := nameTLV.Value
	n += nameTLV.BytesRead

	// unmarshal type
	typeTLV := encoding.NewTLVUnmarshaler[byte](byteUnmarshaler)
	if err := typeTLV.UnmarshalBinary(data[n:]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: type: %w", err)
	}
	dataTypeVal := typeTLV.Value
	n += typeTLV.BytesRead

	// unmarshal allow null
	allowNullTLV := encoding.NewTLVUnmarshaler[byte](byteUnmarshaler)
	if err := allowNullTLV.UnmarshalBinary(data[n:]); err != nil {
		return fmt.Errorf("ColumnDefinitionMarshaler.UnmarshalBinary: allow null: %w", err)
	}
	allowNull := allowNullTLV.Value
	n += allowNullTLV.BytesRead

	copy(c.Name[:], name)
	c.DataType = dataTypeVal
	c.AllowNull = allowNull != 0
	return nil
}
