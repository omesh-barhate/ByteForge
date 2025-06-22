package parser

import (
	"fmt"

	"github.com/omesh-barhate/ByteForge/internal/platform/parser/encoding"
	"github.com/omesh-barhate/ByteForge/internal/platform/parser/io"
	"github.com/omesh-barhate/ByteForge/internal/platform/types"
)

type TLVParser struct {
	reader *io.Reader
}

func NewTLVParser(reader *io.Reader) *TLVParser {
	return &TLVParser{
		reader: reader,
	}
}

func (p *TLVParser) Parse() (interface{}, error) {
	data, err := p.reader.ReadTLV()
	if err != nil {
		return fmt.Errorf("TLVParser.Parse: %w", err), nil
	}

	switch data[0] {
	case types.TypeInt64:
		return unmarshalValue[int64](data)
	case types.TypeInt32:
		return unmarshalValue[int32](data)
	case types.TypeByte:
		return unmarshalValue[byte](data)
	case types.TypeBool:
		return unmarshalValue[bool](data)
	case types.TypeString:
		return unmarshalValue[string](data)
	}
	return nil, fmt.Errorf("TLVParser.Parse: unknown type: %d", data[0])
}

func unmarshalValue[T any](data []byte) (interface{}, error) {
	tlvUnmarshaler := encoding.NewTLVUnmarshaler(encoding.NewValueUnmarshaler[T]())
	if err := tlvUnmarshaler.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("parser.unmarshalValue: %w", err)
	}
	return tlvUnmarshaler.Value, nil
}
