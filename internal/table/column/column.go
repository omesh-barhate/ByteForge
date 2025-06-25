package column

import (
	"fmt"

	platformbytes "github.com/omesh-barhate/ByteForge/internal/platform/bytes"
	columnencoding "github.com/omesh-barhate/ByteForge/internal/table/column/encoding"
)

const (
	NameLength byte = 64
)

type Column struct {
	name     [NameLength]byte
	dataType byte
	Opts     Opts
}

func New(name string, dataType byte, opts Opts) (*Column, error) {
	if len(name) > int(NameLength) {
		return nil, fmt.Errorf("New: %w", NewNameTooLongError(int(NameLength), len(name)))
	}
	col := &Column{
		dataType: dataType,
		Opts:     opts,
	}
	copy(col.name[:], name)
	return col, nil
}

type Opts struct {
	AllowNull   bool
	FullTextIdx bool
}

func NewColumnOpts(allowNull bool, fullTextIdx bool) Opts {
	return Opts{
		AllowNull:   allowNull,
		FullTextIdx: fullTextIdx,
	}
}

func (c *Column) MarshalBinary() ([]byte, error) {
	marshaler := columnencoding.NewColumnDefinitionMarshaler(c.name, c.dataType, c.Opts.AllowNull, c.Opts.FullTextIdx)
	return marshaler.MarshalBinary()
}

func (c *Column) UnmarshalBinary(data []byte) error {
	marshaler := columnencoding.NewColumnDefinitionMarshaler(c.name, c.dataType, c.Opts.AllowNull, c.Opts.FullTextIdx)
	if err := marshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("Column.UnmarshalBinary: %w", err)
	}
	c.name = marshaler.Name
	c.dataType = marshaler.DataType
	c.Opts.AllowNull = marshaler.AllowNull
	c.Opts.FullTextIdx = marshaler.FullTextIdx
	return nil
}

func (c *Column) NameToStr() string {
	trimmed := platformbytes.TrimZeroBytes(c.name[:])
	str := ""
	for _, v := range trimmed {
		str += string(v)
	}
	return str
}

func (c *Column) String() string {
	return fmt.Sprintf("name: %s type: %d allow_null: %t\n", c.NameToStr(), c.dataType, c.Opts.AllowNull)
}
