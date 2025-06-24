package column

import (
	"fmt"

	platformbytes "github.com/omesh-barhate/ByteForge/internal/platform/bytes"
	columnencoding "github.com/omesh-barhate/ByteForge/internal/table/column/encoding"
)

const (
	NameLength byte = 64
)

type (
	Column struct {
		Name     [NameLength]byte
		DataType byte
		Opts     Opts
	}
	Opts struct {
		AllowNull bool
	}
)

func New(name string, dataType byte, opts Opts) (*Column, error) {
	if len(name) > int(NameLength) {
		return nil, NewNameTooLongError(int(NameLength), len(name))
	}
	col := &Column{
		DataType: dataType,
		Opts:     opts,
	}
	copy(col.Name[:], name)
	return col, nil
}

func NewOpts(allowNull bool) Opts {
	return Opts{
		AllowNull: allowNull,
	}
}

func (c *Column) MarshalBinary() ([]byte, error) {
	marshaler := columnencoding.NewColumnDefinitionMarshaler(c.Name, c.DataType, c.Opts.AllowNull)
	return marshaler.MarshalBinary()
}

func (c *Column) UnmarshalBinary(data []byte) error {
	marshaler := columnencoding.NewColumnDefinitionMarshaler(c.Name, c.DataType, c.Opts.AllowNull)
	if err := marshaler.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("Column.UnmarshalBinary: %w", err)
	}
	c.Name = marshaler.Name
	c.DataType = marshaler.DataType
	c.Opts.AllowNull = marshaler.AllowNull
	return nil
}

func (c *Column) NameToStr() string {
	trimmed := platformbytes.TrimZeroBytes(c.Name[:])
	str := ""
	for _, v := range trimmed {
		str += string(v)
	}
	return str
}

func (c *Column) String() string {
	return fmt.Sprintf("name: %s type: %d allow_null: %t\n", c.NameToStr(), c.DataType, c.Opts.AllowNull)
}
