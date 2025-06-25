package encoding

import (
	stdencoding "encoding"
)

// EmbeddedValueMarshaler describes a type that can be marshaled as a hash map value or list item, hence the name "embedded". Sorry
// Implementing encoding.BinaryMarshaler means the value can marshal itself into a []byte
// Implementing BinaryLengther means it can return the data length (such as an int64 is 8bytes) so a hash map or list can calculate with it when marshaling multiple items, etc
type EmbeddedValueMarshaler interface {
	stdencoding.BinaryMarshaler
	BinaryLengther
}

// EmbeddedValueUnmarshaler describes a type that can be unmarshaled from a hash map value or list item, hence the name "embedded". Sorry
// Implementing encoding.BinaryUnmarshaler means the value can unmarshal itself into from a []byte
// Implementing BinaryLengther means it can return the data length (such as an int64 is 8bytes) so a hash map or list can calculate with it when unmarshaling multiple items, etc
// Implementing ValueHolder means the object can return the unmarshaled data so a hash map or list can work with it when constructing itself
type EmbeddedValueUnmarshaler interface {
	stdencoding.BinaryUnmarshaler
	BinaryLengther
	ValueHolder
}

type BinaryLengther interface {
	BinaryLen() uint32
}

type ValueHolder interface {
	GetValue() interface{}
}
