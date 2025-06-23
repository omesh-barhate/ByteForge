package encoding

import "fmt"

type UnsupportedDataTypeError struct {
	dataType string
}

func NewUnsupportedDataTypeError(dataType string) *UnsupportedDataTypeError {
	return &UnsupportedDataTypeError{dataType: dataType}
}

func (e *UnsupportedDataTypeError) Error() string {
	return fmt.Sprintf("TLV: unsupported data type: %s", e.dataType)
}
