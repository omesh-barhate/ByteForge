package encoding

import "fmt"

type UnsupportedDataTypeError struct {
	dataType string
}

func (e *UnsupportedDataTypeError) Error() string {
	return fmt.Sprintf("TLV: unsupported data type: %s", e.dataType)
}
