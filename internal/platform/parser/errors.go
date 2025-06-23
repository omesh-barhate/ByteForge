package parser

import "fmt"

type InvalidIDError struct {
	record *RawRecord
}

func NewInvalidIDError(record *RawRecord) *InvalidIDError {
	return &InvalidIDError{record: record}
}

func (e *InvalidIDError) Error() string {
	return fmt.Sprintf("invalid ID for raw record: %v", e.record)
}
