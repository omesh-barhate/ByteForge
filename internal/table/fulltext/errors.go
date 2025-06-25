package fulltext

import (
	"errors"
	"fmt"
)

var ErrItemNotFound = errors.New("item not found")

type ColumnNotFoundError struct {
}

func NewColumnNotFoundError() *ColumnNotFoundError {
	return &ColumnNotFoundError{}
}

func (e *ColumnNotFoundError) Error() string {
	return fmt.Sprintf("no full-text indexed column found")
}
