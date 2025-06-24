package platform

import (
	"errors"
	"fmt"
)

type ItemNotFoundError struct {
	values any
	item   any
}

func NewItemNotFoundError(values, item any) *ItemNotFoundError {
	return &ItemNotFoundError{values: values, item: item}
}

func (e *ItemNotFoundError) Error() string {
	return fmt.Sprintf("item %v not found in %v", e.item, e.values)
}

func (e *ItemNotFoundError) Is(target error) bool {
	var errItemNotFound *ItemNotFoundError
	return errors.As(target, &errItemNotFound)
}
