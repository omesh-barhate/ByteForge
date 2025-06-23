package index

import "fmt"

type IncompleteWriteError struct {
	exceptedBytes int
	actualBytes   int
}

func NewIncompleteWriteError(expectedBytes, actualBytes int) *IncompleteWriteError {
	return &IncompleteWriteError{actualBytes: actualBytes, exceptedBytes: expectedBytes}
}

func (e *IncompleteWriteError) Error() string {
	return fmt.Sprintf("incomplete write: expected to write %d bytes, but %d bytes were written", e.exceptedBytes, e.actualBytes)
}

type IncompleteReadError struct {
	exceptedBytes int
	actualBytes   int
}

func NewIncompleteReadError(expectedBytes, actualBytes int) *IncompleteReadError {
	return &IncompleteReadError{actualBytes: actualBytes, exceptedBytes: expectedBytes}
}

func (e *IncompleteReadError) Error() string {
	return fmt.Sprintf("incomplete read: expected to read %d bytes, but %d bytes were read", e.exceptedBytes, e.actualBytes)
}

type ItemNotFoundError struct {
	id int64
}

func NewItemNotFoundError(id int64) *ItemNotFoundError {
	return &ItemNotFoundError{id: id}
}

func (e *ItemNotFoundError) Error() string {
	return fmt.Sprintf("item with ID %d not found in index", e.id)
}
