package io

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
