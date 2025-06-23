package io

import "fmt"

type IncompleteReadError struct {
	exceptedBytes int
	actualBytes   int
}

func NewIncompleteReadError(expectedBytes, actualBytes int) *IncompleteReadError {
	return &IncompleteReadError{exceptedBytes: expectedBytes, actualBytes: actualBytes}
}

func (e *IncompleteReadError) Error() string {
	return fmt.Sprintf("incomplete read: expected to read %d bytes, but %d bytes were read", e.exceptedBytes, e.actualBytes)
}
