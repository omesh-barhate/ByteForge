package io

import "fmt"

type IncompleteReadError struct {
	exceptedBytes int
	actualBytes   int
}

func (e *IncompleteReadError) Error() string {
	return fmt.Sprintf("incomplete read: expected to read %d bytes, but %d bytes were read", e.exceptedBytes, e.actualBytes)
}
