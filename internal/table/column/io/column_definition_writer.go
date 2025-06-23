package io

import (
	"fmt"
	"io"
)

type ColumnDefinitionWriter struct {
	w io.Writer
}

func NewColumnDefinitionWriter(w io.Writer) *ColumnDefinitionWriter {
	return &ColumnDefinitionWriter{
		w: w,
	}
}

func (c *ColumnDefinitionWriter) Write(data []byte) (int, error) {
	n, err := c.w.Write(data)
	if err != nil {
		return 0, fmt.Errorf("ColumnDefinitionWriter.Write: file.Write: %w", err)
	}
	if n != len(data) {
		return n, NewIncompleteWriteError(len(data), n)
	}
	return 0, nil
}
