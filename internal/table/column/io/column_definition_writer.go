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
		return n, fmt.Errorf("ColumneDefinitionWriter.Write: %w", err)
	}
	if n != len(data) {
		return n, fmt.Errorf("ColumnDefinitionWriter.Write:: %w", NewIncompleteWriteError(n, len(data)))
	}
	return n, nil
}
