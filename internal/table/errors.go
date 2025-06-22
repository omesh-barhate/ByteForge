package table

import "fmt"

type InvalidFilename struct {
	filename string
}

func NewInvalidFilename(filename string) *InvalidFilename {
	return &InvalidFilename{filename: filename}
}

func (e *InvalidFilename) Error() string {
	return fmt.Sprintf("invalid filename: %s", e.filename)
}
