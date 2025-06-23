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

type PageNotFoundError struct {
	pages []int64
	pos   int64
}

func NewPageNotFoundError(pages []int64, pos int64) *PageNotFoundError {
	return &PageNotFoundError{pages: pages, pos: pos}
}

func (e *PageNotFoundError) Error() string {
	return fmt.Sprintf("page not found: pages: %v, pos: %d", e.pages, e.pos)
}

type PageNotEmptyError struct {
	pos    int64
	length uint32
}

func NewPageNotEmptyError(pos int64, length uint32) *PageNotEmptyError {
	return &PageNotEmptyError{pos: pos, length: length}
}

func (e *PageNotEmptyError) Error() string {
	return fmt.Sprintf("page is not empty: pos: %d, length: %d", e.pos, e.length)
}
