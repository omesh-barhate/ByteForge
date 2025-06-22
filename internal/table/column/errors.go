package column

import "fmt"

type NameTooLongError struct {
	maxLength    int
	actualLength int
}

func NewNameTooLongError(maxLength, actualLength int) *NameTooLongError {
	return &NameTooLongError{maxLength: maxLength, actualLength: actualLength}
}

func (e *NameTooLongError) Error() string {
	return fmt.Sprintf("column name cannot be larger than %d characters. %d given", e.maxLength, e.actualLength)
}

type MismatchingColumnsError struct {
	expected int
	actual   int
}

func NewMismatchingColumnsError(expected, actual int) *MismatchingColumnsError {
	return &MismatchingColumnsError{expected: expected, actual: actual}
}

func (e *MismatchingColumnsError) Error() string {
	return fmt.Sprintf("column number mismatch: expected: %d, actual: %d", e.expected, e.actual)
}

type UnknownColumnError struct {
	table  string
	column string
}

func NewUnknownColumnError(table, column string) *UnknownColumnError {
	return &UnknownColumnError{table: table, column: column}
}

func (e *UnknownColumnError) Error() string {
	return fmt.Sprintf("unknown column %s in table %s", e.column, e.table)
}

type CannotBeNullError struct {
	column string
}

func NewCannotBeNullError(column string) *CannotBeNullError {
	return &CannotBeNullError{column: column}
}

func (e *CannotBeNullError) Error() string {
	return fmt.Sprintf("column %s cannot be null", e.column)
}
