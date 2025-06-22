package internal

import "fmt"

type DatabaseDoesNotExistError struct {
	name string
}

func NewDatabaseDoesNotExistError(name string) *DatabaseDoesNotExistError {
	return &DatabaseDoesNotExistError{name: name}
}

func (e *DatabaseDoesNotExistError) Error() string {
	return fmt.Sprintf("database does not exist: %s", e.name)
}

type DatabaseAlreadyExistsError struct {
	name string
}

func NewDatabaseAlreadyExistsError(name string) *DatabaseAlreadyExistsError {
	return &DatabaseAlreadyExistsError{name: name}
}

func (e *DatabaseAlreadyExistsError) Error() string {
	return fmt.Sprintf("database already exists: %s", e.name)
}

type TableAlreadyExistsError struct {
	name string
}

func NewTableAlreadyExistsError(name string) *TableAlreadyExistsError {
	return &TableAlreadyExistsError{name: name}
}

func (e *TableAlreadyExistsError) Error() string {
	return fmt.Sprintf("table already exists: %s", e.name)
}

type CannotCreateTableError struct {
	reason error
	name   string
}

func NewCannotCreateTableError(reason error, name string) *CannotCreateTableError {
	return &CannotCreateTableError{reason: reason, name: name}
}

func (e *CannotCreateTableError) Error() string {
	return fmt.Errorf("cannot create table %s: %w", e.name, e.reason).Error()
}
