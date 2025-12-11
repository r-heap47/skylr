package errors

import "errors"

var (
	// ErrNotFound - error, which signifies that provided key was not found in the storage
	ErrNotFound = errors.New("not found")
	// ErrNotImplemented - error, which signigies that smth is not implemented
	ErrNotImplemented = errors.New("not implemented")
)
