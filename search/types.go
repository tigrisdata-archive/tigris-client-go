package search

import "github.com/tigrisdata/tigris-client-go/driver"

type DocStatus struct {
	ID    string `json:"id"`
	Error error
}

type Response struct {
	Statuses []DocStatus
}

// DeleteResponse includes metadata about just deleted documents.
// Returned by Delete-documents collection API.
type DeleteResponse struct{}

type IndexDoc[T any] struct {
	Document T

	Metadata
}

type Error driver.Error

func (e *Error) AsTigrisError(de *driver.Error) bool {
	e.TigrisError = de.TigrisError

	return true
}
