package tigris

import (
	"encoding/json"

	"github.com/tigrisdata/tigris-client-go/driver"
)

// Iterator is used to iterate documents
// returned by streaming APIs
//
// Example:
//   it, err := c.Read(...)
//   // handle err
//   defer it.Close()
//   var doc UserDocType
//   for it.Next(&doc) {
//     //handle document here
//   }
//   err := it.Err()
type Iterator[T interface{}] struct {
	driver.Iterator
	err error
}

// Next populates 'doc' with the next document in the iteration order
// Returns false at the end of the stream or in the case of error
func (it *Iterator[T]) Next(doc *T) bool {
	var b driver.Document

	if it.err != nil {
		return false
	}

	if !it.Iterator.Next(&b) {
		return false
	}

	var v T

	if err := json.Unmarshal(b, &v); err != nil {
		it.err = err
		it.Close()
		return false
	}

	// The copy is need to strip fields filled in doc and empty in v
	*doc = v

	return true
}

// Err returns nil if iteration was successful,
// otherwise return error details
func (it *Iterator[T]) Err() error {
	if it.Iterator.Err() != nil {
		return it.Iterator.Err()
	}
	return it.err
}

// Close closes iterator stream
func (it *Iterator[T]) Close() {
	it.Iterator.Close()
}
