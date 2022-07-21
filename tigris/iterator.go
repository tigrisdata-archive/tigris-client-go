// Copyright 2022 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tigris

import (
	"encoding/json"

	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/search"
)

// Iterator is used to iterate documents
// returned by streaming APIs
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

// Close closes Iterator stream
func (it *Iterator[T]) Close() {
	it.Iterator.Close()
}

// SearchIterator is used to iterate search documents
type SearchIterator[T interface{}] struct {
	Iterator driver.SearchResultIterator
	err      error
}

func (it *SearchIterator[T]) Next(res *search.Result[T]) bool {
	var r driver.SearchResponse
	if it.err != nil {
		return false
	}

	if !it.Iterator.Next(&r) {
		return false
	}
	// catching json marshaling error
	if err := res.From(r); err != nil {
		it.err = err
		it.Close()
		return false
	}
	return true
}

// Err returns nil if iteration was successful,
// otherwise return error details
func (it *SearchIterator[T]) Err() error {
	if it.Iterator.Err() != nil {
		return it.Iterator.Err()
	}
	return it.err
}

// Close closes Iterator stream
func (it *SearchIterator[T]) Close() {
	it.Iterator.Close()
}
