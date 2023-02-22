// Copyright 2022-2023 Tigris Data, Inc.
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

package search

import (
	"github.com/tigrisdata/tigris-client-go/driver"
)

// Iterator is used to iterate search documents.
type Iterator[T interface{}] struct {
	Iterator driver.SearchIndexResultIterator
	err      error
}

func (it *Iterator[T]) Next(res *Result[T]) bool {
	var r driver.SearchIndexResponse

	if it.err != nil {
		return false
	}

	if !it.Iterator.Next(&r) {
		return false
	}

	// catching json marshaling error
	if err := res.FromIndexResponse(r); err != nil {
		it.err = err
		it.Close()
		return false
	}

	return true
}

// Err returns nil if iteration was successful,
// otherwise return error details.
func (it *Iterator[T]) Err() error {
	if it.Iterator.Err() != nil {
		return it.Iterator.Err()
	}
	return it.err
}

// Close closes Iterator stream.
func (it *Iterator[T]) Close() {
	it.Iterator.Close()
}
