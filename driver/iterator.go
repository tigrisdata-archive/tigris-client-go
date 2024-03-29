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

package driver

import (
	"errors"
	"io"
)

type Iterator interface {
	Next(d *Document) bool
	Err() error
	Close()
}

type streamReader interface {
	read() (Document, error)
	close() error
}

type readIterator struct {
	streamReader
	eof bool
	err error
}

func (i *readIterator) Next(d *Document) bool {
	if i.eof {
		return false
	}

	doc, err := i.read()
	if errors.Is(err, io.EOF) {
		i.eof = true
		_ = i.close()

		return false
	}

	if err != nil {
		i.eof = true
		i.err = err
		_ = i.close()
		return false
	}

	*d = doc

	return true
}

func (i *readIterator) Err() error {
	return i.err
}

func (i *readIterator) Close() {
	if i.eof {
		return
	}

	_ = i.close()
	i.eof = true
}

type SearchResultIterator interface {
	Next(r *SearchResponse) bool
	Err() error
	Close()
}

type searchStreamReader interface {
	read() (SearchResponse, error)
	close() error
}

type searchResultIterator struct {
	searchStreamReader
	eof bool
	err error
}

func (i *searchResultIterator) Next(r *SearchResponse) bool {
	if i.eof {
		return false
	}

	resp, err := i.read()
	if errors.Is(err, io.EOF) {
		i.eof = true
		_ = i.close()
		return false
	}

	if err != nil {
		i.eof = true
		i.err = err
		_ = i.close()

		return false
	}

	*r = resp

	return true
}

func (i *searchResultIterator) Err() error {
	return i.err
}

func (i *searchResultIterator) Close() {
	if i.eof {
		return
	}

	_ = i.close()
	i.eof = true
}
