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

package driver

import "io"

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
	if err == io.EOF {
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

type EventIterator interface {
	Next(d *Event) bool
	Err() error
	Close()
}

type eventStreamReader interface {
	read() (Event, error)
	close() error
}

type eventReadIterator struct {
	eventStreamReader
	eof bool
	err error
}

func (i *eventReadIterator) Next(e *Event) bool {
	if i.eof {
		return false
	}

	event, err := i.read()
	if err == io.EOF {
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

	*e = event
	return true
}

func (i *eventReadIterator) Err() error {
	return i.err
}

func (i *eventReadIterator) Close() {
	if i.eof {
		return
	}
	_ = i.close()
	i.eof = true
}
