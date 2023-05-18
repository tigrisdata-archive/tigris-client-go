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
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/code"
	"github.com/tigrisdata/tigris-client-go/driver"
)

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

// AsTigrisError needed to convert driver.Error to search.Error
// when called by errors.As(driver.Error, *search.Error)
// see driver.Error.As for more information.
func (e *Error) AsTigrisError(de *driver.Error) bool {
	e.TigrisError = de.TigrisError

	return true
}

func NewError(c code.Code, format string, a ...any) *Error {
	if c == code.OK {
		return nil
	}

	return &Error{TigrisError: api.Errorf(c, format, a...)}
}
