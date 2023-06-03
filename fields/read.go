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

// Package fields provides helpers to construct "field projections".
// In other words subset of fields of a document.
// This is used by read API to fetch partial documents.
package fields

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris-client-go/driver"
)

// All represents fields which includes all field of the documents.
var All = &Read{built: driver.Projection(`{}`), fields: map[string]bool{}}

type Read struct {
	built  driver.Projection
	fields map[string]bool
}

func ReadBuilder() *Read {
	return &Read{fields: map[string]bool{}}
}

// Include flags the field to be returned by read.
func (pr *Read) Include(field string) *Read {
	pr.fields[field] = true

	return pr
}

// Exclude flags the fields to be excluded by the read.
func (pr *Read) Exclude(field string) *Read {
	pr.fields[field] = false

	return pr
}

// Build materializes the fields, built by Include/Exclude,
// to be used by driver API.
func (pr *Read) Build() (*Read, error) {
	if pr == nil {
		return All, nil
	}

	if pr.fields == nil || len(pr.fields) == 0 {
		pr.built = All.built

		return pr, nil
	}

	if pr.built != nil {
		return pr, nil
	}

	var err error

	pr.built, err = jsoniter.Marshal(pr.fields)

	return pr, err
}

func (pr *Read) Built() driver.Projection {
	return pr.built
}

// Include flags the field to be returned by read.
func Include(field string) *Read {
	pr := Read{fields: map[string]bool{}}
	pr.fields[field] = true

	return &pr
}

// Exclude flags the fields to be excluded by the read.
func Exclude(field string) *Read {
	pr := Read{fields: map[string]bool{}}
	pr.fields[field] = false

	return &pr
}
