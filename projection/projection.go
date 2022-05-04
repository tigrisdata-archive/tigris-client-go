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

// Package projection provides helpers to construct "field projections".
// In other words subset of fields of a document.
// This is used by read API to fetch partial documents.
package projection

import (
	"encoding/json"

	"github.com/tigrisdata/tigris-client-go/driver"
)

var (
	//All represents projection which includes all field of the documents
	All = (Projection)(nil)
)

type Projection map[string]bool

func Builder() Projection {
	return Projection{}
}

// Include flags the field to be returned by read
func (pr Projection) Include(field string) Projection {
	pr[field] = true
	return pr
}

// Exclude flags the fields to be excluded by the read
func (pr Projection) Exclude(field string) Projection {
	pr[field] = false
	return pr
}

// Build materializes the projection, built by Include/Exclude,
// to be used by driver API.
func (pr Projection) Build() (driver.Projection, error) {
	if pr == nil || len(pr) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(pr)
	return b, err
}

// Include flags the field to be returned by read
func Include(field string) Projection {
	pr := Projection{}
	pr[field] = true
	return pr
}

// Exclude flags the fields to be excluded by the read
func Exclude(field string) Projection {
	pr := Projection{}
	pr[field] = false
	return pr
}
