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
	"time"

	"github.com/tigrisdata/tigris-client-go/driver"
)

// metadataSetter allows user document model to customize metadata handling.
type metadataSetter interface {
	setCreatedAt(t time.Time)
	setUpdatedAt(t time.Time)
}

type Metadata struct {
	// These fields are automatically populated from
	// request metadata
	createdAt time.Time `json:"-"`
	updatedAt time.Time `json:"-"`
}

func newMetadata(c time.Time, u time.Time) Metadata {
	return Metadata{createdAt: c, updatedAt: u}
}

// GetCreatedAt returns time of document creation.
func (m *Metadata) GetCreatedAt() time.Time {
	return m.createdAt
}

// GetUpdatedAt returns time of last document update.
func (m *Metadata) GetUpdatedAt() time.Time {
	return m.updatedAt
}

func (m *Metadata) setCreatedAt(t time.Time) {
	m.createdAt = t
}

func (m *Metadata) setUpdatedAt(t time.Time) {
	m.updatedAt = t
}

func populateIndexDocMetadata(umodel any, md *driver.IndexDoc) {
	if m, ok := umodel.(metadataSetter); ok {
		m.setCreatedAt(md.Metadata.CreatedAt.AsTime())
		m.setUpdatedAt(md.Metadata.UpdatedAt.AsTime())
	}
}
