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

package tigris

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
)

// metadataSetter allows user document model to customize metadata handling.
type metadataSetter interface {
	setCreatedAt(t time.Time)
	setUpdatedAt(t time.Time)
	setDeletedAt(t time.Time)
}

type Metadata struct {
	// These fields are automatically populated from
	// request metadata
	createdAt time.Time
	updatedAt time.Time
	deletedAt time.Time
}

// Model contains document metadata
// Metadata is populated automatically if embedded into user document
//
// Example:
//
//	type User {
//	   tigris.Model
//	   // User fields
//	   Name string
//	   ...
//	}
type Model struct {
	Metadata

	// This field is unused if user defines its own primary key
	// If no primary key is defined in the user model
	// this field is automatically populated with
	// autogenerated unique UUID
	ID uuid.UUID `tigris:"primary_key,autoGenerate"`
}

// GetCreatedAt returns time of document creation.
func (m *Metadata) GetCreatedAt() time.Time {
	return m.createdAt
}

// GetUpdatedAt returns time of last document update.
func (m *Metadata) GetUpdatedAt() time.Time {
	return m.updatedAt
}

// GetDeletedAt returns time of document deletion.
func (m *Metadata) GetDeletedAt() time.Time {
	return m.deletedAt
}

func (m *Metadata) setCreatedAt(t time.Time) {
	m.createdAt = t
}

func (m *Metadata) setUpdatedAt(t time.Time) {
	m.updatedAt = t
}

func (m *Metadata) setDeletedAt(t time.Time) {
	m.deletedAt = t
}

func populateModelMetadata(umodel any, md *api.ResponseMetadata, keys []byte) error {
	if m, ok := umodel.(metadataSetter); ok {
		m.setCreatedAt(md.CreatedAt.AsTime())
		m.setUpdatedAt(md.UpdatedAt.AsTime())
		m.setDeletedAt(md.DeletedAt.AsTime())
	}

	if keys == nil {
		return nil
	}

	return json.Unmarshal(keys, umodel)
}
