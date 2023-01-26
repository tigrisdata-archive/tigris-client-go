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

package fields

import (
	"time"

	"github.com/google/uuid"
)

// Package level versions

// SetInt64 instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func SetInt64(field string, value int64) *Update {
	return Set(field, value)
}

// SetInt instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func SetInt(field string, value int) *Update {
	return Set(field, value)
}

// SetInt32 instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func SetInt32(field string, value int32) *Update {
	return Set(field, value)
}

// SetString instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func SetString(field string, value string) *Update {
	return Set(field, value)
}

// SetBytes instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func SetBytes(field string, value []byte) *Update {
	return Set(field, value)
}

// SetFloat32 instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func SetFloat32(field string, value float32) *Update {
	return Set(field, value)
}

// SetFloat64 instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func SetFloat64(field string, value float64) *Update {
	return Set(field, value)
}

// SetTime composes 'equal' operation. from time.Time value.
// Result is equivalent to:
//
//	field = value
func SetTime(field string, value time.Time) *Update {
	return Set(field, value)
}

// SetUUID composes 'equal' operation. from uuid.UUID value.
// Result is equivalent to:
//
//	field = value
func SetUUID(field string, value uuid.UUID) *Update {
	return Set(field, value)
}

// Scoped versions

// SetInt64 instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func (u *Update) SetInt64(field string, value int64) *Update {
	return u.Set(field, value)
}

// SetInt instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func (u *Update) SetInt(field string, value int) *Update {
	return Set(field, value)
}

// SetInt32 instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func (u *Update) SetInt32(field string, value int32) *Update {
	return Set(field, value)
}

// SetString instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func (u *Update) SetString(field string, value string) *Update {
	return Set(field, value)
}

// SetBytes instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func (u *Update) SetBytes(field string, value []byte) *Update {
	return Set(field, value)
}

// SetFloat32 instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func (u *Update) SetFloat32(field string, value float32) *Update {
	return Set(field, value)
}

// SetFloat64 instructs update to set given field to the provided value
// Result is equivalent to:
//
//	field = value
func (u *Update) SetFloat64(field string, value float64) *Update {
	return Set(field, value)
}

// SetTime composes 'equal' operation. from time.Time value.
// Result is equivalent to:
//
//	field = value
func (u *Update) SetTime(field string, value time.Time) *Update {
	return Set(field, value)
}

// SetUUID composes 'equal' operation. from uuid.UUID value.
// Result is equivalent to:
//
//	field = value
func (u *Update) SetUUID(field string, value uuid.UUID) *Update {
	return Set(field, value)
}
