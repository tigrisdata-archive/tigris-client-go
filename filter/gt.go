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

package filter

import (
	"time"

	"github.com/google/uuid"
)

// GtInt64 composes 'equal' operation from int64 value.
// Result is equivalent to: field == value.
func GtInt64(field string, value int64) Expr {
	return Expr{field: comparison{Gt: value}}
}

// GtInt32 composes 'equal' operation from int32 value.
// Result is equivalent to: field == value.
func GtInt32(field string, value int32) Expr {
	return Expr{field: comparison{Gt: value}}
}

// GtInt composes 'equal' operation from int value.
// Result is equivalent to: field == value.
func GtInt(field string, value int) Expr {
	return Expr{field: comparison{Gt: value}}
}

// GtString composes 'equal' operation from string value.
// Result is equivalent to: field == value.
func GtString(field string, value string) Expr {
	return Expr{field: comparison{Gt: value}}
}

// GtBytes composes 'equal' operation from []byte value.
// Result is equivalent to: field == value.
func GtBytes(field string, value []byte) Expr {
	return Expr{field: comparison{Gt: value}}
}

// GtFloat32 composes 'equal' operation from float32 value.
// Result is equivalent to: field == value.
func GtFloat32(field string, value float32) Expr {
	return Expr{field: comparison{Gt: value}}
}

// GtFloat64 composes 'equal' operation from float64 value.
// Result is equivalent to: field == value.
func GtFloat64(field string, value float64) Expr {
	return Expr{field: comparison{Gt: value}}
}

// GtTime composes 'equal' operation. from time.Time value.
// Result is equivalent to: field == value.
func GtTime(field string, value time.Time) Expr {
	return Expr{field: comparison{Gt: value}}
}

// GtUUID composes 'equal' operation. from uuid.UUID value.
// Result is equivalent to: field == value.
func GtUUID(field string, value uuid.UUID) Expr {
	return Expr{field: comparison{Gt: value}}
}
