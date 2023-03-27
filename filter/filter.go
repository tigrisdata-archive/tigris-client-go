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

// Package filter provides methods to build logical filters.
// Filters are used to read or update only documents,
// which satisfies the filter.
package filter

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/schema"
)

type (
	value any
)

type comparison struct {
	Gt  value `json:"$gt,omitempty"`
	Gte value `json:"$gte,omitempty"`
	Lt  value `json:"$lt,omitempty"`
	Lte value `json:"$lte,omitempty"`
	Ne  value `json:"$ne,omitempty"`
	Eq  value `json:"$eq,omitempty"`

	Contains    value `json:"$contains,omitempty"`
	NotContains value `json:"$not_contains,omitempty"`
}

const (
	and = "$and"
	or  = "$or"
	//	not = "$not"
)

// All represents filter which includes all the documents of the collection.
var All = Expr{}

var (
	True  = Expr{}
	False = Expr(nil)
)

type (
	Expr   map[string]any
	Filter = Expr
)

func IsTrue(e Expr) bool {
	return e != nil && len(e) == 0
}

func IsFalse(e Expr) bool {
	return e == nil
}

// And composes 'and' operation.
// Result is equivalent to: (ops[0] && ... && ops[len(ops-1]).
func And(ops ...Expr) Expr {
	return Expr{and: ops}
}

// Or composes 'or' operation.
// Result is equivalent to: (ops[0] || ... || ops[len(ops-1]).
func Or(ops ...Expr) Expr {
	return Expr{or: ops}
}

// Eq composes 'equal' operation.
// Result is equivalent to: field == value.
func Eq[T schema.PrimitiveFieldType](field string, value T) Expr {
	return Expr{field: comparison{Eq: value}}
}

func Eq1(field string, value any) Expr {
	return Expr{field: comparison{Eq: value}}
}

// Ne composes 'not equal' operation.
// Result is equivalent to: field != value
func Ne(field string, value any) Expr {
	return Expr{field: comparison{Ne: value}}
}

// Gt composes 'greater than' operation.
// Result is equivalent to: field > value.
func Gt(field string, value any) Expr {
	return Expr{field: comparison{Gt: value}}
}

// Gte composes 'greater than or equal' operation.
// Result is equivalent to: field >= value.
func Gte(field string, value any) Expr {
	return Expr{field: comparison{Gte: value}}
}

// Lt composes 'less than' operation.
// Result is equivalent to: field < value.
func Lt(field string, value any) Expr {
	return Expr{field: comparison{Lt: value}}
}

// Lte composes 'less than or equal' operation.
// Result is equivalent to: field <= value.
func Lte(field string, value any) Expr {
	return Expr{field: comparison{Lte: value}}
}

// Build materializes the filter.
func (e Expr) Build() (driver.Filter, error) {
	if e == nil {
		return nil, nil
	}

	return jsoniter.Marshal(e)
}
