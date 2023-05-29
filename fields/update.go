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

// Package fields package provides a builder to construct update mutation of the Update API.
//
// Example:
//
//	update.SetInt("field1", 123).Unset("field2")
package fields

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"

	"github.com/tigrisdata/tigris-client-go/driver"
)

type Update struct {
	built      driver.Update
	SetF       map[string]any     `json:"$set,omitempty"`
	UnsetF     map[string]any     `json:"$unset,omitempty"`
	IncrementF map[string]float64 `json:"$increment,omitempty"`
	DecrementF map[string]float64 `json:"$decrement,omitempty"`
	MultiplyF  map[string]float64 `json:"$multiply,omitempty"`
	DivideF    map[string]float64 `json:"$divide,omitempty"`
}

// UpdateBuilder returns and object to construct the update field of Update API.
func UpdateBuilder() *Update {
	return &Update{
		SetF:       map[string]any{},
		UnsetF:     map[string]any{},
		IncrementF: make(map[string]float64),
		DecrementF: make(map[string]float64),
		MultiplyF:  make(map[string]float64),
		DivideF:    make(map[string]float64),
	}
}

func (u *Update) Build() (*Update, error) {
	if len(u.SetF) == 0 && len(u.UnsetF) == 0 && len(u.IncrementF) == 0 &&
		len(u.DecrementF) == 0 && len(u.MultiplyF) == 0 && len(u.DivideF) == 0 {
		return nil, fmt.Errorf("empty update")
	}

	if u.built != nil {
		return u, nil
	}

	var err error

	u.built, err = jsoniter.Marshal(u)
	return u, err
}

func (u *Update) Built() driver.Update {
	return u.built
}

// Set instructs operation to set given field to the provided value
// The result is equivalent to
//
//	field = value
func (u *Update) Set(field string, value any) *Update {
	u.SetF[field] = value

	return u
}

// Unset instructs operation to clear given field in the document
// The result is equivalent to
//
//	field = null
func (u *Update) Unset(field string) *Update {
	u.UnsetF[field] = nil

	return u
}

// Increment instructs operation to increment given field in the document by the provided value
// The result is equivalent to
//
//	field = field + value
func (u *Update) Increment(field string, value float64) *Update {
	u.IncrementF[field] = value

	return u
}

// Decrement instructs operation to decrement given field in the document by the provided value
// The result is equivalent to
//
//	field = field - value
func (u *Update) Decrement(field string, value float64) *Update {
	u.DecrementF[field] = value

	return u
}

// Multiply instructs operation to multiply given field in the document by the provided value
// The result is equivalent to
//
//	field = field * value
func (u *Update) Multiply(field string, value float64) *Update {
	u.MultiplyF[field] = value

	return u
}

// Divide instructs operation to increment given field in the document by the provided value
// The result is equivalent to
//
//	field = field / value
func (u *Update) Divide(field string, value float64) *Update {
	u.DivideF[field] = value

	return u
}

// Set instructs operation to set given field to the provided value
// The result is equivalent to
//
//	field = value
func Set(field string, value any) *Update {
	u := UpdateBuilder()
	u.SetF[field] = value

	return u
}

// Unset instructs operation to clear given field in the document
// The result is equivalent to
//
//	field = null
func Unset(field string) *Update {
	u := UpdateBuilder()
	u.UnsetF[field] = nil

	return u
}

// Increment instructs operation to increment given field in the document by the provided value
// The result is equivalent to
//
//	field = field + value
func Increment(field string, value float64) *Update {
	u := UpdateBuilder()
	u.IncrementF[field] = value

	return u
}

// Decrement instructs operation to decrement given field in the document by the provided value
// The result is equivalent to
//
//	field = field - value
func Decrement(field string, value float64) *Update {
	u := UpdateBuilder()
	u.DecrementF[field] = value

	return u
}

// Multiply instructs operation to multiply given field in the document by the provided value
// The result is equivalent to
//
//	field = field * value
func Multiply(field string, value float64) *Update {
	u := UpdateBuilder()
	u.MultiplyF[field] = value

	return u
}

// Divide instructs operation to divide given field in the document by the provided value
// The result is equivalent to
//
//	field = field / value
func Divide(field string, value float64) *Update {
	u := UpdateBuilder()
	u.DivideF[field] = value

	return u
}
