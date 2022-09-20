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

// Package fields package provides a builder to construct update mutation of the Update API.
//
// Example:
//   update.SetInt("field1", 123).Unset("field2")
package fields

import (
	"encoding/json"
	"fmt"

	"github.com/tigrisdata/tigris-client-go/driver"
)

type Update struct {
	built  driver.Update
	SetF   map[string]interface{} `json:"$set,omitempty"`
	UnsetF map[string]interface{} `json:"$unset,omitempty"`
}

// UpdateBuilder returns and object to construct the update field of Update API
func UpdateBuilder() *Update {
	return &Update{SetF: map[string]interface{}{}, UnsetF: map[string]interface{}{}}
}

func (u *Update) Build() (*Update, error) {
	if len(u.SetF) == 0 && len(u.UnsetF) == 0 {
		return nil, fmt.Errorf("empty update")
	}
	if u.built != nil {
		return u, nil
	}
	var err error
	u.built, err = json.Marshal(u)
	return u, err
}

func (u *Update) Built() driver.Update {
	return u.built
}

// Set instructs operation to set given field to the provided value
// The result is equivalent to
//   field = value
func (u *Update) Set(field string, value interface{}) *Update {
	u.SetF[field] = value
	return u
}

// Unset instructs operation to clear given field in the document
// The result is equivalent to
//   field = null
func (u *Update) Unset(field string) *Update {
	u.UnsetF[field] = nil
	return u
}

// Set instructs operation to set given field to the provided value
// The result is equivalent to
//   field = value
func Set(field string, value interface{}) *Update {
	u := UpdateBuilder()
	u.SetF[field] = value
	return u
}

// Unset instructs operation to clear given field in the document
// The result is equivalent to
//   field = null
func Unset(field string) *Update {
	u := UpdateBuilder()
	u.UnsetF[field] = nil
	return u
}
