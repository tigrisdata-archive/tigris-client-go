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

package sort

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tigrisdata/tigris-client-go/driver"
)

func TestAscending(t *testing.T) {
	s := Ascending("field_1")
	assert.Equal(t, "field_1", s[0].FieldName())
	assert.Equal(t, map[string]string{"field_1": "$asc"}, s[0].ToSortOrder())
}

func TestDescending(t *testing.T) {
	s := Descending("field_1")
	assert.Equal(t, "field_1", s[0].FieldName())
	assert.Equal(t, map[string]string{"field_1": "$desc"}, s[0].ToSortOrder())
}

func TestNewSortOrder(t *testing.T) {
	t.Run("empty build", func(t *testing.T) {
		o := NewSortOrder()
		assert.Empty(t, o)
	})

	t.Run("multiple sort orders", func(t *testing.T) {
		o := NewSortOrder(Descending("field_1"), Ascending("parent.field_2"))
		assert.Len(t, o, 2)
		assert.Equal(t, map[string]string{"field_1": "$desc"}, o[0].ToSortOrder())
		assert.Equal(t, map[string]string{"parent.field_2": "$asc"}, o[1].ToSortOrder())
	})

	t.Run("multiple sort orders", func(t *testing.T) {
		o := Descending("field_1").Ascending("parent.field_2")
		assert.Len(t, o, 2)
		assert.Equal(t, map[string]string{"field_1": "$desc"}, o[0].ToSortOrder())
		assert.Equal(t, map[string]string{"parent.field_2": "$asc"}, o[1].ToSortOrder())
	})
}

func TestExpr_Built(t *testing.T) {
	t.Run("empty build", func(t *testing.T) {
		b, err := Order{}.Built()
		assert.Nil(t, err)
		assert.Nil(t, b)
	})

	t.Run("build with multiple sort orders", func(t *testing.T) {
		b, err := NewSortOrder(Ascending("field_1"), Ascending("field_1"), Descending("field_2")).Built()
		assert.Nil(t, err)
		assert.NotNil(t, b)
		assert.Equal(t, driver.SortOrder([]json.RawMessage{
			json.RawMessage(`{"field_1":"$asc"}`),
			json.RawMessage(`{"field_1":"$asc"}`),
			json.RawMessage(`{"field_2":"$desc"}`),
		}), b)
	})
}
