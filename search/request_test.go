/*
 * Copyright 2022 Tigris Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package search

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tigrisdata/tigris-client-go/driver"
)

func TestMatchAllQuery(t *testing.T) {
	req := MatchAll().Build()
	assert.Equal(t, "", req.Q)
}

func TestRequestBuilder_Build(t *testing.T) {
	inputQ := "search text"

	t.Run("empty build", func(t *testing.T) {
		req := NewRequestBuilder().Build()
		assert.Equal(t, "", req.Q)
		assert.Empty(t, req.SearchFields)
		assert.Empty(t, req.Filter)
		assert.Empty(t, req.Facet)
		assert.Empty(t, req.ReadFields)
		assert.Nil(t, req.Options)
	})

	t.Run("with search fields", func(t *testing.T) {
		req := NewRequestBuilder().WithQuery(inputQ).WithSearchFields("field_1", "field_2").Build()
		assert.Len(t, req.SearchFields, 2)
		assert.Subset(t, []string{"field_1", "field_2"}, req.SearchFields)
	})

	t.Run("with facet fields", func(t *testing.T) {
		req := NewRequestBuilder().WithFacetFields("field_1", "field_2").Build()
		assert.Len(t, req.Facet.FacetFields, 2)
		for f, _ := range req.Facet.FacetFields {
			assert.Contains(t, []string{"field_1", "field_2"}, f)
		}
	})
}

func TestFacetQueryBuilder_Build(t *testing.T) {

	t.Run("empty build", func(t *testing.T) {
		f := NewFacetQueryBuilder().Build()
		assert.Len(t, f.FacetFields, 0)
	})

	t.Run("with fields", func(t *testing.T) {
		f := NewFacetQueryBuilder().WithFields("field_1", "field_2").Build()
		assert.Len(t, f.FacetFields, 2)
		for field, options := range f.FacetFields {
			assert.Contains(t, []string{"field_1", "field_2"}, field)
			assert.Equal(t, 10, options.Size)
		}
	})

	t.Run("with field options", func(t *testing.T) {
		options := FacetQueryOptions{
			Size: 20,
		}
		f := NewFacetQueryBuilder().WithFieldAndOption("field_1", options).Build()
		assert.Len(t, f.FacetFields, 1)
		assert.Equal(t, 20, f.FacetFields["field_1"].Size)
	})

	t.Run("with options map", func(t *testing.T) {
		m := map[string]FacetQueryOptions{
			"field_1": {Size: 5},
			"field_4": {Size: 25},
		}
		f := NewFacetQueryBuilder().WithFieldOptions(m).Build()
		assert.Len(t, f.FacetFields, 2)
		for field, options := range f.FacetFields {
			assert.Contains(t, []string{"field_1", "field_4"}, field)
			assert.Equal(t, m[field].Size, options.Size)
		}
	})
}

func TestFacetQuery_Built(t *testing.T) {

	t.Run("nil object marshal", func(t *testing.T) {
		b, err := (&FacetQuery{}).Built()
		assert.Nil(t, err)
		assert.Nil(t, b)
	})

	t.Run("empty object marshal", func(t *testing.T) {
		b, err := NewFacetQueryBuilder().Build().Built()
		assert.Nil(t, err)
		assert.Nil(t, b)
	})

	t.Run("typed object marshal", func(t *testing.T) {
		f := NewFacetQueryBuilder().WithFields("field_1", "field_2").Build()
		b, err := f.Built()
		assert.Nil(t, err)
		assert.Equal(t, "{\"field_1\":{\"size\":10},\"field_2\":{\"size\":10}}", string(b))
	})
}

func TestReadFieldsBuilder_Build(t *testing.T) {

	t.Run("empty build", func(t *testing.T) {
		rf := NewReadFieldsBuilder().Build()
		assert.Len(t, rf.Fields, 0)
	})

	t.Run("with included fields", func(t *testing.T) {
		incF := []string{"field_1", "Field2", "field_3"}
		excF := []string{"field_4", "field_1"} // override
		rf := NewReadFieldsBuilder().Include(incF...).Exclude(excF...).Build()
		assert.Len(t, rf.Fields, 4)
		for _, f := range []string{"Field2", "field_3"} {
			assert.Contains(t, rf.Fields, f)
			assert.True(t, rf.Fields[f])
		}
		for _, f := range []string{"field_1", "field_4"} {
			assert.Contains(t, rf.Fields, f)
			assert.False(t, rf.Fields[f])
		}
	})
}

func TestReadFields_Built(t *testing.T) {

	t.Run("nil object marshal", func(t *testing.T) {
		b, err := (&ReadFields{}).Built()
		assert.Nil(t, err)
		assert.Equal(t, driver.SearchProjection(`{}`), b)
	})

	t.Run("empty object marshal", func(t *testing.T) {
		b, err := NewReadFieldsBuilder().Build().Built()
		assert.Nil(t, err)
		assert.Equal(t, driver.SearchProjection(`{}`), b)
	})

	t.Run("typed object marshal", func(t *testing.T) {
		incF := []string{"field_1", "Field2", "field_3"}
		excF := []string{"field_4", "field_1"} // override
		rf, err := NewReadFieldsBuilder().Include(incF...).Exclude(excF...).Build().Built()
		assert.Nil(t, err)
		var rawMap map[string]bool
		err = json.Unmarshal(rf, &rawMap)
		assert.Nil(t, err)

		for _, f := range []string{"Field2", "field_3"} {
			assert.Contains(t, rawMap, f)
			assert.True(t, rawMap[f])
		}
		for _, f := range []string{"field_1", "field_4"} {
			assert.Contains(t, rawMap, f)
			assert.False(t, rawMap[f])
		}
	})
}

func ExampleNewRequestBuilder() {
	req := NewRequestBuilder().WithQuery("my search text").Build()
	fmt.Println(req.Q)
	// Output: my search text
}

func ExampleRequestBuilder_WithSearchFields() {
	req := NewRequestBuilder().WithQuery("some text").WithSearchFields("field_1").Build()
	b, _ := json.Marshal(req.SearchFields)
	fmt.Println(string(b))
	// Output: ["field_1"]
}
