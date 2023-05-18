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
	"encoding/json"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/sort"
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
		assert.Empty(t, req.Sort)
		assert.Empty(t, req.IncludeFields)
		assert.Empty(t, req.ExcludeFields)
		assert.Nil(t, req.Options)
	})

	t.Run("with search fields", func(t *testing.T) {
		req := NewRequestBuilder().WithQuery(inputQ).WithSearchFields("field_1", "field_2").Build()
		assert.Len(t, req.SearchFields, 2)
		assert.Equal(t, map[string]bool{"field_1": true, "field_2": true}, req.SearchFields)
	})

	t.Run("with facet fields", func(t *testing.T) {
		req := NewRequestBuilder().WithFacetFields("field_1", "field_2").Build()
		assert.Len(t, req.Facet.FacetFields, 2)
		for f := range req.Facet.FacetFields {
			assert.Contains(t, []string{"field_1", "field_2"}, f)
		}
	})

	t.Run("with field selection", func(t *testing.T) {
		req := NewRequestBuilder().
			WithIncludeFields("field_1").
			WithExcludeFields("field_2", "field_3").
			Build()
		assert.Len(t, req.IncludeFields, 1)
		assert.Len(t, req.ExcludeFields, 2)
		assert.Contains(t, req.IncludeFields, "field_1")
		assert.Subset(t, []string{"field_3", "field_2"}, req.ExcludeFields)
	})

	t.Run("with sorting", func(t *testing.T) {
		req := NewRequestBuilder().
			WithSorting(sort.Ascending("field_1"), sort.Descending("field_2")).
			Build()
		assert.Len(t, req.Sort, 2)
		b, err := req.Sort.Built()
		assert.Nil(t, err)
		assert.Equal(t, driver.SortOrder{json.RawMessage(`{"field_1":"$asc"}`), json.RawMessage(`{"field_2":"$desc"}`)}, b)
	})

	t.Run("with sort oder", func(t *testing.T) {
		order := sort.NewSortOrder(sort.Ascending("field_1"), sort.Descending("field_2"))
		req := NewRequestBuilder().
			WithSortOrder(order).
			Build()
		assert.Equal(t, order, req.Sort)
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
	t.Run("with vector search", func(t *testing.T) {
		f := NewRequestBuilder().WithVectorSearch("vec_field1", []float64{1.1, 2.2, 3.3}).Build()
		assert.Len(t, f.Vector, 1)
		assert.Equal(t, VectorType{"vec_field1": {1.1, 2.2, 3.3}}, f.Vector)
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

	t.Run("with vector search", func(t *testing.T) {
		f := NewRequestBuilder().WithVectorSearch("vec_field1", []float64{1.1, 2.2, 3.3}).Build()
		r, err := f.BuildInternal()
		require.NoError(t, err)

		assert.Equal(t, &driver.SearchRequest{Vector: driver.Vector(`{"vec_field1":[1.1,2.2,3.3]}`)}, r)
	})
}

func ExampleNewRequestBuilder() {
	req := NewRequestBuilder().WithQuery("my search text").Build()
	_, _ = fmt.Println(req.Q)
	// Output: my search text
}

func ExampleRequestBuilder_WithSearchFields() {
	req := NewRequestBuilder().WithQuery("some text").WithSearchFields("field_1").Build()
	b, err := jsoniter.Marshal(req.SearchFields)
	if err != nil {
		panic(err)
	}
	_, _ = fmt.Println(string(b))
	// Output: {"field_1":true}
}
