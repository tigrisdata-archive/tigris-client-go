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
	"testing"

	"github.com/stretchr/testify/assert"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Coll1 struct {
	Key1   string `tigris:"primary_key"`
	Field1 int64
}

func TestResult_From(t *testing.T) {
	t.Run("from nil", func(t *testing.T) {
		r := &Result[Coll1]{}
		err := r.From(nil)
		assert.Nil(t, err)
		assert.NotNil(t, r.Hits)
		assert.Len(t, r.Hits, 0)
		assert.NotNil(t, r.Facets)
		assert.Len(t, r.Facets, 0)
		assert.NotNil(t, r.Meta)
	})

	t.Run("from nil hits", func(t *testing.T) {
		r := &Result[Coll1]{}
		err := r.From(&api.SearchResponse{Hits: nil})
		assert.Nil(t, err)
		assert.NotNil(t, r.Hits)
		assert.Len(t, r.Hits, 0)
		assert.NotNil(t, r.Facets)
		assert.Len(t, r.Facets, 0)
		assert.NotNil(t, r.Meta)
	})

	t.Run("from nil facets", func(t *testing.T) {
		r := &Result[Coll1]{}
		err := r.From(&api.SearchResponse{Facets: nil})
		assert.Nil(t, err)
		assert.NotNil(t, r.Hits)
		assert.Len(t, r.Hits, 0)
		assert.NotNil(t, r.Facets)
		assert.Len(t, r.Facets, 0)
		assert.NotNil(t, r.Meta)
	})

	t.Run("from nil meta", func(t *testing.T) {
		r := &Result[Coll1]{}
		err := r.From(&api.SearchResponse{Meta: nil})
		assert.Nil(t, err)
		assert.NotNil(t, r.Hits)
		assert.Len(t, r.Hits, 0)
		assert.NotNil(t, r.Facets)
		assert.Len(t, r.Facets, 0)
		assert.NotNil(t, r.Meta)
	})

	t.Run("when document unmarshalling fails", func(t *testing.T) {
		r := &Result[Coll1]{}
		err := r.From(&api.SearchResponse{
			Hits: []*api.SearchHit{{Data: []byte(`{"Key1": "aaa", "Field1": "val1"}`)}},
		})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "cannot unmarshal")
	})

	t.Run("from complete response", func(t *testing.T) {
		r, sum := &Result[Coll1]{}, float64(5)
		pbtime := timestamppb.Now()
		err := r.From(&api.SearchResponse{
			Hits: []*api.SearchHit{{
				Data:     []byte(`{"Key1": "aaa", "Field1": 34}`),
				Metadata: &api.SearchHitMeta{UpdatedAt: pbtime},
			}},
			Facets: map[string]*api.SearchFacet{
				"Field2": {
					Counts: []*api.FacetCount{
						{Value: "value2", Count: 6},
					},
					Stats: &api.FacetStats{Count: 1, Sum: &sum},
				},
			},
			Meta: &api.SearchMetadata{
				Found:      12,
				TotalPages: 2,
				Page: &api.Page{
					Current: 1,
					Size:    9,
				},
			},
		})
		assert.Nil(t, err)
		assert.Len(t, r.Hits, 1)
		assert.Equal(t, &Coll1{Key1: "aaa", Field1: int64(34)}, r.Hits[0].Document)
		assert.Nil(t, r.Hits[0].Meta.CreatedAt)
		assert.Equal(t, pbtime.AsTime(), *r.Hits[0].Meta.UpdatedAt)
		assert.Contains(t, r.Facets, "Field2")

		f := r.Facets["Field2"]
		assert.ElementsMatch(t, []FacetCount{{Count: 6, Value: "value2"}}, f.Counts)
		assert.Equal(t, int64(1), f.Stats.Count)
		assert.Equal(t, float64(5), *f.Stats.Sum)
		assert.Nil(t, f.Stats.Avg)

		m := r.Meta
		assert.Equal(t, int64(12), m.Found)
		assert.Equal(t, int32(2), m.TotalPages)
		assert.Equal(t, int32(1), m.Page.Current)
		assert.Equal(t, int32(9), m.Page.Size)
	})
}

func TestHit_From(t *testing.T) {
	t.Run("from nil", func(t *testing.T) {
		h := &Hit[Coll1]{}
		err := h.From(nil)
		assert.Nil(t, err)
		assert.NotNil(t, h)
		assert.Nil(t, h.Document)
		assert.Nil(t, h.Meta)
	})

	t.Run("from default response throws error", func(t *testing.T) {
		h := &Hit[Coll1]{}
		err := h.From(&api.SearchHit{})
		assert.NotNil(t, err)
		assert.Equal(t, "unexpected end of JSON input", err.Error())
	})

	t.Run("from missing collection document", func(t *testing.T) {
		h := &Hit[Coll1]{}
		err := h.From(&api.SearchHit{Metadata: &api.SearchHitMeta{CreatedAt: timestamppb.Now()}})
		assert.NotNil(t, err)
		assert.Equal(t, "unexpected end of JSON input", err.Error())
	})

	t.Run("from document with missing values", func(t *testing.T) {
		h := &Hit[Coll1]{}
		err := h.From(&api.SearchHit{
			Data:     []byte(`{"invalidKey": "aaa", "invalidField": "val1"}`),
			Metadata: &api.SearchHitMeta{UpdatedAt: timestamppb.Now()},
		})
		assert.Nil(t, err)
		assert.NotNil(t, h.Document)
		assert.Equal(t, "", h.Document.Key1)
		assert.Equal(t, int64(0), h.Document.Field1)
		assert.NotNil(t, h.Meta)
		assert.NotNil(t, h.Meta.UpdatedAt)
	})

	t.Run("from invalid collection document throws error", func(t *testing.T) {
		h := &Hit[Coll1]{}
		err := h.From(&api.SearchHit{
			Data:     []byte(`{"Key1": "aaa", "Field1": "val1"}`),
			Metadata: &api.SearchHitMeta{CreatedAt: timestamppb.Now()},
		})
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "cannot unmarshal")
	})

	t.Run("from valid response", func(t *testing.T) {
		h := &Hit[Coll1]{}
		err := h.From(&api.SearchHit{
			Data:     []byte(`{"Key1": "aaa", "Field1": 345}`),
			Metadata: &api.SearchHitMeta{CreatedAt: timestamppb.Now()},
		})
		assert.Nil(t, err)
		assert.NotNil(t, h.Document)
		assert.Equal(t, int64(345), h.Document.Field1)
		assert.Equal(t, "aaa", h.Document.Key1)
		assert.NotNil(t, h.Meta)
		assert.NotNil(t, h.Meta.CreatedAt)
	})
}

func TestHitMeta_From(t *testing.T) {
	t.Run("from nil", func(t *testing.T) {
		hm := &HitMeta{}
		hm.From(nil)
		assert.NotNil(t, hm)
		assert.Nil(t, hm.CreatedAt)
		assert.Nil(t, hm.UpdatedAt)
	})

	t.Run("from default response", func(t *testing.T) {
		hm := &HitMeta{}
		hm.From(&api.SearchHitMeta{})
		assert.NotNil(t, hm)
		assert.Nil(t, hm.CreatedAt)
		assert.Nil(t, hm.UpdatedAt)
	})

	t.Run("from partial response", func(t *testing.T) {
		// Saturday, January 1, 2022 12:00:00 AM
		expected := &timestamppb.Timestamp{
			Seconds: 1640995200,
			Nanos:   542,
		}
		hm := &HitMeta{}
		hm.From(&api.SearchHitMeta{CreatedAt: expected})
		assert.Nil(t, hm.UpdatedAt)
		assert.Equal(t, "2022-01-01 00:00:00.000000542 +0000 UTC", hm.CreatedAt.String())
	})
}

func TestFacetDistribution_From(t *testing.T) {
	t.Run("from nil", func(t *testing.T) {
		fd := &Facet{}
		fd.From(nil)
		assert.NotNil(t, fd)
		assert.NotNil(t, fd.Counts)
		assert.Len(t, fd.Counts, 0)
		assert.NotNil(t, fd.Stats)
	})

	t.Run("from partial response", func(t *testing.T) {
		fd := &Facet{}
		fd.From(&api.SearchFacet{})
		assert.NotNil(t, fd)
		assert.NotNil(t, fd.Counts)
		assert.Len(t, fd.Counts, 0)
		assert.NotNil(t, fd.Stats)
	})

	t.Run("from complete response", func(t *testing.T) {
		fd := &Facet{}
		fd.From(&api.SearchFacet{
			Counts: []*api.FacetCount{
				{
					Count: 6,
					Value: "value_1",
				},
				nil,
				{
					Count: 8,
					Value: "value_2",
				},
			},
			Stats: &api.FacetStats{Count: 12},
		})
		assert.Len(t, fd.Counts, 2)
		assert.ElementsMatch(t, []FacetCount{
			{Count: 8, Value: "value_2"},
			{Count: 6, Value: "value_1"},
		}, fd.Counts)
		assert.Equal(t, int64(12), fd.Stats.Count)
	})
}

func TestFacetCount_From(t *testing.T) {
	t.Run("from nil", func(t *testing.T) {
		fc := &FacetCount{}
		fc.From(nil)
		assert.NotNil(t, fc)
		assert.Equal(t, int64(0), fc.Count)
		assert.Equal(t, "", fc.Value)
	})

	t.Run("from partial response", func(t *testing.T) {
		fc := &FacetCount{}
		fc.From(&api.FacetCount{Count: 20})
		assert.NotNil(t, fc)
		assert.Equal(t, int64(20), fc.Count)
		assert.Equal(t, "", fc.Value)
	})

	t.Run("from complete response", func(t *testing.T) {
		fc := &FacetCount{}
		fc.From(&api.FacetCount{Count: 22, Value: "Some field value"})
		assert.NotNil(t, fc)
		assert.Equal(t, int64(22), fc.Count)
		assert.Equal(t, "Some field value", fc.Value)
	})
}

func TestFacetStats_From(t *testing.T) {
	t.Run("from nil", func(t *testing.T) {
		st := &FacetStats{}
		st.From(nil)
		assert.Nil(t, st.Avg)
		assert.Nil(t, st.Max)
		assert.Nil(t, st.Min)
		assert.Nil(t, st.Sum)
		assert.Equal(t, int64(0), st.Count)
	})

	t.Run("from partial api response", func(t *testing.T) {
		st := &FacetStats{}
		avg, sum := 34.5, float64(45)
		st.From(&api.FacetStats{Avg: &avg, Count: 238, Sum: &sum})
		assert.Equal(t, 34.5, *st.Avg)
		assert.Nil(t, st.Max)
		assert.Nil(t, st.Min)
		assert.Equal(t, float64(45), *st.Sum)
		assert.Equal(t, int64(238), st.Count)
	})
}

func TestMeta_From(t *testing.T) {
	t.Run("from nil", func(t *testing.T) {
		m := &Meta{}
		m.From(nil)
		assert.NotNil(t, m)
		assert.Equal(t, int64(0), m.Found)
		assert.Equal(t, int32(0), m.TotalPages)
		assert.NotNil(t, m.Page)
	})

	t.Run("from partial api response", func(t *testing.T) {
		m := &Meta{}
		m.From(&api.SearchMetadata{Found: 32, TotalPages: 5})
		assert.Equal(t, int64(32), m.Found)
		assert.Equal(t, int32(5), m.TotalPages)
	})
}

func TestPage_From(t *testing.T) {
	t.Run("from nil", func(t *testing.T) {
		p := &Page{}
		p.From(nil)
		assert.NotNil(t, p)
		assert.Equal(t, Page{Current: 0, Size: 0}, *p)
	})

	t.Run("from partial api response", func(t *testing.T) {
		p := &Page{}
		p.From(&api.Page{Current: 3})
		assert.Equal(t, Page{Current: 3, Size: 0}, *p)
	})

	t.Run("from complete api response", func(t *testing.T) {
		p := &Page{}
		p.From(&api.Page{Current: 2, Size: 12})
		assert.Equal(t, Page{Current: 2, Size: 12}, *p)
	})
}
