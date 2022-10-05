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

package search

import (
	"encoding/json"
	"time"

	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/schema"
)

// Result represents response to a search query.
type Result[T schema.Model] struct {
	// Hits is the results of the query as a list
	Hits []Hit[T]
	// Facets contain the facet distribution of any requested faceted fields
	Facets map[string]Facet
	// Meta represents metadata associated with this search result
	Meta Meta
}

func (result *Result[T]) From(apiResponse *api.SearchResponse) error {
	m := &Meta{}
	result.Hits = []Hit[T]{}
	result.Facets = make(map[string]Facet)

	if apiResponse == nil {
		m.From(nil)
	} else {
		for _, h := range apiResponse.Hits {
			if h == nil {
				continue
			}
			hit := &Hit[T]{}
			if err := hit.From(h); err != nil {
				return err
			}
			result.Hits = append(result.Hits, *hit)
		}

		for field, facets := range apiResponse.Facets {
			f := &Facet{}
			f.From(facets)
			result.Facets[field] = *f
		}

		m.From(apiResponse.Meta)
	}

	result.Meta = *m

	return nil
}

// Hit represents a matched document for search query and relevant matching metadata.
type Hit[T schema.Model] struct {
	// Document represents the matched collection document unmarshalled into type T
	Document *T
	// Meta is the relevance metadata for this collection document
	Meta *HitMeta
}

// From constructs a Hit by unmarshalling the response as Collection Type T
// throws error if json unmarshalling fails.
func (h *Hit[T]) From(apiHit *api.SearchHit) error {
	if apiHit != nil {
		var d T
		if err := json.Unmarshal(apiHit.Data, &d); err != nil {
			return err
		}

		h.Document = &d
		h.Meta = &HitMeta{}
		h.Meta.From(apiHit.Metadata)
	}
	return nil
}

// HitMeta represents the metadata associated with a search hit.
type HitMeta struct {
	// CreatedAt is the time at which document was inserted/replaced
	// Measured in nanoseconds since the UTC Unix epoch.
	CreatedAt *time.Time
	// UpdatedAt is the time at which document was inserted/replaced
	// Measured in nanoseconds since the UTC Unix epoch.
	UpdatedAt *time.Time
}

// From constructs HitMeta from Tigris server's response.
func (hm *HitMeta) From(apiHitMeta *api.SearchHitMeta) {
	if apiHitMeta == nil {
		return
	}

	if apiHitMeta.CreatedAt.CheckValid() == nil {
		t := apiHitMeta.CreatedAt.AsTime()
		hm.CreatedAt = &t
	}

	if apiHitMeta.UpdatedAt.CheckValid() == nil {
		t := apiHitMeta.UpdatedAt.AsTime()
		hm.UpdatedAt = &t
	}
}

// Facet represents unique values with counts and aggregated summary of values in a faceted field.
type Facet struct {
	// Counts represent distinct field values and number of times they appear in a faceted field
	Counts []FacetCount
	Stats  FacetStats
}

// From constructs Facet from Tigris server's search response
// sets default values for missing/nil input.
func (f *Facet) From(apiFacet *api.SearchFacet) {
	f.Counts = []FacetCount{}

	st := &FacetStats{}
	if apiFacet == nil {
		st.From(nil)
	} else {
		st.From(apiFacet.Stats)
		if apiFacet.Counts != nil {
			for _, c := range apiFacet.Counts {
				if c == nil {
					continue
				}
				fc := &FacetCount{}
				fc.From(c)
				f.Counts = append(f.Counts, *fc)
			}
		}
	}

	f.Stats = *st
}

// FacetCount represents number/ Count of times a Value appeared in the faceted field.
type FacetCount struct {
	Count int64
	Value string
}

// From constructs FacetCount from Tigris server's search response
// sets default values for missing/nil input.
func (f *FacetCount) From(apiFacetCount *api.FacetCount) {
	if apiFacetCount != nil {
		f.Value = apiFacetCount.Value
		f.Count = apiFacetCount.Count
	}
}

// FacetStats represent statistics for the faceted field.
type FacetStats struct {
	// Average of all values in a field. Only available for numeric fields
	Avg *float64
	// Maximum of all values in a field. Only available for numeric fields
	Max *float64
	// Minimum of all values in a field. Only available for numeric fields
	Min *float64
	// Sum of all values in a field. Only available for numeric fields
	Sum *float64
	// Total number of values in a field
	Count int64
}

// From constructs FacetStats from Tigris server's search response
// sets default values for missing/nil input.
func (f *FacetStats) From(apiFacet *api.FacetStats) {
	if apiFacet != nil {
		f.Avg = apiFacet.Avg
		f.Max = apiFacet.Max
		f.Min = apiFacet.Min
		f.Sum = apiFacet.Sum
		f.Count = apiFacet.Count
	}
}

// Meta represents search response metadata from server.
type Meta struct {
	// Found represents total number of results matching the search query
	Found int64
	// TotalPages represent the total number of pages of search results
	TotalPages int32
	// Page represents pagination metadata for current page
	Page Page
}

// From constructs Meta from Tigris server's search response metadata
// sets default values for missing/nil input.
func (m *Meta) From(apiMeta *api.SearchMetadata) {
	p := &Page{}
	if apiMeta != nil {
		p.From(apiMeta.GetPage())
		m.Found = apiMeta.Found
		m.TotalPages = apiMeta.TotalPages
	}

	m.Page = *p
}

// Page includes pagination metadata for search results.
type Page struct {
	// Current page number for the paginated search results
	Current int32
	// Size represents maximum number of search results displayed per page
	Size int32
}

// From constructs a Page from Tigris server's search response
// sets default values for missing/nil input.
func (p *Page) From(apiPage *api.Page) {
	if apiPage != nil {
		p.Current = apiPage.Current
		p.Size = apiPage.Size
	}
}
