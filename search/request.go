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
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/sort"
)

// Request for search.
type Request struct {
	// Q is the text search query associated with this request
	Q string
	// Optional SearchFields is an array of fields to project Q against
	// if not specified, query will be projected against all searchable fields
	SearchFields map[string]bool
	// Optional Filter is applied on search results to further refine them
	Filter filter.Filter
	// Optional Facet query can be used to request categorical arrangement of the indexed terms
	Facet *FacetQuery
	// Optional Sort order can be specified to order the search results
	Sort sort.Order
	// Optional IncludeFields sets the document fields to include in search results
	// By default, all documents fields will be included, unless ExcludeFields is specified
	IncludeFields []string
	// Optional ExcludeFields sets the document fields that shouldn't be included in results
	ExcludeFields []string
	// Vector is the map of fields for vector search
	Vector VectorType
	// Optional Options provide pagination input
	Options *Options
}

func MatchAll() RequestBuilder {
	return NewRequestBuilder().WithQuery("")
}

func NewRequestBuilder() RequestBuilder {
	return &Request{
		SearchFields: make(map[string]bool),
	}
}

type RequestBuilder interface {
	WithQuery(q string) RequestBuilder
	WithSearchFields(fields ...string) RequestBuilder
	WithFilter(filter.Filter) RequestBuilder
	WithFacetFields(fields ...string) RequestBuilder
	WithFacet(*FacetQuery) RequestBuilder
	WithSorting(sortByFields ...sort.Order) RequestBuilder
	WithSortOrder(sortOrder sort.Order) RequestBuilder
	WithIncludeFields(fields ...string) RequestBuilder
	WithExcludeFields(fields ...string) RequestBuilder
	WithOptions(*Options) RequestBuilder
	WithVectorSearch(field string, value []float64) RequestBuilder
	Build() *Request
}

type VectorType map[string][]float64

func (r *Request) WithQuery(q string) RequestBuilder {
	r.Q = q

	return r
}

func (r *Request) WithSearchFields(fields ...string) RequestBuilder {
	for _, f := range fields {
		r.SearchFields[f] = true
	}

	return r
}

func (r *Request) WithFilter(f filter.Filter) RequestBuilder {
	r.Filter = f

	return r
}

func (r *Request) WithFacetFields(fields ...string) RequestBuilder {
	facetQuery := NewFacetQueryBuilder().WithFields(fields...).Build()
	r.Facet = facetQuery

	return r
}

func (r *Request) WithFacet(facet *FacetQuery) RequestBuilder {
	r.Facet = facet

	return r
}

func (r *Request) WithSorting(sortByFields ...sort.Order) RequestBuilder {
	r.Sort = sort.NewSortOrder(sortByFields...)

	return r
}

func (r *Request) WithSortOrder(sortOrder sort.Order) RequestBuilder {
	r.Sort = sortOrder

	return r
}

func (r *Request) WithIncludeFields(fields ...string) RequestBuilder {
	r.IncludeFields = fields

	return r
}

func (r *Request) WithExcludeFields(fields ...string) RequestBuilder {
	r.ExcludeFields = fields

	return r
}

func (r *Request) WithOptions(options *Options) RequestBuilder {
	r.Options = options

	return r
}

func (r *Request) WithVectorSearch(field string, value []float64) RequestBuilder {
	if r.Vector == nil {
		r.Vector = make(VectorType)
	}

	r.Vector[field] = value

	return r
}

func (r *Request) Build() *Request {
	return r
}

func (r *Request) BuildInternal() (*driver.SearchRequest, error) {
	req := r
	if req == nil {
		return nil, fmt.Errorf("search request cannot be null")
	}

	dr := driver.SearchRequest{
		Q:             req.Q,
		IncludeFields: req.IncludeFields,
		ExcludeFields: req.ExcludeFields,
	}

	for v := range req.SearchFields {
		dr.SearchFields = append(dr.SearchFields, v)
	}

	if req.Options != nil {
		dr.Page = req.Options.Page
		dr.PageSize = req.Options.PageSize
	}

	f, err := req.Filter.Build()
	if err != nil {
		return nil, err
	}

	if f != nil {
		dr.Filter = f
	}

	if req.Facet != nil {
		facet, err := req.Facet.Built()
		if err != nil {
			return nil, err
		}

		if facet != nil {
			dr.Facet = facet
		}
	}

	if req.Sort != nil {
		sortOrder, err := req.Sort.Built()
		if err != nil {
			return nil, err
		}

		if sortOrder != nil {
			dr.Sort = sortOrder
		}
	}

	if req.Vector != nil {
		b, err := jsoniter.Marshal(req.Vector)
		if err != nil {
			return nil, err
		}

		dr.Vector = b
	}

	return &dr, nil
}

type Options struct {
	Page      int32
	PageSize  int32
	Collation *driver.Collation
}

var DefaultSearchOptions = Options{Page: 1, PageSize: 20}

type FacetQuery struct {
	FacetFields map[string]FacetQueryOptions
	built       driver.Facet
}

var DefaultFacetQuery = FacetQuery{built: nil, FacetFields: map[string]FacetQueryOptions{}}

// Built marshals the facet query.
func (f *FacetQuery) Built() (driver.Facet, error) {
	if f.FacetFields == nil || len(f.FacetFields) == 0 {
		return DefaultFacetQuery.built, nil
	}

	if f.built != nil {
		return f.built, nil
	}

	m := make(map[string]map[string]int)
	for f, o := range f.FacetFields {
		m[f] = map[string]int{"size": o.Size}
	}

	var err error

	f.built, err = jsoniter.Marshal(m)

	return f.built, err
}

type FacetQueryOptions struct{ Size int }

var DefaultFacetQueryOptions = FacetQueryOptions{Size: 10}

func NewFacetQueryBuilder() FacetQueryBuilder {
	return &facetQueryBuilder{
		fields: make(map[string]FacetQueryOptions),
	}
}

type FacetQueryBuilder interface {
	// WithFields would add fields to query with default query options
	WithFields(...string) FacetQueryBuilder
	WithFieldAndOption(string, FacetQueryOptions) FacetQueryBuilder
	WithFieldOptions(map[string]FacetQueryOptions) FacetQueryBuilder
	Build() *FacetQuery
}

type facetQueryBuilder struct {
	fields map[string]FacetQueryOptions
}

func (b *facetQueryBuilder) WithFields(fields ...string) FacetQueryBuilder {
	for _, f := range fields {
		b.fields[f] = DefaultFacetQueryOptions
	}

	return b
}

func (b *facetQueryBuilder) WithFieldAndOption(field string, options FacetQueryOptions) FacetQueryBuilder {
	b.fields[field] = options

	return b
}

func (b *facetQueryBuilder) WithFieldOptions(m map[string]FacetQueryOptions) FacetQueryBuilder {
	for field, options := range m {
		b.fields[field] = options
	}

	return b
}

func (b *facetQueryBuilder) Build() *FacetQuery {
	return &FacetQuery{FacetFields: b.fields}
}
