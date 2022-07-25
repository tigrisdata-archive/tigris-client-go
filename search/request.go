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

	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/filter"
)

// Request for search
type Request struct {
	// Q is the text search query associated with this request
	Q string
	// Optional SearchFields is an array of fields to project Q against
	// if not specified, query will be projected against all searchable fields
	SearchFields []string
	// Optional Filter is applied on search results to further refine them
	Filter filter.Filter
	// Optional Facet query can be used to request categorical arrangement of the indexed terms
	Facet *FacetQuery
	// Optional IncludeFields sets the document fields to include in search results
	// By default, all documents fields will be included, unless ExcludeFields is specified
	IncludeFields []string
	// Optional ExcludeFields sets the document fields that shouldn't be included in results
	ExcludeFields []string
	// Optional Options provide pagination input
	Options *Options
}

func MatchAll() RequestBuilder {
	return NewRequestBuilder().WithQuery("")
}

func NewRequestBuilder() RequestBuilder {
	return &requestBuilder{
		searchFields: make(map[string]bool)}
}

type RequestBuilder interface {
	WithQuery(q string) RequestBuilder
	WithSearchFields(fields ...string) RequestBuilder
	WithFilter(filter.Filter) RequestBuilder
	WithFacetFields(fields ...string) RequestBuilder
	WithFacet(*FacetQuery) RequestBuilder
	WithIncludeFields(fields ...string) RequestBuilder
	WithExcludeFields(fields ...string) RequestBuilder
	WithOptions(*Options) RequestBuilder
	Build() *Request
}

type requestBuilder struct {
	q            string
	searchFields map[string]bool
	filter       filter.Filter
	facet        *FacetQuery
	include      []string
	exclude      []string
	options      *Options
}

func (b *requestBuilder) WithQuery(q string) RequestBuilder {
	b.q = q
	return b
}

func (b *requestBuilder) WithSearchFields(fields ...string) RequestBuilder {
	for _, f := range fields {
		b.searchFields[f] = true
	}
	return b
}

func (b *requestBuilder) WithFilter(f filter.Filter) RequestBuilder {
	b.filter = f
	return b
}

func (b *requestBuilder) WithFacetFields(fields ...string) RequestBuilder {
	facetQuery := NewFacetQueryBuilder().WithFields(fields...).Build()
	b.facet = facetQuery
	return b
}

func (b *requestBuilder) WithFacet(facet *FacetQuery) RequestBuilder {
	b.facet = facet
	return b
}

func (b *requestBuilder) WithIncludeFields(fields ...string) RequestBuilder {
	b.include = fields
	return b
}

func (b *requestBuilder) WithExcludeFields(fields ...string) RequestBuilder {
	b.exclude = fields
	return b
}

func (b *requestBuilder) WithOptions(options *Options) RequestBuilder {
	b.options = options
	return b
}

func (b *requestBuilder) Build() *Request {
	searchFields := make([]string, len(b.searchFields))
	i := 0
	for f := range b.searchFields {
		searchFields[i] = f
		i++
	}

	return &Request{
		Q:             b.q,
		SearchFields:  searchFields,
		Filter:        b.filter,
		Facet:         b.facet,
		IncludeFields: b.include,
		ExcludeFields: b.exclude,
		Options:       b.options,
	}
}

type Options struct {
	Page     int32
	PageSize int32
}

var DefaultSearchOptions = Options{Page: 1, PageSize: 20}

type FacetQuery struct {
	FacetFields map[string]FacetQueryOptions
	built       driver.Facet
}

var DefaultFacetQuery = FacetQuery{built: nil, FacetFields: map[string]FacetQueryOptions{}}

// Built marshals the facet query
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
	f.built, err = json.Marshal(m)
	return f.built, err
}

type FacetQueryOptions struct{ Size int }

var DefaultFacetQueryOptions = FacetQueryOptions{Size: 10}

func NewFacetQueryBuilder() FacetQueryBuilder {
	return &facetQueryBuilder{
		fields: make(map[string]FacetQueryOptions)}
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
