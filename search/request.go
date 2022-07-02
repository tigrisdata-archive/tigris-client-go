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
	"github.com/tigrisdata/tigris-client-go/fields"
	"github.com/tigrisdata/tigris-client-go/filter"
)

// Request for search
type Request struct {
	Q            string
	SearchFields []string
	Filter       filter.Filter
	Facet        *FacetQuery
	ReadFields   fields.Read
	Options      *RequestOptions
}

func NewRequestBuilder(q string) RequestBuilder {
	return &requestBuilder{
		q:            q,
		searchFields: make(map[string]bool)}
}

type RequestBuilder interface {
	WithSearchFields(fields ...string) RequestBuilder
	WithFilter(filter.Filter) RequestBuilder
	WithFacet(*FacetQuery) RequestBuilder
	WithReadOptions(fields.Read) RequestBuilder
	WithOptions(*RequestOptions) RequestBuilder
	Build() *Request
}

type requestBuilder struct {
	q            string
	searchFields map[string]bool
	filter       filter.Filter
	facet        *FacetQuery
	readFields   fields.Read
	options      *RequestOptions
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

func (b *requestBuilder) WithFacet(facet *FacetQuery) RequestBuilder {
	b.facet = facet
	return b
}

func (b *requestBuilder) WithReadOptions(read fields.Read) RequestBuilder {
	b.readFields = read
	return b
}

func (b *requestBuilder) WithOptions(options *RequestOptions) RequestBuilder {
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

	// default options
	if b.options == nil {
		b.options = &DefaultRequestOptions
	}

	return &Request{
		Q:            b.q,
		SearchFields: searchFields,
		Filter:       b.filter,
		Facet:        b.facet,
		ReadFields:   b.readFields,
		Options:      b.options,
	}
}

type RequestOptions struct {
	Page    int32
	PerPage int32
}

var DefaultRequestOptions = RequestOptions{Page: 1, PerPage: 20}

type FacetQuery struct {
	FacetFields map[string]FacetQueryOptions
}

// Built marshals the facet query
func (f *FacetQuery) Built() (driver.Facet, error) {
	if f.FacetFields == nil || len(f.FacetFields) == 0 {
		return driver.Facet(`{}`), nil
	}
	m := make(map[string]map[string]int)
	for f, o := range f.FacetFields {
		opt := map[string]int{
			"size": o.Size,
		}
		m[f] = opt
	}
	b, err := json.Marshal(m)
	return b, err
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
