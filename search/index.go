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
	"context"
	"encoding/json"
	"fmt"

	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/schema"
)

// Collation allows you to specify string comparison rules. Default is case-sensitive.
type Collation struct {
	// Case is case-sensitive by default, to perform case-insensitive query override it and set
	// this to 'ci'.
	Case string
}

// Index provides an interface for documents manipulation.
// Such as Insert, Update, Delete, Read.
type Index[T schema.Model] struct {
	Name         string
	SearchClient driver.SearchClient
}

func getSearch(_ context.Context, crud driver.SearchClient) driver.SearchClient {
	return crud
}

func setStatuses(resp []*driver.DocStatus) []DocStatus {
	statuses := make([]DocStatus, 0, len(resp))

	for _, v := range resp {
		status := DocStatus{ID: v.Id}
		if v.Error != nil {
			status.Error = &Error{TigrisError: &api.TigrisError{Code: v.Error.Code, Message: v.Error.Message}}
		}

		statuses = append(statuses, status)
	}

	return statuses
}

const (
	create = iota
	replace
	update
)

func (c *Index[T]) createOrReplace(ctx context.Context, tp int, docs ...*T) (*Response, error) {
	var err error

	bdocs := make([]driver.Document, len(docs))

	for k, v := range docs {
		if bdocs[k], err = json.Marshal(v); err != nil {
			return nil, err
		}
	}

	var md []*driver.DocStatus

	switch tp {
	case replace:
		md, err = getSearch(ctx, c.SearchClient).CreateOrReplace(ctx, c.Name, bdocs)
	case create:
		md, err = getSearch(ctx, c.SearchClient).Create(ctx, c.Name, bdocs)
	case update:
		md, err = getSearch(ctx, c.SearchClient).Update(ctx, c.Name, bdocs)
	}

	if err != nil {
		return nil, err
	}

	return &Response{Statuses: setStatuses(md)}, nil
}

// Create is used for indexing a single or multiple documents. The API expects an array of documents.
// Each document is a JSON object. An "id" is optional and the server can automatically generate it for you in
// case it is missing. In cases when an id is provided in the document and the document already exists then that
// document will not be indexed and in the response there will be an error corresponding to that document id other
// documents will succeed. Returns an array of status indicating the status of each document.
func (c *Index[T]) Create(ctx context.Context, docs ...*T) (*Response, error) {
	return c.createOrReplace(ctx, create, docs...)
}

// CreateOrReplace creates or replaces one or more documents. Each document is a JSON object. A document is replaced
// if it already exists. An "id" is generated automatically in case it is missing in the document. The
// document is created if "id" doesn't exist otherwise it is replaced. Returns an array of status indicating
// the status of each document.
func (c *Index[T]) CreateOrReplace(ctx context.Context, docs ...*T) (*Response, error) {
	return c.createOrReplace(ctx, replace, docs...)
}

// Update updates one or more documents by "id". Each document is required to have the
// "id" field in it. Returns an array of status indicating the status of each document. Each status
// has an error field that is set to null in case document is updated successfully otherwise the error
// field is set with a code and message.
func (c *Index[T]) Update(ctx context.Context, docs ...*T) (*Response, error) {
	return c.createOrReplace(ctx, update, docs...)
}

// Get retrieves one or more documents by id. The response is an array of documents in the same order it is requests.
// A null is returned for the documents that are not found.
func (c *Index[T]) Get(ctx context.Context, ids []string) ([]T, error) {
	resp, err := getSearch(ctx, c.SearchClient).Get(ctx, c.Name, ids)
	if err != nil {
		return nil, err
	}

	docs := make([]T, 0, len(resp))

	for _, v := range resp {
		var doc T

		err = json.Unmarshal(v.Data, &doc)
		if err != nil {
			return nil, err
		}

		populateIndexDocMetadata(&doc, v)

		docs = append(docs, doc)
	}

	return docs, nil
}

func getSearchRequest(req *Request) (*driver.SearchRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("search request cannot be null")
	}

	r := driver.SearchRequest{
		Q:             req.Q,
		SearchFields:  req.SearchFields,
		IncludeFields: req.IncludeFields,
		ExcludeFields: req.ExcludeFields,
	}
	if req.Options != nil {
		r.Page = req.Options.Page
		r.PageSize = req.Options.PageSize
	}

	f, err := req.Filter.Build()
	if err != nil {
		return nil, err
	}

	if f != nil {
		r.Filter = f
	}

	if req.Facet != nil {
		facet, err := req.Facet.Built()
		if err != nil {
			return nil, err
		}

		if facet != nil {
			r.Facet = facet
		}
	}

	if req.Sort != nil {
		sortOrder, err := req.Sort.Built()
		if err != nil {
			return nil, err
		}

		if sortOrder != nil {
			r.Sort = sortOrder
		}
	}

	return &r, nil
}

// Search returns Iterator which iterates over matched documents
// in the index.
func (c *Index[T]) Search(ctx context.Context, req *Request) (*Iterator[T], error) {
	r, err := getSearchRequest(req)
	if err != nil || r == nil {
		return nil, err
	}

	it, err := getSearch(ctx, c.SearchClient).Search(ctx, c.Name, r)
	if err != nil {
		return nil, err
	}

	return &Iterator[T]{Iterator: it}, err
}

// Delete one or more documents by id. Returns an array of status indicating the status of each document. Each status
// has an error field that is set to null in case document is deleted successfully otherwise it will be non-null with
// an error code and message.
func (c *Index[T]) Delete(ctx context.Context, ids []string) (*Response, error) {
	resp, err := getSearch(ctx, c.SearchClient).Delete(ctx, c.Name, ids)
	if err != nil {
		return nil, err
	}

	return &Response{Statuses: setStatuses(resp)}, nil
}

// DeleteByQuery is used to delete documents that match the filter. A filter is required. To delete document by id,
// you can pass the filter as follows ```{"id": "test"}```. Returns a count of number of documents deleted.
func (c *Index[T]) DeleteByQuery(ctx context.Context, filter filter.Filter) (int, error) {
	f, err := filter.Build()
	if err != nil {
		return 0, err
	}

	resp, err := getSearch(ctx, c.SearchClient).DeleteByQuery(ctx, c.Name, f)
	if err != nil {
		return 0, err
	}

	return int(resp), err
}
