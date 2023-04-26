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

package tigris

import (
	"context"
	"encoding/json"
	"fmt"

	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/code"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/fields"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/schema"
	"github.com/tigrisdata/tigris-client-go/search"
	"github.com/tigrisdata/tigris-client-go/sort"
)

var ErrNotFound = NewError(code.NotFound, "document not found")

// Collation allows you to specify string comparison rules. Default is case-sensitive.
type Collation struct {
	// Case is case-sensitive by default, to perform case-insensitive query override it and set
	// this to 'ci'.
	Case string
}

// ReadOptions modifies read request behavior.
type ReadOptions struct {
	// Limit the number of documents returned by the read operation.
	Limit int64
	// Number of documents to skip before starting to return resulting documents.
	Skip int64
	// A cursor for use in pagination. The next streams will return documents after this offset.
	Offset []byte
	// Collation allows you to specify string comparison rules. Default is case-sensitive.
	Collation *driver.Collation
	// Sort order
	Sort sort.Order
}

// Collection provides an interface for documents manipulation.
// Such as Insert, Update, Delete, Read.
type Collection[T schema.Model] struct {
	name string
	db   driver.Database
}

// Drop drops the collection.
func (c *Collection[T]) Drop(ctx context.Context) error {
	return getDB(ctx, c.db).DropCollection(ctx, c.name)
}

// Insert inserts documents into the collection.
// Returns an error if the documents with the same primary key exists already.
func (c *Collection[T]) Insert(ctx context.Context, docs ...*T) (*InsertResponse, error) {
	var err error

	bdocs := make([]driver.Document, len(docs))

	for k, v := range docs {
		if bdocs[k], err = json.Marshal(v); err != nil {
			return nil, err
		}
	}

	md, err := getDB(ctx, c.db).Insert(ctx, c.name, bdocs)
	if err != nil {
		return nil, err
	}

	if md == nil {
		return &InsertResponse{}, nil
	}

	if len(md.Keys) > 0 && len(md.Keys) != len(docs) {
		return nil, fmt.Errorf("broken response. number of inserted documents is not the same as number of provided documents")
	}

	for k, v := range md.Keys {
		if err := populateModelMetadata(docs[k], md.Metadata, v); err != nil {
			return nil, err
		}
	}

	return &InsertResponse{Keys: md.Keys}, nil
}

// InsertOrReplace inserts new documents and in the case of duplicate key
// replaces existing documents with the new document.
func (c *Collection[T]) InsertOrReplace(ctx context.Context, docs ...*T) (*InsertOrReplaceResponse, error) {
	var err error

	bdocs := make([]driver.Document, len(docs))

	for k, v := range docs {
		if bdocs[k], err = json.Marshal(v); err != nil {
			return nil, err
		}
	}

	md, err := getDB(ctx, c.db).Replace(ctx, c.name, bdocs)
	if err != nil {
		return nil, err
	}

	if md == nil {
		return &InsertOrReplaceResponse{}, nil
	}

	if len(md.Keys) > 0 && len(md.Keys) != len(docs) {
		return nil, fmt.Errorf("broken response. number of inserted documents is not the same as number of provided documents")
	}

	for k, v := range md.Keys {
		if err := populateModelMetadata(docs[k], md.Metadata, v); err != nil {
			return nil, err
		}
	}

	return &InsertOrReplaceResponse{Keys: md.Keys}, nil
}

func (c *Collection[T]) updateWithOptions(ctx context.Context, filter filter.Filter, update *fields.Update, options *driver.UpdateOptions) (*UpdateResponse, error) {
	f, err := filter.Build()
	if err != nil {
		return nil, err
	}

	u, err := update.Build()
	if err != nil {
		return nil, err
	}

	if options != nil {
		_, err = getDB(ctx, c.db).Update(ctx, c.name, f, u.Built(), options)
	} else {
		_, err = getDB(ctx, c.db).Update(ctx, c.name, f, u.Built())
	}
	if err != nil {
		return nil, err
	}

	// TODO: forward response
	return &UpdateResponse{}, nil
}

// Update partially updates documents based on the provided filter
// and provided document mutation.
func (c *Collection[T]) Update(ctx context.Context, filter filter.Filter, update *fields.Update) (*UpdateResponse, error) {
	return c.updateWithOptions(ctx, filter, update, nil)
}

// UpdateOne partially updates first document matching the filter.
func (c *Collection[T]) UpdateOne(ctx context.Context, filter filter.Filter, update *fields.Update) (*UpdateResponse, error) {
	return c.updateWithOptions(ctx, filter, update, &driver.UpdateOptions{Limit: 1})
}

func getFields(fields ...*fields.Read) (driver.Projection, error) {
	p := driver.Projection(nil)

	if len(fields) > 0 {
		if len(fields) > 1 {
			return nil, fmt.Errorf("only one fields parameter is allowed")
		}

		f, err := fields[0].Build()
		if err != nil {
			return nil, err
		}

		p = f.Built()
	}

	return p, nil
}

// Read returns documents which satisfies the filter.
// Only field from the give fields are populated in the documents. By default, all fields are populated.
func (c *Collection[T]) Read(ctx context.Context, filter filter.Filter, fields ...*fields.Read) (*Iterator[T], error) {
	p, err := getFields(fields...)
	if err != nil {
		return nil, err
	}

	f, err := filter.Build()
	if err != nil {
		return nil, err
	}

	it, err := getDB(ctx, c.db).Read(ctx, c.name, f, p)
	if err != nil {
		return nil, err
	}

	return &Iterator[T]{Iterator: it}, nil
}

// ReadWithOptions returns specific fields of the documents according to the filter.
// It allows further configure returned documents by providing options:
//
//	Limit - only returned first "Limit" documents from the result
//	Skip - start returning documents after skipping "Skip" documents from the result
func (c *Collection[T]) ReadWithOptions(ctx context.Context, filter filter.Filter, fields *fields.Read, options *ReadOptions) (*Iterator[T], error) {
	p, err := getFields(fields)
	if err != nil {
		return nil, err
	}

	f, err := filter.Build()
	if err != nil {
		return nil, err
	}
	if options == nil {
		return nil, fmt.Errorf("API expecting options but received null")
	}

	var sortOrderbytes []byte = nil
	if options.Sort != nil {
		sortOrder, err := options.Sort.Built()
		if err != nil {
			return nil, err
		}
		sortOrderbytes, err = json.Marshal(sortOrder)
		if err != nil {
			return nil, err
		}
	}

	it, err := getDB(ctx, c.db).Read(ctx, c.name, f, p, &driver.ReadOptions{
		Limit:     options.Limit,
		Skip:      options.Skip,
		Offset:    options.Offset,
		Collation: (*api.Collation)(options.Collation),
		Sort:      sortOrderbytes,
	})
	if err != nil {
		return nil, err
	}

	return &Iterator[T]{Iterator: it}, nil
}

// ReadOne reads one document from the collection satisfying the filter.
func (c *Collection[T]) ReadOne(ctx context.Context, filter filter.Filter, fields ...*fields.Read) (*T, error) {
	var doc T

	it, err := c.Read(ctx, filter, fields...)
	if err != nil {
		return nil, err
	}

	defer it.Close()

	if !it.Next(&doc) {
		if it.Err() != nil {
			return nil, it.Err()
		}

		return nil, ErrNotFound
	}

	return &doc, nil
}

// ReadAll returns Iterator which iterates over all the documents
// in the collection.
func (c *Collection[T]) ReadAll(ctx context.Context, fields ...*fields.Read) (*Iterator[T], error) {
	p, err := getFields(fields...)
	if err != nil {
		return nil, err
	}

	it, err := getDB(ctx, c.db).Read(ctx, c.name, driver.Filter("{}"), p)

	return &Iterator[T]{Iterator: it}, err
}

// Search returns Iterator which iterates over matched documents
// in the collection.
func (c *Collection[T]) Search(ctx context.Context, req *search.Request) (*SearchIterator[T], error) {
	r, err := req.BuildInternal()
	if err != nil || r == nil {
		return nil, err
	}

	it, err := getDB(ctx, c.db).Search(ctx, c.name, r)
	if err != nil {
		return nil, err
	}
	return &SearchIterator[T]{Iterator: it}, err
}

func (c *Collection[T]) deleteWithOptions(ctx context.Context, filter filter.Filter, options *driver.DeleteOptions) (*DeleteResponse, error) {
	f, err := filter.Build()
	if err != nil {
		return nil, err
	}

	if options != nil {
		_, err = getDB(ctx, c.db).Delete(ctx, c.name, f, options)
	} else {
		_, err = getDB(ctx, c.db).Delete(ctx, c.name, f)
	}
	if err != nil {
		return nil, err
	}

	// TODO: forward response
	return &DeleteResponse{}, err
}

// Delete removes documents from the collection according to the filter.
func (c *Collection[T]) Delete(ctx context.Context, filter filter.Filter) (*DeleteResponse, error) {
	return c.deleteWithOptions(ctx, filter, nil)
}

func (c *Collection[T]) DeleteOne(ctx context.Context, filter filter.Filter) (*DeleteResponse, error) {
	return c.deleteWithOptions(ctx, filter, &driver.DeleteOptions{Limit: 1})
}

// DeleteAll removes all the documents from the collection.
func (c *Collection[T]) DeleteAll(ctx context.Context) (*DeleteResponse, error) {
	_, err := getDB(ctx, c.db).Delete(ctx, c.name, driver.Filter("{}"))
	if err != nil {
		return nil, err
	}
	// TODO: forward response
	return &DeleteResponse{}, nil
}

// Count returns documents which satisfies the filter.
// Only field from the give fields are populated in the documents. By default, all fields are populated.
func (c *Collection[T]) Count(ctx context.Context, filter filter.Filter) (int64, error) {
	f, err := filter.Build()
	if err != nil {
		return 0, err
	}

	return getDB(ctx, c.db).Count(ctx, c.name, f)
}

// Explain describes the given query execution plan.
func (c *Collection[T]) Explain(ctx context.Context, filter filter.Filter, fields *fields.Read, options *ReadOptions) (*ExplainResponse, error) {
	p, err := getFields(fields)
	if err != nil {
		return nil, err
	}

	f, err := filter.Build()
	if err != nil {
		return nil, err
	}

	var sortOrderbytes []byte = nil
	if options.Sort != nil {
		sortOrder, err := options.Sort.Built()
		if err != nil {
			return nil, err
		}
		sortOrderbytes, err = json.Marshal(sortOrder)
		if err != nil {
			return nil, err
		}
	}

	return getDB(ctx, c.db).Explain(ctx, c.name, f, p, &driver.ReadOptions{
		Limit:     options.Limit,
		Skip:      options.Skip,
		Offset:    options.Offset,
		Collation: (*api.Collation)(options.Collation),
		Sort:      sortOrderbytes,
	})
}

// Describe returns information about the collection.
func (c *Collection[T]) Describe(ctx context.Context) (*DescribeCollectionResponse, error) {
	return getDB(ctx, c.db).DescribeCollection(ctx, c.name, &driver.DescribeCollectionOptions{})
}
