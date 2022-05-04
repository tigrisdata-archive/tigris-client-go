package tigris

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/projection"
	"github.com/tigrisdata/tigris-client-go/schema"
	"github.com/tigrisdata/tigris-client-go/update"
)

var (
	errNotFound = fmt.Errorf("document not found")
)

type Collection[T schema.Model] struct {
	name   string
	driver driver.Driver
	schema *schema.Schema
	model  interface{}
	crud   driver.Database
}

// Drop drops the collection
func (c *Collection[T]) Drop(ctx context.Context) error {
	return c.crud.DropCollection(ctx, c.name)
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

	_, err = c.crud.Insert(ctx, c.name, bdocs)
	if err != nil {
		return nil, err
	}

	// TODO: forward response
	return &InsertResponse{}, nil
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

	_, err = c.crud.Replace(ctx, c.name, bdocs)
	if err != nil {
		return nil, err
	}

	// TODO: forward response
	return &InsertOrReplaceResponse{}, nil
}

// Update partially updates documents based on the provided filter
// and provided document mutation.
func (c *Collection[T]) Update(ctx context.Context, filter filter.Filter, update *update.Update) (*UpdateResponse, error) {
	f, err := filter.Build()
	if err != nil {
		return nil, err
	}
	u, err := update.Build()
	if err != nil {
		return nil, err
	}
	_, err = c.crud.Update(ctx, c.name, f, u)
	if err != nil {
		return nil, err
	}

	// TODO: forward response
	return &UpdateResponse{}, nil
}

func getProjection(projection ...projection.Projection) (driver.Projection, error) {
	var err error
	p := driver.Projection(nil)
	if len(projection) > 0 {
		if len(projection) > 1 {
			return nil, fmt.Errorf("only one projection parameter is allowed")
		}
		p, err = projection[0].Build()
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

// Read returns documents which satisfies the filter.
// Only field from the give projection are populated in the documents. By default, all fields are populated.
func (c *Collection[T]) Read(ctx context.Context, filter filter.Filter, projection ...projection.Projection) (*Iterator[T], error) {
	p, err := getProjection(projection...)
	if err != nil {
		return nil, err
	}
	f, err := filter.Build()
	if err != nil {
		return nil, err
	}

	it, err := c.crud.Read(ctx, c.name, f, p)
	if err != nil {
		return nil, err
	}

	return &Iterator[T]{Iterator: it}, nil
}

// ReadOne reads one document from the collection satisfying the filter.
func (c *Collection[T]) ReadOne(ctx context.Context, filter filter.Filter, projection ...projection.Projection) (*T, error) {
	var doc T
	it, err := c.Read(ctx, filter, projection...)
	if err != nil {
		return nil, err
	}
	if !it.Next(&doc) {
		if it.Err() != nil {
			return nil, it.Err()
		}

		return nil, errNotFound
	}
	return &doc, nil
}

// ReadAll returns iterator which iterates over all the documents
// in the collection.
func (c *Collection[T]) ReadAll(ctx context.Context, projection ...projection.Projection) (*Iterator[T], error) {
	p, err := getProjection(projection...)
	if err != nil {
		return nil, err
	}
	it, err := c.crud.Read(ctx, c.name, filter.All, p)
	return &Iterator[T]{Iterator: it}, err
}

// Delete removes documents from the collection according to the filter.
func (c *Collection[T]) Delete(ctx context.Context, filter filter.Filter) (*DeleteResponse, error) {
	f, err := filter.Build()
	if err != nil {
		return nil, err
	}
	_, err = c.crud.Delete(ctx, c.name, f)
	if err != nil {
		return nil, err
	}
	// TODO: forward response
	return &DeleteResponse{}, err
}

// DeleteAll removes all the documents from the collection.
func (c *Collection[T]) DeleteAll(ctx context.Context) (*DeleteResponse, error) {
	_, err := c.crud.Delete(ctx, c.name, filter.All)
	if err != nil {
		return nil, err
	}
	// TODO: forward response
	return &DeleteResponse{}, nil
}
