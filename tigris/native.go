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
	"bytes"
	"context"
	"fmt"
	"reflect"
	"runtime"
	"text/template"
	"time"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/schema"
)

type NativeCollection[T schema.Model, P schema.Model] struct {
	proj *Projection[T, P]
}

type NativeFilter struct {
	Raw      string
	Compiled *template.Template
}

type Args struct {
	Time time.Time
	Arg  any
}

var (
	Filters map[string]NativeFilter
	Updates map[string]NativeFilter
)

// interpolate filter or update function arguments
func interpolate(name string, filters map[string]NativeFilter, args any) []byte {
	flt, ok := filters[name]
	if !ok {
		panic(fmt.Errorf("filter not found: %v", name))
	}

	var buf bytes.Buffer
	if err := flt.Compiled.Execute(&buf, &Args{Time: time.Now(), Arg: args}); err != nil {
		panic(fmt.Errorf("filter not found: %v", name))
	}

	log.Debug().Str("name", name).Str("filter", flt.Raw).Str("interpolated", buf.String()).Msg("interpolate")

	return buf.Bytes()
}

// GetNativeCollection returns collection object corresponding to collection model T.
func GetNativeCollection[T schema.Model](db *Database) *NativeCollection[T, T] {
	return &NativeCollection[T, T]{proj: GetProjection[T, T](db)}
}

// GetNativeProjection returns projection corresponding to collection model T, suitable to read
// sub-object P.
func GetNativeProjection[T, P schema.Model](db *Database) *NativeCollection[T, P] {
	return &NativeCollection[T, P]{proj: GetProjection[T, P](db)}
}

func getFilter[T schema.Model, F any](filter func(T, F) bool, args F) []byte {
	name := runtime.FuncForPC(reflect.ValueOf(filter).Pointer()).Name()

	return interpolate(name, Filters, args)
}

func getUpdate[T schema.Model, U any](update func(T, U), uargs U) []byte {
	name := runtime.FuncForPC(reflect.ValueOf(update).Pointer()).Name()

	return interpolate(name, Updates, uargs)
}

// Update partially updates documents based on the provided filter function
// and provided document mutation function.
func Update[T, P schema.Model, F any, U any](ctx context.Context, c *NativeCollection[T, P], filter func(T, F) bool, update func(T, U), args F, uargs U) (*UpdateResponse, error) {
	flt := getFilter(filter, args)
	upd := getUpdate(update, uargs)

	if _, err := c.proj.coll.db.Update(ctx, c.proj.coll.name, flt, upd); err != nil {
		return nil, err
	}

	return &UpdateResponse{}, nil
}

// UpdateOne partially updates first document matching the filter.
func UpdateOne[T, P schema.Model, F any, U any](ctx context.Context, c *NativeCollection[T, P], filter func(T, F) bool, update func(T, U), args F, uargs U) (*UpdateResponse, error) {
	flt := getFilter(filter, args)
	upd := getUpdate(update, uargs)

	if _, err := c.proj.coll.db.Update(ctx, c.proj.coll.name, flt, upd, &driver.UpdateOptions{Limit: 1}); err != nil {
		return nil, err
	}

	return &UpdateResponse{}, nil
}

// UpdateAll applies provided mutation function to all documents in the collection.
func UpdateAll[T, P schema.Model, U any](ctx context.Context, c *NativeCollection[T, P], update func(T, U), uargs U) (*UpdateResponse, error) {
	upd := getUpdate(update, uargs)

	if _, err := c.proj.coll.db.Update(ctx, c.proj.coll.name, driver.Filter(`{}`), upd); err != nil {
		return nil, err
	}

	return &UpdateResponse{}, nil
}

// Read returns documents which satisfies the filter.
func Read[T, P schema.Model, F any](ctx context.Context, c *NativeCollection[T, P], filter func(T, F) bool, args F) (*Iterator[P], error) {
	flt := getFilter(filter, args)

	it, err := getDB(ctx, c.proj.coll.db).Read(ctx, c.proj.coll.name, flt, c.proj.projection)
	if err != nil {
		return nil, err
	}

	return &Iterator[P]{Iterator: it, unmarshaler: c.proj.projectionUnmarshal}, nil
}

// ReadOne reads one document from the collection satisfying the filter.
func ReadOne[T, P schema.Model, F any](ctx context.Context, c *NativeCollection[T, P], filter func(T, F) bool, args F) (*P, error) {
	it, err := ReadWithOptions[T, P, F](ctx, c, filter, args, &ReadOptions{Limit: 1})
	if err != nil {
		return nil, err
	}

	defer it.Close()

	var doc P

	if !it.Next(&doc) {
		if it.Err() != nil {
			return nil, it.Err()
		}

		return nil, ErrNotFound
	}

	return &doc, nil
}

// ReadWithOptions returns specific fields of the documents according to the filter.
// It allows further configure returned documents by providing options:
//
//	Limit - only returned first "Limit" documents from the result
//	Skip - start returning documents after skipping "Skip" documents from the result
func ReadWithOptions[T, P schema.Model, F any](ctx context.Context, c *NativeCollection[T, P], filter func(T, F) bool, args F, options *ReadOptions) (*Iterator[P], error) {
	flt := getFilter(filter, args)

	it, err := getDB(ctx, c.proj.coll.db).Read(ctx, c.proj.coll.name, flt, c.proj.projection, &driver.ReadOptions{
		Limit:     options.Limit,
		Skip:      options.Skip,
		Offset:    options.Offset,
		Collation: (*api.Collation)(options.Collation),
	})
	if err != nil {
		return nil, err
	}

	return &Iterator[P]{Iterator: it, unmarshaler: c.proj.projectionUnmarshal}, nil
}

// ReadAll returns Iterator which iterates over all the documents
func ReadAll[T, P schema.Model](ctx context.Context, c *NativeCollection[T, P]) (*Iterator[P], error) {
	it, err := getDB(ctx, c.proj.coll.db).Read(ctx, c.proj.coll.name, driver.Filter(`{}`), c.proj.projection)
	if err != nil {
		return nil, err
	}

	return &Iterator[P]{Iterator: it, unmarshaler: c.proj.projectionUnmarshal}, nil
}

// Delete removes documents from the collection according to the filter.
func Delete[T, P schema.Model, F any](ctx context.Context, c *NativeCollection[T, P], filter func(T, F) bool, args F,
) (*DeleteResponse, error) {
	flt := getFilter(filter, args)

	_, err := getDB(ctx, c.proj.coll.db).Delete(ctx, c.proj.coll.name, flt)
	if err != nil {
		return nil, err
	}

	// TODO: forward response
	return &DeleteResponse{}, err
}

// DeleteOne deletes first document satisfying the filter.
func DeleteOne[T, P schema.Model, F any](ctx context.Context, c *NativeCollection[T, P], filter func(T, F) bool, args F,
) (*DeleteResponse, error) {
	flt := getFilter(filter, args)

	_, err := getDB(ctx, c.proj.coll.db).Delete(ctx, c.proj.coll.name, flt, &driver.DeleteOptions{Limit: 1})
	if err != nil {
		return nil, err
	}

	// TODO: forward response
	return &DeleteResponse{}, err
}

// DeleteAll removes all the documents from the collection.
func DeleteAll[T, P schema.Model](ctx context.Context, c *NativeCollection[T, P]) (*DeleteResponse, error) {
	_, err := getDB(ctx, c.proj.coll.db).Delete(ctx, c.proj.coll.name, driver.Filter(`{}`))
	if err != nil {
		return nil, err
	}

	// TODO: forward response
	return &DeleteResponse{}, err
}

// Drop drops the collection.
func (c *NativeCollection[T, P]) Drop(ctx context.Context) error {
	return getDB(ctx, c.proj.coll.db).DropCollection(ctx, c.proj.coll.name)
}

// Insert inserts documents into the collection.
// Returns an error if the documents with the same primary key exists already.
func (c *NativeCollection[T, P]) Insert(ctx context.Context, docs ...*T) (*InsertOrReplaceResponse, error) {
	return insertOrReplaceLow(ctx, c.proj.coll.db, c.proj.coll.name, true, docs...)
}

// InsertOrReplace inserts new documents and in the case of duplicate key
// replaces existing documents with the new document.
func (c *NativeCollection[T, P]) InsertOrReplace(ctx context.Context, docs ...*T) (*InsertOrReplaceResponse, error) {
	return insertOrReplaceLow(ctx, c.proj.coll.db, c.proj.coll.name, false, docs...)
}
