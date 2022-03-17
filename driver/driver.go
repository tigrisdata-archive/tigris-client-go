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

package driver

import (
	"context"
	"fmt"
	"reflect"
)

type Driver interface {
	Insert(ctx context.Context, db string, collection string, docs []Document, options ...*InsertOptions) (InsertResponse, error)
	Read(ctx context.Context, db string, collection string, filter Filter, options ...*ReadOptions) (Iterator, error)
	Update(ctx context.Context, db string, collection string, filter Filter, fields Fields, options ...*UpdateOptions) (UpdateResponse, error)
	Delete(ctx context.Context, db string, collection string, filter Filter, options ...*DeleteOptions) (DeleteResponse, error)
	CreateCollection(ctx context.Context, db string, collection string, schema Schema, options ...*CollectionOptions) error
	AlterCollection(ctx context.Context, db string, collection string, schema Schema, options ...*CollectionOptions) error
	DropCollection(ctx context.Context, db string, collection string, options ...*CollectionOptions) error
	CreateDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error
	DropDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error
	ListCollections(ctx context.Context, db string) ([]string, error)
	ListDatabases(ctx context.Context) ([]string, error)
	BeginTx(ctx context.Context, db string, options ...*TxOptions) (Tx, error)
	Close() error
}

type Tx interface {
	Insert(ctx context.Context, collection string, docs []Document, options ...*InsertOptions) (InsertResponse, error)
	Read(ctx context.Context, collection string, filter Filter, options ...*ReadOptions) (Iterator, error)
	Update(ctx context.Context, collection string, filter Filter, fields Fields, options ...*UpdateOptions) (UpdateResponse, error)
	Delete(ctx context.Context, collection string, filter Filter, options ...*DeleteOptions) (DeleteResponse, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type driver struct {
	driverWithOptions
}

func (c *driver) Insert(ctx context.Context, db string, collection string, docs []Document, options ...*InsertOptions) (InsertResponse, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.insertWithOptions(ctx, db, collection, docs, opts.(*InsertOptions))
}

func (c *driver) Update(ctx context.Context, db string, collection string, filter Filter, fields Fields, options ...*UpdateOptions) (UpdateResponse, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.updateWithOptions(ctx, db, collection, filter, fields, opts.(*UpdateOptions))
}

func (c *driver) Delete(ctx context.Context, db string, collection string, filter Filter, options ...*DeleteOptions) (DeleteResponse, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.deleteWithOptions(ctx, db, collection, filter, opts.(*DeleteOptions))
}

func (c *driver) Read(ctx context.Context, db string, collection string, filter Filter, options ...*ReadOptions) (Iterator, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.readWithOptions(ctx, db, collection, filter, opts.(*ReadOptions))
}

func (c *driver) CreateCollection(ctx context.Context, db string, collection string, schema Schema, options ...*CollectionOptions) error {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return err
	}

	return c.createCollectionWithOptions(ctx, db, collection, schema, opts.(*CollectionOptions))
}

func (c *driver) AlterCollection(ctx context.Context, db string, collection string, schema Schema, options ...*CollectionOptions) error {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return err
	}

	return c.createCollectionWithOptions(ctx, db, collection, schema, opts.(*CollectionOptions))
}

func (c *driver) DropCollection(ctx context.Context, db string, collection string, options ...*CollectionOptions) error {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return err
	}

	return c.dropCollectionWithOptions(ctx, db, collection, opts.(*CollectionOptions))
}

func (c *driver) CreateDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return err
	}

	return c.createDatabaseWithOptions(ctx, db, opts.(*DatabaseOptions))
}

func (c *driver) DropDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return err
	}

	return c.dropDatabaseWithOptions(ctx, db, opts.(*DatabaseOptions))
}

func (c *driver) BeginTx(ctx context.Context, db string, options ...*TxOptions) (Tx, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	tx, err := c.beginTxWithOptions(ctx, db, opts.(*TxOptions))
	if err != nil {
		return nil, err
	}
	return &driverTxWithOptions{txWithOptions: tx, db: db}, nil
}

type driverTxWithOptions struct {
	txWithOptions
	db string
}

func (c *driverTxWithOptions) Insert(ctx context.Context, collection string, docs []Document, options ...*InsertOptions) (InsertResponse, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.insertWithOptions(ctx, collection, docs, opts.(*InsertOptions))
}

func (c *driverTxWithOptions) Update(ctx context.Context, collection string, filter Filter, fields Fields, options ...*UpdateOptions) (UpdateResponse, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.updateWithOptions(ctx, collection, filter, fields, opts.(*UpdateOptions))
}

func (c *driverTxWithOptions) Delete(ctx context.Context, collection string, filter Filter, options ...*DeleteOptions) (DeleteResponse, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.deleteWithOptions(ctx, collection, filter, opts.(*DeleteOptions))
}

func (c *driverTxWithOptions) Read(ctx context.Context, collection string, filter Filter, options ...*ReadOptions) (Iterator, error) {
	opts, err := validateOptionsParam(options)
	if err != nil {
		return nil, err
	}

	return c.readWithOptions(ctx, collection, filter, opts.(*ReadOptions))
}

func validateOptionsParam(options interface{}) (interface{}, error) {
	v := reflect.ValueOf(options)

	if (v.Kind() != reflect.Array && v.Kind() != reflect.Slice) || v.Len() > 1 {
		return nil, fmt.Errorf("API accepts no more then one options parameter")
	}

	if v.Len() < 1 {
		return nil, nil
	}

	return v.Index(0).Interface(), nil
}

func NewDriver(ctx context.Context, url string, config *Config) (Driver, error) {
	if DefaultProtocol == GRPC {
		return NewGRPCClient(ctx, url, config)
	} else if DefaultProtocol == HTTP {
		return NewHTTPClient(ctx, url, config)
	}
	return nil, fmt.Errorf("unsupported protocol")
}
