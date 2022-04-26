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

// Driver implements Tigris API
type Driver interface {
	// Insert array of documents into specified database and collection
	Insert(ctx context.Context, db string, collection string, docs []Document, options ...*InsertOptions) (*InsertResponse, error)
	// Replace array of documents into specified database and collection
	// Creates document if it doesn't exist
	Replace(ctx context.Context, db string, collection string, docs []Document, options ...*ReplaceOptions) (*ReplaceResponse, error)
	// Read documents matching specified filter in the specified database and collection
	Read(ctx context.Context, db string, collection string, filter Filter, fields Projection, options ...*ReadOptions) (Iterator, error)
	// Update documents matching specified filter, with provided fields projection
	Update(ctx context.Context, db string, collection string, filter Filter, fields Update, options ...*UpdateOptions) (*UpdateResponse, error)
	// Delete documents matching specified filter form the specified database and collection
	Delete(ctx context.Context, db string, collection string, filter Filter, options ...*DeleteOptions) (*DeleteResponse, error)
	// CreateOrUpdateCollection either creates a collection or update the collection with the new schema
	// There are three categories of data types supported:
	//   Primitive: Strings, Numbers, Binary Data, Booleans, UUIDs, DateTime
	//   Complex: Arrays
	//   Objects: A container data type defined by the user that stores fields of primitive types,
	//   complex types as well as other Objects
	//
	//  The data types are derived from the types defined in the JSON schema specification
	//  with extensions that enable support for richer semantics.
	//  As an example, the string is defined like this,
	//   {
	//     "name": {
	//       "type": "string"
	//     }
	//   }
	// More detailed information here: https://docs.tigrisdata.com/datamodels/types
	CreateOrUpdateCollection(ctx context.Context, db string, collection string, schema Schema, options ...*CollectionOptions) error
	// DropCollection deletes the collection and all documents it contains
	DropCollection(ctx context.Context, db string, collection string, options ...*CollectionOptions) error
	// CreateDatabase creates new database
	CreateDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error
	// DropDatabase deletes the database and all collections it contains
	DropDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error
	// ListCollections lists collections in the database
	ListCollections(ctx context.Context, db string, options ...*CollectionOptions) ([]string, error)
	// ListDatabases in the current namespace
	ListDatabases(ctx context.Context) ([]string, error)
	// DescribeCollection returns metadata of the collection in the database
	DescribeCollection(ctx context.Context, db string, collection string, options ...*CollectionOptions) (*DescribeCollectionResponse, error)
	// DescribeDatabase returns database metadata
	DescribeDatabase(ctx context.Context, db string) (*DescribeDatabaseResponse, error)
	// BeginTx starts new transaction
	BeginTx(ctx context.Context, db string, options ...*TxOptions) (Tx, error)
	// Close releases resources of the driver
	Close() error
}

// Tx object is used to atomically modify documents.
// This object is returned by BeginTx
type Tx interface {
	// Commit all the modification of the transaction
	Commit(ctx context.Context) error

	// Rollback discard all the modification made by the transaction
	Rollback(ctx context.Context) error

	CRUDTx
}

// CRUDTx is the interface that encapsulates the CRUD portions of the transaction API.
type CRUDTx interface {
	// Insert array of documents into specified database and collection.
	Insert(ctx context.Context, collection string, docs []Document, options ...*InsertOptions) (*InsertResponse, error)

	// Replace array of documents into specified database and collection
	// Creates document if it doesn't exist.
	Replace(ctx context.Context, collection string, docs []Document, options ...*ReplaceOptions) (*ReplaceResponse, error)

	// Read documents from the collection matching the specified filter.
	Read(ctx context.Context, collection string, filter Filter, fields Projection, options ...*ReadOptions) (Iterator, error)

	// Update documents in the collection matching the speficied filter.
	Update(ctx context.Context, collection string, filter Filter, fields Update, options ...*UpdateOptions) (*UpdateResponse, error)

	// Delete documents from the collection matching specified filter.
	Delete(ctx context.Context, collection string, filter Filter, options ...*DeleteOptions) (*DeleteResponse, error)

	// CreateOrUpdateCollection either creates a collection or update the collection with the new schema.
	CreateOrUpdateCollection(ctx context.Context, collection string, schema Schema, options ...*CollectionOptions) error

	// DropCollection deletes the collection and all documents it contains.
	DropCollection(ctx context.Context, collection string, options ...*CollectionOptions) error

	// ListCollections lists collections in the database.
	ListCollections(ctx context.Context, options ...*CollectionOptions) ([]string, error)
}

type driver struct {
	driverWithOptions
}

func (c *driver) Insert(ctx context.Context, db string, collection string, docs []Document, options ...*InsertOptions) (*InsertResponse, error) {
	opts, err := validateOptionsParam(options, &InsertOptions{})
	if err != nil {
		return nil, err
	}

	return c.insertWithOptions(ctx, db, collection, docs, opts.(*InsertOptions))
}

func (c *driver) Replace(ctx context.Context, db string, collection string, docs []Document, options ...*ReplaceOptions) (*ReplaceResponse, error) {
	opts, err := validateOptionsParam(options, &ReplaceOptions{})
	if err != nil {
		return nil, err
	}

	return c.replaceWithOptions(ctx, db, collection, docs, opts.(*ReplaceOptions))
}

func (c *driver) Update(ctx context.Context, db string, collection string, filter Filter, fields Update, options ...*UpdateOptions) (*UpdateResponse, error) {
	opts, err := validateOptionsParam(options, &UpdateOptions{})
	if err != nil {
		return nil, err
	}

	return c.updateWithOptions(ctx, db, collection, filter, fields, opts.(*UpdateOptions))
}

func (c *driver) Delete(ctx context.Context, db string, collection string, filter Filter, options ...*DeleteOptions) (*DeleteResponse, error) {
	opts, err := validateOptionsParam(options, &DeleteOptions{})
	if err != nil {
		return nil, err
	}

	return c.deleteWithOptions(ctx, db, collection, filter, opts.(*DeleteOptions))
}

func (c *driver) Read(ctx context.Context, db string, collection string, filter Filter, fields Projection, options ...*ReadOptions) (Iterator, error) {
	opts, err := validateOptionsParam(options, &ReadOptions{})
	if err != nil {
		return nil, err
	}

	return c.readWithOptions(ctx, db, collection, filter, fields, opts.(*ReadOptions))
}

func (c *driver) CreateOrUpdateCollection(ctx context.Context, db string, collection string, schema Schema, options ...*CollectionOptions) error {
	opts, err := validateOptionsParam(options, &CollectionOptions{})
	if err != nil {
		return err
	}

	return c.createOrUpdateCollectionWithOptions(ctx, db, collection, schema, opts.(*CollectionOptions))
}

func (c *driver) DropCollection(ctx context.Context, db string, collection string, options ...*CollectionOptions) error {
	opts, err := validateOptionsParam(options, &CollectionOptions{})
	if err != nil {
		return err
	}

	return c.dropCollectionWithOptions(ctx, db, collection, opts.(*CollectionOptions))
}

func (c *driver) ListCollections(ctx context.Context, db string, options ...*CollectionOptions) ([]string, error) {
	opts, err := validateOptionsParam(options, &CollectionOptions{})
	if err != nil {
		return nil, err
	}

	return c.listCollectionsWithOptions(ctx, db, opts.(*CollectionOptions))
}

func (c *driver) DescribeCollection(ctx context.Context, db string, collection string, options ...*CollectionOptions) (*DescribeCollectionResponse, error) {
	opts, err := validateOptionsParam(options, &CollectionOptions{})
	if err != nil {
		return nil, err
	}

	return c.describeCollectionWithOptions(ctx, db, collection, opts.(*CollectionOptions))
}

func (c *driver) CreateDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error {
	opts, err := validateOptionsParam(options, &DatabaseOptions{})
	if err != nil {
		return err
	}

	return c.createDatabaseWithOptions(ctx, db, opts.(*DatabaseOptions))
}

func (c *driver) DropDatabase(ctx context.Context, db string, options ...*DatabaseOptions) error {
	opts, err := validateOptionsParam(options, &DatabaseOptions{})
	if err != nil {
		return err
	}

	return c.dropDatabaseWithOptions(ctx, db, opts.(*DatabaseOptions))
}
func (c *driver) BeginTx(ctx context.Context, db string, options ...*TxOptions) (Tx, error) {
	opts, err := validateOptionsParam(options, &TxOptions{})
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

func (c *driverTxWithOptions) Insert(ctx context.Context, collection string, docs []Document, options ...*InsertOptions) (*InsertResponse, error) {
	opts, err := validateOptionsParam(options, &InsertOptions{})
	if err != nil {
		return nil, err
	}

	return c.insertWithOptions(ctx, collection, docs, opts.(*InsertOptions))
}

func (c *driverTxWithOptions) Replace(ctx context.Context, collection string, docs []Document, options ...*ReplaceOptions) (*ReplaceResponse, error) {
	opts, err := validateOptionsParam(options, &ReplaceOptions{})
	if err != nil {
		return nil, err
	}

	return c.replaceWithOptions(ctx, collection, docs, opts.(*ReplaceOptions))
}

func (c *driverTxWithOptions) Update(ctx context.Context, collection string, filter Filter, fields Update, options ...*UpdateOptions) (*UpdateResponse, error) {
	opts, err := validateOptionsParam(options, &UpdateOptions{})
	if err != nil {
		return nil, err
	}

	return c.updateWithOptions(ctx, collection, filter, fields, opts.(*UpdateOptions))
}

func (c *driverTxWithOptions) Delete(ctx context.Context, collection string, filter Filter, options ...*DeleteOptions) (*DeleteResponse, error) {
	opts, err := validateOptionsParam(options, &DeleteOptions{})
	if err != nil {
		return nil, err
	}

	return c.deleteWithOptions(ctx, collection, filter, opts.(*DeleteOptions))
}

func (c *driverTxWithOptions) Read(ctx context.Context, collection string, filter Filter, fields Projection, options ...*ReadOptions) (Iterator, error) {
	opts, err := validateOptionsParam(options, &ReadOptions{})
	if err != nil {
		return nil, err
	}

	return c.readWithOptions(ctx, collection, filter, fields, opts.(*ReadOptions))
}

func (c *driverTxWithOptions) CreateOrUpdateCollection(ctx context.Context, collection string, schema Schema, options ...*CollectionOptions) error {
	opts, err := validateOptionsParam(options, &CollectionOptions{})
	if err != nil {
		return err
	}

	return c.createOrUpdateCollectionWithOptions(ctx, collection, schema, opts.(*CollectionOptions))
}

func (c *driverTxWithOptions) DropCollection(ctx context.Context, collection string, options ...*CollectionOptions) error {
	opts, err := validateOptionsParam(options, &CollectionOptions{})
	if err != nil {
		return err
	}

	return c.dropCollectionWithOptions(ctx, collection, opts.(*CollectionOptions))
}

func (c *driverTxWithOptions) ListCollections(ctx context.Context, options ...*CollectionOptions) ([]string, error) {
	opts, err := validateOptionsParam(options, &CollectionOptions{})
	if err != nil {
		return nil, err
	}
	return c.listCollectionsWithOptions(ctx, opts.(*CollectionOptions))
}

func validateOptionsParam(options interface{}, out interface{}) (interface{}, error) {
	v := reflect.ValueOf(options)

	if (v.Kind() != reflect.Array && v.Kind() != reflect.Slice) || v.Len() > 1 {
		return nil, fmt.Errorf("API accepts no more then one options parameter")
	}

	if v.Len() < 1 {
		return out, nil
	}

	return v.Index(0).Interface(), nil
}

// NewDriver connect to Tigris at the specified URL
// URL should be in the form: {hostname}:{port}
func NewDriver(ctx context.Context, url string, config *Config) (Driver, error) {
	if config == nil {
		config = &Config{}
	}
	if DefaultProtocol == GRPC {
		return NewGRPCClient(ctx, url, config)
	} else if DefaultProtocol == HTTP {
		return NewHTTPClient(ctx, url, config)
	}
	return nil, fmt.Errorf("unsupported protocol")
}
