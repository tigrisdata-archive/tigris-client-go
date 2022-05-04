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

// Package tigris provides an interface for accessing Tigris data-platform
// This is the main client package you are looking for.
package tigris

import (
	"context"
	"fmt"
	"strings"

	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/schema"
)

// Database is the interface for interacting with a Tigris Database
// Due to the limitations of Golang generics instantiations of the collections
// should be done using GetCollection[Model](ctx, db) top level function instead of
// method of this interface.
// Similarly to get access to collection APIs in a transaction
// top level GetTxCollection(ctx, tx) function should be used
// instead of method of Tx interface
type Database struct {
	name   string
	driver driver.Driver
}

func newDatabase(name string, driver driver.Driver) *Database {
	return &Database{
		name:   name,
		driver: driver,
	}
}

// CreateCollections creates collections in the Database using provided collection models
// This method is only needed if collections need to be created dynamically,
// all static collections are created by OpenDatabase
func (db *Database) CreateCollections(ctx context.Context, model schema.Model, models ...schema.Model) error {
	schemas, err := schema.FromCollectionModels(model, models...)
	if err != nil {
		return fmt.Errorf("error parsing model schema: %w", err)
	}

	return db.createCollectionsFromSchemas(ctx, db.name, schemas)
}

// createCollectionsFromSchemas transactionally creates collections from the provided schema map
func (db *Database) createCollectionsFromSchemas(ctx context.Context, dbName string, schemas map[string]*schema.Schema) error {
	tx, err := db.driver.BeginTx(ctx, dbName)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, v := range schemas {
		sch, err := schema.Build(v)
		if err != nil {
			return err
		}
		err = tx.CreateOrUpdateCollection(ctx, v.Name, sch)
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

// Drop Database.
// All the collections in the Database will be dropped
func (db *Database) Drop(ctx context.Context) error {
	return db.driver.DropDatabase(ctx, db.name)
}

// Tx executes given set of operations in a transaction
//
// All operation in the "fn" closure is executed atomically in a transaction.
// If the closure returns no error the changes are applied to the database,
// when error is returned then changes just discarded,
// database stays intact.
func (db *Database) Tx(ctx context.Context, fn func(ctx context.Context, tx *Tx) error) error {
	dtx, err := db.driver.BeginTx(ctx, db.name)
	if err != nil {
		return err
	}
	defer func() { _ = dtx.Rollback(ctx) }()

	tx := &Tx{db, dtx}

	if err = fn(ctx, tx); err != nil {
		return err
	}

	return dtx.Commit(ctx)
}

// openDatabaseFromModels creates Database and collections from the provided collection models
func openDatabaseFromModels(ctx context.Context, d driver.Driver, cfg *config.Database, dbName string, model schema.Model, models ...schema.Model) (*Database, error) {
	// optionally creates database if it's allowed
	if !cfg.MustExist {
		err := d.CreateDatabase(ctx, dbName)
		if err != nil {
			if !strings.Contains(err.Error(), "already exist") {
				return nil, err
			}
		}
	}

	db := newDatabase(dbName, d)

	err := db.CreateCollections(ctx, model, models...)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// OpenDatabase initializes Database from given collection models.
// It creates Database if necessary.
// Creates and migrates schemas of the collections which constitutes the Database
func OpenDatabase(ctx context.Context, cfg *config.Database, dbName string, model schema.Model, models ...schema.Model) (*Database, error) {
	d, err := driver.NewDriver(ctx, &cfg.Driver)
	if err != nil {
		return nil, err
	}

	return openDatabaseFromModels(ctx, d, cfg, dbName, model, models...)
}

// GetCollection returns collection object corresponding to collection model T
func GetCollection[T schema.Model](db *Database) *Collection[T] {
	var m T
	name := schema.ModelName(&m)
	return getNamedCollection[T](db, name)
}

func getNamedCollection[T schema.Model](db *Database, name string) *Collection[T] {
	return &Collection[T]{name: name, crud: db.driver.UseDatabase(db.name)}
}

// Tx is the interface for accessing APIs in a transactional way
type Tx struct {
	db *Database
	tx driver.Tx
}

// GetTxCollection returns collection object corresponding to collection model T
func GetTxCollection[T schema.Model](tx *Tx) *Collection[T] {
	var m T
	name := schema.ModelName(&m)
	return getNamedTxCollection[T](tx, name)
}

func getNamedTxCollection[T schema.Model](tx *Tx, name string) *Collection[T] {
	return &Collection[T]{name: name, crud: tx.tx}
}
