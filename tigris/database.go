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

// Package tigris provides an interface for accessing Tigris data-platform.
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
// instead of method of Tx interface.
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
// all static collections are created by OpenDatabase.
func (db *Database) CreateCollections(ctx context.Context, model schema.Model, models ...schema.Model) error {
	schemas, err := schema.FromCollectionModels(schema.Documents, model, models...)
	if err != nil {
		return fmt.Errorf("error parsing model schema: %w", err)
	}

	return db.createCollectionsFromSchemas(ctx, db.name, schemas)
}

// CreateTopics creates message type collections.
func (db *Database) CreateTopics(ctx context.Context, model schema.Model, models ...schema.Model) error {
	schemas, err := schema.FromCollectionModels(schema.Messages, model, models...)
	if err != nil {
		return fmt.Errorf("error parsing model schema: %w", err)
	}

	return db.createCollectionsFromSchemas(ctx, db.name, schemas)
}

func (db *Database) createCollectionsFromSchemasLow(ctx context.Context, tx driver.Tx, schemas map[string]*schema.Schema) error {
	for _, v := range schemas {
		sch, err := schema.Build(v)
		if err != nil {
			return err
		}

		if err = tx.CreateOrUpdateCollection(ctx, v.Name, sch); err != nil {
			return err
		}
	}

	return nil
}

// createCollectionsFromSchemas transactionally creates collections from the provided schema map.
func (db *Database) createCollectionsFromSchemas(ctx context.Context, dbName string,
	schemas map[string]*schema.Schema,
) error {
	// Run in existing transaction
	if tx := getTxCtx(ctx); tx != nil {
		return db.createCollectionsFromSchemasLow(ctx, tx.tx, schemas)
	}

	// Run in new implicit transaction
	tx, err := db.driver.BeginTx(ctx, dbName)
	if err != nil {
		return err
	}

	defer func() { _ = tx.Rollback(ctx) }()

	if err = db.createCollectionsFromSchemasLow(ctx, tx, schemas); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}

// openDatabaseFromModels creates Database and collections from the provided collection models.
func openDatabaseFromModels(ctx context.Context, d driver.Driver, cfg *config.Database, dbName string,
	model schema.Model, models ...schema.Model,
) (*Database, error) {
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
// Creates and migrates schemas of the collections which constitutes the Database.
func OpenDatabase(ctx context.Context, cfg *config.Database, dbName string, model schema.Model,
	models ...schema.Model,
) (*Database, error) {
	if getTxCtx(ctx) != nil {
		return nil, ErrNotTransactional
	}

	d, err := driver.NewDriver(ctx, &cfg.Driver)
	if err != nil {
		return nil, err
	}

	return openDatabaseFromModels(ctx, d, cfg, dbName, model, models...)
}

// DropDatabase deletes the database and all collections in it.
func DropDatabase(ctx context.Context, cfg *config.Database, dbName string) error {
	if getTxCtx(ctx) != nil {
		return ErrNotTransactional
	}

	d, err := driver.NewDriver(ctx, &cfg.Driver)
	if err != nil {
		return err
	}

	defer func() { _ = d.Close() }()

	return d.DropDatabase(ctx, dbName)
}

// GetCollection returns collection object corresponding to collection model T.
func GetCollection[T schema.Model](db *Database) *Collection[T] {
	var m T
	name := schema.ModelName(&m)
	return getNamedCollection[T](db, name)
}

func getNamedCollection[T schema.Model](db *Database, name string) *Collection[T] {
	return &Collection[T]{name: name, db: db.driver.UseDatabase(db.name)}
}

// GetTopic returns topic object corresponding to topic model T.
func GetTopic[T schema.Model](db *Database) *Topic[T] {
	var m T
	name := schema.ModelName(&m)
	return getNamedTopic[T](db, name)
}

func getNamedTopic[T schema.Model](db *Database, name string) *Topic[T] {
	return &Topic[T]{name: name, db: db.driver.UseDatabase(db.name)}
}
