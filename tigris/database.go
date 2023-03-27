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

// Package tigris provides an interface for accessing Tigris data-platform.
// This is the main client package you are looking for.
package tigris

import (
	"context"
	"fmt"

	"github.com/tigrisdata/tigris-client-go/util"

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
	driver driver.Driver
	name   string
}

func newDatabase(name string, drv driver.Driver) *Database {
	return &Database{
		name:   name,
		driver: drv,
	}
}

// CreateCollections creates collections in the Database using provided collection models
// This method is only needed if collections need to be created dynamically,
// all static collections are created by OpenDatabase.
func (db *Database) CreateCollections(ctx context.Context, model schema.Model, models ...schema.Model) error {
	schemas, err := schema.FromCollectionModels(SchemaVersion, schema.Documents, model, models...)
	if err != nil {
		return fmt.Errorf("error parsing model schema: %w", err)
	}

	return db.createCollectionsFromSchemas(ctx, schemas)
}

// CreateCollection creates collection in the Database using provided collection model and optional name.
// This method is only needed if collection need to be created dynamically,
// all static collections are created by OpenDatabase.
func (db *Database) CreateCollection(ctx context.Context, model schema.Model, name ...string) error {
	schemas, err := schema.FromCollectionModels(SchemaVersion, schema.Documents, model)
	if err != nil {
		return fmt.Errorf("error parsing model schema: %w", err)
	}

	if len(name) > 1 {
		return fmt.Errorf("only one name parameter allowed")
	}

	if len(name) == 1 {
		// there only one schema
		for _, v := range schemas {
			v.Name = name[0]
		}
	}

	return db.createCollectionsFromSchemas(ctx, schemas)
}

func (db *Database) createCollectionsFromSchemasLow(ctx context.Context, tx driver.Tx, inSchemas map[string]*schema.Schema) error {
	schemas := make([]driver.Schema, len(inSchemas))

	var i int

	for _, v := range inSchemas {
		sch, err := schema.Build(v)
		if err != nil {
			return err
		}

		schemas[i] = sch
		i++
	}

	var err error

	if tx != nil {
		_, err = tx.CreateOrUpdateCollections(ctx, schemas)
	} else {
		_, err = db.driver.UseDatabase(db.name).CreateOrUpdateCollections(ctx, schemas)
	}

	if err != nil {
		return err
	}

	// TODO: Handle partial failure. Use FailedAtIndex to retry and continue

	return nil
}

// createCollectionsFromSchemas transactionally creates collections from the provided schema map.
func (db *Database) createCollectionsFromSchemas(ctx context.Context, schemas map[string]*schema.Schema,
) error {
	// Run in existing transaction
	if tx := getTxCtx(ctx); tx != nil {
		return db.createCollectionsFromSchemasLow(ctx, tx.tx, schemas)
	}

	if SchemaVersion != 0 {
		dtx, err := db.driver.UseDatabase(db.name).BeginTx(ctx)
		if err != nil {
			return err
		}

		defer func() { _ = dtx.Rollback(ctx) }()

		if err = db.createCollectionsFromSchemasLow(ctx, dtx, schemas); err != nil {
			return err
		}

		return dtx.Commit(ctx)
	}

	return db.createCollectionsFromSchemasLow(ctx, nil, schemas)
}

// CreateBranch creates a branch of this database.
func (db *Database) CreateBranch(ctx context.Context, name string) (*driver.CreateBranchResponse, error) {
	inUseDB := db.driver.UseDatabase(db.name)
	return inUseDB.CreateBranch(ctx, name)
}

// DeleteBranch deletes a branch of this database, throws an error if "main" branch is being deleted.
func (db *Database) DeleteBranch(ctx context.Context, name string) (*driver.DeleteBranchResponse, error) {
	inUseDB := db.driver.UseDatabase(db.name)
	return inUseDB.DeleteBranch(ctx, name)
}

func openDatabase(ctx context.Context, d driver.Driver,
	project string, models ...schema.Model,
) (*Database, error) {
	db := newDatabase(project, d)

	if len(models) > 0 {
		err := db.CreateCollections(ctx, models[0], models[1:]...)
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

// OpenDatabase initializes Database from given collection models.
// It creates Database if necessary.
// Creates and migrates schemas of the collections which constitutes the Database.
// This is identical to calling:
//
//	client := tigris.NewClient(...)
//	client.OpenDatabase(...)
func OpenDatabase(ctx context.Context, cfg *Config, models ...schema.Model,
) (*Database, error) {
	util.Configure(cfg.Log)

	if getTxCtx(ctx) != nil {
		return nil, ErrNotTransactional
	}

	driver.SetSchemaVersion(SchemaVersion)

	d, err := driver.NewDriver(ctx, driverConfig(cfg))
	if err != nil {
		return nil, err
	}

	return openDatabase(ctx, d, cfg.Project, models...)
}

func MustOpenDatabase(ctx context.Context, cfg *Config, models ...schema.Model,
) *Database {
	db, err := OpenDatabase(ctx, cfg, models...)
	if err != nil {
		panic(err)
	}

	return db
}

// GetCollection returns collection object corresponding to collection model T.
func GetCollection[T schema.Model](db *Database, name ...string) *Collection[T] {
	var m T

	nm := schema.ModelName(&m)

	if len(name) > 0 {
		nm = name[0]
	}

	return getNamedCollection[T](db, nm)
}

func getNamedCollection[T schema.Model](db *Database, name string) *Collection[T] {
	return &Collection[T]{name: name, db: db.driver.UseDatabase(db.name)}
}

// TestOpenDatabase allows to provide mocked driver in tests.
func TestOpenDatabase(ctx context.Context, d driver.Driver,
	project string, models ...schema.Model,
) (*Database, error) {
	return openDatabase(ctx, d, project, models...)
}

// DropAllCollections allows to drop all database collections.
func (db *Database) DropAllCollections(ctx context.Context) error {
	return db.driver.UseDatabase(db.name).DropAllCollections(ctx)
}
