package client

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/schema"
)

// TxFunc is a user-provided function that will be run
// within the context of a transaction.
type TxFunc func(
	ctx context.Context,
	tr Tx,
) (interface{}, error)

// Tx is the interface for a client-level transaction. It does
// not expose operations like Commit()/Abort as it is meant to be used
// within the Transact() method which abstracts away those operations.
type Tx interface {
	driver.CRUDTx
}

// Database is the interface for interacting with a specific database
// in Tigris.
type Database interface {
	// Transact runs the provided TransactionFunc in a transaction. If the
	// function returns an error then the transaction will be aborted,
	// otherwise it will be committed.
	Transact(ctx context.Context, fn TxFunc) (interface{}, error)

	CreateCollections(ctx context.Context, dbName string, schemas []*schema.Schema) error
}

type database struct {
	name   string
	driver driver.Driver
}

func newDatabase(name string, driver driver.Driver) Database {
	return &database{
		name:   name,
		driver: driver,
	}
}

func (db *database) Transact(
	ctx context.Context,
	fn TxFunc,
) (interface{}, error) {
	tx, err := db.driver.BeginTx(ctx, db.name, nil)
	if err != nil {
		return nil, fmt.Errorf("error beginning transaction: %w", err)
	}

	res, err := fn(ctx, tx)
	if err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return nil, fmt.Errorf(
				"error trying to rollback transaction: %v, original error: %w",
				rollbackErr, err)
		}
		return nil, fmt.Errorf("error running transaction: %w", err)
	}

	return res, nil
}

func (db *database) CreateCollectionsFromModels(ctx context.Context, dbName string, model interface{}, models ...interface{}) error {
	schemas, err := schema.FromCollectionModels(model, models)
	if err != nil {
		return fmt.Errorf("error parsing model schema: %w", err)
	}

	return db.CreateCollections(ctx, dbName, schemas)
}

func (db *database) CreateCollectionsFromDatabaseModel(ctx context.Context, dbModel interface{}) error {
	dbName, schemas, err := schema.FromDatabaseModel(dbModel)
	if err != nil {
		return err
	}

	return db.CreateCollections(ctx, dbName, schemas)
}

func (db *database) CreateCollections(ctx context.Context, dbName string, schemas []*schema.Schema) error {
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
		log.Debug().Interface("schema", v).Str("collection", v.Name).Msg("migrateModel")
		err = tx.CreateOrUpdateCollection(ctx, v.Name, sch)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}
