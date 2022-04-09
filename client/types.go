package client

import (
	"context"

	"github.com/tigrisdata/tigrisdb-client-go/driver"
)

// TransactionFunc is a user-provided function that will be run
// within the context of a tranaction.
type TransactionFunc func(
	ctx context.Context,
	tr driver.Tx,
) (interface{}, error)

// Client is the interface for a TigrisDB client.
type Client interface {
	// Database returns a Database interface for the provided
	// database name.
	//
	// TODO: Return bool/error if the database exists or not
	//       or should the client just get the error when they
	//       try to use it? Latter is more performant.
	Database(name string) Database

	// CreateDatabaseIfNotExist creates a database if it doesn't
	// already exist.
	CreateDatabaseIfNotExist(
		ctx context.Context,
		db string,
		options ...*driver.DatabaseOptions,
	) error

	// Driver returns the lower-level Driver interface
	// in case the caller needs it for low-level operations.
	Driver() driver.Driver
}

// Database is the interface for interacting with a specific database
// in TigrisDB.
type Database interface {
	// Run runs the provided TranactionFunc in a transaction. If the
	// function returns an error then the transaction will be aborted,
	// otherwise it will be comitted.
	Run(ctx context.Context, fn TransactionFunc) (interface{}, error)

	// ApplySchemasFromDirectory reads all the files in the provided
	// directory and attempts to apply any files with the .json
	// extension to the database as collection schemas in a single
	// transaction.
	ApplySchemasFromDirectory(path string) error
}
