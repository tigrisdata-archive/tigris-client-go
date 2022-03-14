package driver

import (
	"context"
	"os"
)

type driverWithOptions interface {
	insertWithOptions(ctx context.Context, db string, collection string, docs []Document, options *InsertOptions) (InsertResponse, error)
	readWithOptions(ctx context.Context, db string, collection string, filter Filter, options *ReadOptions) (Iterator, error)
	updateWithOptions(ctx context.Context, db string, collection string, filter Filter, fields Fields, options *UpdateOptions) (UpdateResponse, error)
	deleteWithOptions(ctx context.Context, db string, collection string, filter Filter, options *DeleteOptions) (DeleteResponse, error)
	createCollectionWithOptions(ctx context.Context, db string, collection string, schema Schema, options *CollectionOptions) error
	alterCollectionWithOptions(ctx context.Context, db string, collection string, schema Schema, options *CollectionOptions) error
	dropCollectionWithOptions(ctx context.Context, db string, collection string, options *CollectionOptions) error
	createDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error
	dropDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error
	beginTxWithOptions(ctx context.Context, db string, options *TxOptions) (txWithOptions, error)

	ListCollections(ctx context.Context, db string) ([]string, error)
	ListDatabases(ctx context.Context) ([]string, error)
	Close() error
}

type txWithOptions interface {
	insertWithOptions(ctx context.Context, collection string, docs []Document, options *InsertOptions) (InsertResponse, error)
	readWithOptions(ctx context.Context, collection string, filter Filter, options *ReadOptions) (Iterator, error)
	updateWithOptions(ctx context.Context, collection string, filter Filter, fields Fields, options *UpdateOptions) (UpdateResponse, error)
	deleteWithOptions(ctx context.Context, collection string, filter Filter, options *DeleteOptions) (DeleteResponse, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

func getAuthToken(config *Config) string {
	token := config.AuthToken
	if os.Getenv(AUTH_TOKEN_ENV) != "" {
		token = os.Getenv(AUTH_TOKEN_ENV)
	}
	return token
}
