package driver

import (
	"context"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/oauth2"
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

func getAuthToken(ctx context.Context, config *Config) (*oauth2.Token, *oauth2.Config, context.Context) {
	token := config.Token
	if os.Getenv(TOKEN_ENV) != "" {
		token = os.Getenv(TOKEN_ENV)
	}

	parts := strings.Split(token, ":")

	t := oauth2.Token{}

	if len(parts) > 0 {
		t.AccessToken = parts[0]
	}

	if len(parts) > 1 {
		t.RefreshToken = parts[1]
		// So as we have refresh token, just disregard current access token and refresh immediately
		t.Expiry = time.Now()
	}

	tr := &http.Transport{
		TLSClientConfig: config.TLS,
	}

	ocfg := &oauth2.Config{Endpoint: oauth2.Endpoint{TokenURL: TOKEN_REFRESH_URL}}

	return &t, ocfg, context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Transport: tr})
}
