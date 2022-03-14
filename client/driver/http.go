package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"unsafe"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	userHTTP "github.com/tigrisdata/tigrisdb-client-go/api/client/v1/user"
)

type httpStatus interface {
	StatusCode() int
	Status() string
}

func HTTPError(err error, resp httpStatus) error {
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK && resp.StatusCode() != http.StatusCreated {
		return fmt.Errorf(resp.Status())
	}
	return nil
}

type httpDriver struct {
	*httpCRUD
}

func NewHTTPClient(_ context.Context, url string, config *Config) (Driver, error) {
	auth, err := securityprovider.NewSecurityProviderBearerToken(getAuthToken(config))
	if err != nil {
		return nil, err
	}

	t := &http.Transport{
		TLSClientConfig: config.TLS,
	}

	hc := http.Client{Transport: t}

	c, err := userHTTP.NewClientWithResponses(url,
		userHTTP.WithRequestEditorFn(auth.Intercept),
		userHTTP.WithHTTPClient(&hc),
	)
	return &driver{driverWithOptions: &httpDriver{httpCRUD: &httpCRUD{api: c}}}, err
}

func (c *httpDriver) Close() error {
	return nil
}

func (c *httpDriver) ListDatabases(ctx context.Context) ([]string, error) {
	resp, err := c.api.TigrisDBListDatabasesWithResponse(ctx)
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	return *resp.JSON200.Dbs, nil
}

func (c *httpDriver) ListCollections(ctx context.Context, db string) ([]string, error) {
	resp, err := c.api.TigrisDBListCollectionsWithResponse(ctx, db)
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	return *resp.JSON200.Collections, nil
}

func convertDatabaseOptions(_ *DatabaseOptions) *userHTTP.DatabaseOptions {
	return &userHTTP.DatabaseOptions{}
}

func convertCollectionOptions(_ *CollectionOptions) *userHTTP.CollectionOptions {
	return &userHTTP.CollectionOptions{}
}

func convertTransactionOptions(_ *TxOptions) *userHTTP.TransactionOptions {
	return &userHTTP.TransactionOptions{}
}

func (c *httpDriver) createDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error {
	resp, err := c.api.TigrisDBCreateDatabaseWithResponse(ctx, db, userHTTP.TigrisDBCreateDatabaseJSONRequestBody{
		Db:      &db,
		Options: convertDatabaseOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) dropDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error {
	resp, err := c.api.TigrisDBDropDatabaseWithResponse(ctx, db, userHTTP.TigrisDBDropDatabaseJSONRequestBody{
		Db:      &db,
		Options: convertDatabaseOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) createCollectionWithOptions(ctx context.Context, db string, collection string, schema Schema, options *CollectionOptions) error {
	resp, err := c.api.TigrisDBCreateCollectionWithResponse(ctx, db, collection, userHTTP.TigrisDBCreateCollectionJSONRequestBody{
		Schema:  json.RawMessage(schema),
		Options: convertCollectionOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) alterCollectionWithOptions(ctx context.Context, db string, collection string, schema Schema, options *CollectionOptions) error {
	resp, err := c.api.TigrisDBAlterCollectionWithResponse(ctx, db, collection, userHTTP.TigrisDBAlterCollectionJSONRequestBody{
		Schema:  json.RawMessage(schema),
		Options: convertCollectionOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) dropCollectionWithOptions(ctx context.Context, db string, collection string, _ *CollectionOptions) error {
	resp, err := c.api.TigrisDBDropCollectionWithResponse(ctx, db, collection)
	return HTTPError(err, resp)
}

func (c *httpDriver) beginTxWithOptions(ctx context.Context, db string, options *TxOptions) (txWithOptions, error) {
	resp, err := c.api.TigrisDBBeginTransactionWithResponse(ctx, db, userHTTP.TigrisDBBeginTransactionJSONRequestBody{
		Options: convertTransactionOptions(options),
	})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	if resp.JSON200.TxCtx == nil {
		return nil, fmt.Errorf("empty transaction context in response")
	}
	return &httpTx{db: db, httpCRUD: &httpCRUD{api: c.api, txCtx: *resp.JSON200.TxCtx}}, nil
}

type httpTx struct {
	db string
	*httpCRUD
}

func (c *httpTx) Commit(ctx context.Context) error {
	resp, err := c.api.TigrisDBCommitTransactionWithResponse(ctx, c.db, userHTTP.TigrisDBCommitTransactionJSONRequestBody{
		TxCtx: &c.txCtx,
	})
	return HTTPError(err, resp)
}

func (c *httpTx) Rollback(ctx context.Context) error {
	resp, err := c.api.TigrisDBRollbackTransactionWithResponse(ctx, c.db, userHTTP.TigrisDBRollbackTransactionJSONRequestBody{
		TxCtx: &c.txCtx,
	})
	return HTTPError(err, resp)
}

func (c *httpTx) insertWithOptions(ctx context.Context, collection string, docs []Document, options *InsertOptions) (InsertResponse, error) {
	return c.httpCRUD.insertWithOptions(ctx, c.db, collection, docs, options)
}

func (c *httpTx) updateWithOptions(ctx context.Context, collection string, filter Filter, fields Fields, options *UpdateOptions) (UpdateResponse, error) {
	return c.httpCRUD.updateWithOptions(ctx, c.db, collection, filter, fields, options)
}

func (c *httpTx) deleteWithOptions(ctx context.Context, collection string, filter Filter, options *DeleteOptions) (DeleteResponse, error) {
	return c.httpCRUD.deleteWithOptions(ctx, c.db, collection, filter, options)
}

func (c *httpTx) readWithOptions(ctx context.Context, collection string, filter Filter, options *ReadOptions) (Iterator, error) {
	return c.httpCRUD.readWithOptions(ctx, c.db, collection, filter, options)
}

type httpCRUD struct {
	api   *userHTTP.ClientWithResponses
	txCtx userHTTP.TransactionCtx
}

func (c *httpCRUD) convertWriteOptions(_ *WriteOptions) *userHTTP.WriteOptions {
	opts := userHTTP.WriteOptions{}
	if c.txCtx.Id != nil && *c.txCtx.Id != "" {
		opts.TxCtx = &userHTTP.TransactionCtx{Id: c.txCtx.Id, Origin: c.txCtx.Origin}
	}
	return &opts
}

func (c *httpCRUD) convertInsertOptions(_ *InsertOptions) *userHTTP.InsertRequestOptions {
	return &userHTTP.InsertRequestOptions{WriteOptions: c.convertWriteOptions(&WriteOptions{})}
}

func (c *httpCRUD) convertUpdateOptions(_ *UpdateOptions) *userHTTP.UpdateRequestOptions {
	return &userHTTP.UpdateRequestOptions{WriteOptions: c.convertWriteOptions(&WriteOptions{})}
}

func (c *httpCRUD) convertDeleteOptions(_ *DeleteOptions) *userHTTP.DeleteRequestOptions {
	return &userHTTP.DeleteRequestOptions{WriteOptions: c.convertWriteOptions(&WriteOptions{})}
}

func (c *httpCRUD) convertReadOptions(_ *ReadOptions) *userHTTP.ReadRequestOptions {
	opts := userHTTP.ReadRequestOptions{}
	if c.txCtx.Id != nil && *c.txCtx.Id != "" {
		opts.TxCtx = &userHTTP.TransactionCtx{Id: c.txCtx.Id, Origin: c.txCtx.Origin}
	}
	return &opts
}

func (c *httpCRUD) insertWithOptions(ctx context.Context, db string, collection string, docs []Document, options *InsertOptions) (InsertResponse, error) {
	resp, err := c.api.TigrisDBInsertWithResponse(ctx, db, collection, userHTTP.TigrisDBInsertJSONRequestBody{
		Documents: (*[]json.RawMessage)(unsafe.Pointer(&docs)),
		Options:   c.convertInsertOptions(options),
	})
	return nil, HTTPError(err, resp)
}

func (c *httpCRUD) updateWithOptions(ctx context.Context, db string, collection string, filter Filter, fields Fields, options *UpdateOptions) (UpdateResponse, error) {
	resp, err := c.api.TigrisDBUpdateWithResponse(ctx, db, collection, userHTTP.TigrisDBUpdateJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Fields:  json.RawMessage(fields),
		Options: c.convertUpdateOptions(options),
	})
	return nil, HTTPError(err, resp)
}

func (c *httpCRUD) deleteWithOptions(ctx context.Context, db string, collection string, filter Filter, options *DeleteOptions) (DeleteResponse, error) {
	resp, err := c.api.TigrisDBDeleteWithResponse(ctx, db, collection, userHTTP.TigrisDBDeleteJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Options: c.convertDeleteOptions(options),
	})
	return nil, HTTPError(err, resp)
}

func (c *httpCRUD) readWithOptions(ctx context.Context, db string, collection string, filter Filter, options *ReadOptions) (Iterator, error) {
	resp, err := c.api.TigrisDBRead(ctx, db, collection, userHTTP.TigrisDBReadJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Options: c.convertReadOptions(options),
	})

	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf(resp.Status)
	}

	dec := json.NewDecoder(resp.Body)

	return &readIterator{streamReader: &httpStreamReader{dec}}, nil
}

type httpStreamReader struct {
	stream *json.Decoder
}

func (g *httpStreamReader) read() (Document, error) {
	var resp userHTTP.ReadResponse
	if err := g.stream.Decode(&resp); err != nil {
		return nil, err
	}

	return Document(resp.Doc), nil
}
