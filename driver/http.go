package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unsafe"

	userHTTP "github.com/tigrisdata/tigrisdb-api/client/v1/user"
	api "github.com/tigrisdata/tigrisdb-api/server/v1"
	"google.golang.org/grpc/codes"
)

const (
	DEFAULT_HTTP_PORT = 443
)

func HTTPError(err error, resp *http.Response) error {
	if err != nil {
		return &api.TigrisDBError{Code: codes.Unknown, Message: err.Error()}
	}

	if resp == nil {
		return nil
	}

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return nil
	}

	defer func() {
		if resp != nil {
			_ = resp.Body.Close()
		}
	}()

	var e api.TigrisDBError
	if err := respDecode(resp.Body, &e); err != nil {
		return err
	}

	return &e
}

type httpDriver struct {
	*httpCRUD
}

func respDecode(body io.ReadCloser, v interface{}) error {
	defer func() {
		_ = body.Close()
	}()

	if err := json.NewDecoder(body).Decode(v); err != nil {
		return &api.TigrisDBError{Code: codes.Unknown, Message: err.Error()}
	}
	return nil
}

func NewHTTPClient(ctx context.Context, url string, config *Config) (Driver, error) {
	token, oCfg, ctxClient := getAuthToken(ctx, config)

	if !strings.Contains(url, ":") {
		url = fmt.Sprintf("%s:%d", url, DEFAULT_HTTP_PORT)
	}

	hc := oCfg.Client(ctxClient, token)
	c, err := userHTTP.NewClientWithResponses(url, userHTTP.WithHTTPClient(hc))

	return &driver{driverWithOptions: &httpDriver{httpCRUD: &httpCRUD{api: c}}}, err
}

func (c *httpDriver) Close() error {
	return nil
}

func (c *httpDriver) ListDatabases(ctx context.Context) ([]string, error) {
	resp, err := c.api.TigrisDBListDatabases(ctx)
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	var l userHTTP.ListDatabasesResponse
	if err := respDecode(resp.Body, &l); err != nil {
		return nil, err
	}
	return *l.Dbs, nil
}

func (c *httpDriver) ListCollections(ctx context.Context, db string) ([]string, error) {
	resp, err := c.api.TigrisDBListCollections(ctx, db)
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	var l userHTTP.ListCollectionsResponse
	if err := respDecode(resp.Body, &l); err != nil {
		return nil, err
	}
	return *l.Collections, nil
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
	resp, err := c.api.TigrisDBCreateDatabase(ctx, db, userHTTP.TigrisDBCreateDatabaseJSONRequestBody{
		Db:      &db,
		Options: convertDatabaseOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) dropDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error {
	resp, err := c.api.TigrisDBDropDatabase(ctx, db, userHTTP.TigrisDBDropDatabaseJSONRequestBody{
		Db:      &db,
		Options: convertDatabaseOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) createCollectionWithOptions(ctx context.Context, db string, collection string, schema Schema, options *CollectionOptions) error {
	resp, err := c.api.TigrisDBCreateCollection(ctx, db, collection, userHTTP.TigrisDBCreateCollectionJSONRequestBody{
		Schema:  json.RawMessage(schema),
		Options: convertCollectionOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) alterCollectionWithOptions(ctx context.Context, db string, collection string, schema Schema, options *CollectionOptions) error {
	resp, err := c.api.TigrisDBAlterCollection(ctx, db, collection, userHTTP.TigrisDBAlterCollectionJSONRequestBody{
		Schema:  json.RawMessage(schema),
		Options: convertCollectionOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) dropCollectionWithOptions(ctx context.Context, db string, collection string, _ *CollectionOptions) error {
	resp, err := c.api.TigrisDBDropCollection(ctx, db, collection)
	return HTTPError(err, resp)
}

func (c *httpDriver) beginTxWithOptions(ctx context.Context, db string, options *TxOptions) (txWithOptions, error) {
	resp, err := c.api.TigrisDBBeginTransaction(ctx, db, userHTTP.TigrisDBBeginTransactionJSONRequestBody{
		Options: convertTransactionOptions(options),
	})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}

	var bTx userHTTP.BeginTransactionResponse
	if err = respDecode(resp.Body, &bTx); err != nil {
		return nil, err
	}

	if bTx.TxCtx == nil || bTx.TxCtx.Id == nil || *bTx.TxCtx.Id == "" {
		return nil, HTTPError(fmt.Errorf("empty transaction context in response"), nil)
	}

	return &httpTx{db: db, httpCRUD: &httpCRUD{api: c.api, txCtx: *bTx.TxCtx}}, nil
}

type httpTx struct {
	db string
	*httpCRUD
}

func (c *httpTx) Commit(ctx context.Context) error {
	resp, err := c.api.TigrisDBCommitTransaction(ctx, c.db, userHTTP.TigrisDBCommitTransactionJSONRequestBody{
		TxCtx: &c.txCtx,
	})
	return HTTPError(err, resp)
}

func (c *httpTx) Rollback(ctx context.Context) error {
	resp, err := c.api.TigrisDBRollbackTransaction(ctx, c.db, userHTTP.TigrisDBRollbackTransactionJSONRequestBody{
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
	resp, err := c.api.TigrisDBInsert(ctx, db, collection, userHTTP.TigrisDBInsertJSONRequestBody{
		Documents: (*[]json.RawMessage)(unsafe.Pointer(&docs)),
		Options:   c.convertInsertOptions(options),
	})
	return nil, HTTPError(err, resp)
}

func (c *httpCRUD) updateWithOptions(ctx context.Context, db string, collection string, filter Filter, fields Fields, options *UpdateOptions) (UpdateResponse, error) {
	resp, err := c.api.TigrisDBUpdate(ctx, db, collection, userHTTP.TigrisDBUpdateJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Fields:  json.RawMessage(fields),
		Options: c.convertUpdateOptions(options),
	})
	return nil, HTTPError(err, resp)
}

func (c *httpCRUD) deleteWithOptions(ctx context.Context, db string, collection string, filter Filter, options *DeleteOptions) (DeleteResponse, error) {
	resp, err := c.api.TigrisDBDelete(ctx, db, collection, userHTTP.TigrisDBDeleteJSONRequestBody{
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
		return nil, HTTPError(err, nil)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, HTTPError(fmt.Errorf(resp.Status), nil)
	}

	dec := json.NewDecoder(resp.Body)

	return &readIterator{streamReader: &httpStreamReader{stream: dec, closer: resp.Body}}, nil
}

type httpStreamReader struct {
	closer io.Closer
	stream *json.Decoder
}

func (g *httpStreamReader) read() (Document, error) {
	var resp userHTTP.ReadResponse
	if err := g.stream.Decode(&resp); err != nil {
		return nil, HTTPError(err, nil)
	}

	return Document(resp.Doc), nil
}

func (g *httpStreamReader) close() error {
	return g.closer.Close()
}
