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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"unsafe"

	apiHTTP "github.com/tigrisdata/tigris-client-go/api/client/v1/api"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	"google.golang.org/grpc/codes"
)

const (
	DefaultHTTPPort = 443
)

func HTTPError(err error, resp *http.Response) error {
	if err != nil {
		if err == io.EOF {
			return err
		}
		return &api.TigrisError{Code: codes.Unknown, Message: err.Error()}
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

	if !strings.Contains(resp.Header.Get("Content-Type"), "json") {
		b, _ := ioutil.ReadAll(resp.Body)
		return &api.TigrisError{Code: codes.Code(resp.StatusCode), Message: string(b)}
	}

	var e api.TigrisError
	if err := respDecode(resp.Body, &e); err != nil {
		return err
	}

	return &e
}

type httpDriver struct {
	api *apiHTTP.ClientWithResponses
}

func respDecode(body io.ReadCloser, v interface{}) error {
	defer func() {
		_ = body.Close()
	}()

	if err := json.NewDecoder(body).Decode(v); err != nil {
		return &api.TigrisError{Code: codes.Unknown, Message: err.Error()}
	}
	return nil
}

func setHeaders(_ context.Context, req *http.Request) error {
	req.Header["Host"] = []string{req.Host}
	req.Header["User-Agent"] = []string{UserAgent}
	req.Header["Accept"] = []string{"*/*"}
	return nil
}

// NewHTTPClient return Driver interface implementation using HTTP transport protocol
func NewHTTPClient(ctx context.Context, url string, config *config.Driver) (Driver, error) {
	token, oCfg, ctxClient := getAuthToken(ctx, config)

	if !strings.Contains(url, ":") {
		url = fmt.Sprintf("%s:%d", url, DefaultHTTPPort)
	}

	if !strings.Contains(url, "://") {
		url = "https://" + url
	}

	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: config.TLS}}
	if token.AccessToken != "" || token.RefreshToken != "" {
		hc = oCfg.Client(ctxClient, token)
	}

	c, err := apiHTTP.NewClientWithResponses(url, apiHTTP.WithHTTPClient(hc), apiHTTP.WithRequestEditorFn(setHeaders))
	if err != nil {
		return nil, err
	}

	//	return &driver{driverWithOptions: &httpDriver{httpCRUD: &httpCRUD{api: c}}}, err
	return &driver{driverWithOptions: &httpDriver{api: c}}, nil
}

func (c *httpDriver) Close() error {
	return nil
}

func (c *httpDriver) UseDatabase(name string) Database {
	return &driverCRUD{&httpCRUD{db: name, api: c.api}}
}

func (c *httpDriver) Info(ctx context.Context) (*InfoResponse, error) {
	resp, err := c.api.TigrisGetInfo(ctx)
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	var i InfoResponse
	if err := respDecode(resp.Body, &i); err != nil {
		return nil, err
	}

	return &i, nil
}

func (c *httpDriver) ListDatabases(ctx context.Context) ([]string, error) {
	resp, err := c.api.TigrisListDatabases(ctx)
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	var l api.ListDatabasesResponse
	if err := respDecode(resp.Body, &l); err != nil {
		return nil, err
	}
	if l.Databases == nil {
		return nil, nil
	}

	var databases []string
	for _, nm := range l.Databases {
		databases = append(databases, nm.Db)
	}
	return databases, nil
}

func (c *httpDriver) DescribeDatabase(ctx context.Context, db string) (*DescribeDatabaseResponse, error) {
	resp, err := c.api.TigrisDescribeDatabase(ctx, db, apiHTTP.TigrisDescribeDatabaseJSONRequestBody{})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	var d apiHTTP.DescribeDatabaseResponse
	if err := respDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	var r DescribeDatabaseResponse
	r.Db = ToString(d.Db)
	for _, v := range *d.Collections {
		r.Collections = append(r.Collections, &api.CollectionDescription{
			Collection: ToString(v.Collection),
			Schema:     v.Schema,
		})
	}
	return &r, nil
}

func convertDatabaseOptions(_ *DatabaseOptions) *apiHTTP.DatabaseOptions {
	return &apiHTTP.DatabaseOptions{}
}

func (c *httpCRUD) convertCollectionOptions(_ *CollectionOptions) *apiHTTP.CollectionOptions {
	opts := apiHTTP.CollectionOptions{}
	if c.txCtx.Id != nil && *c.txCtx.Id != "" {
		opts.TxCtx = &apiHTTP.TransactionCtx{Id: c.txCtx.Id, Origin: c.txCtx.Origin}
	}
	return &opts
}

func convertTransactionOptions(_ *TxOptions) *apiHTTP.TransactionOptions {
	return &apiHTTP.TransactionOptions{}
}

func (c *httpDriver) createDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error {
	resp, err := c.api.TigrisCreateDatabase(ctx, db, apiHTTP.TigrisCreateDatabaseJSONRequestBody{
		Options: convertDatabaseOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) dropDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error {
	resp, err := c.api.TigrisDropDatabase(ctx, db, apiHTTP.TigrisDropDatabaseJSONRequestBody{
		Options: convertDatabaseOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpDriver) beginTxWithOptions(ctx context.Context, db string, options *TxOptions) (txWithOptions, error) {
	resp, err := c.api.TigrisBeginTransaction(ctx, db, apiHTTP.TigrisBeginTransactionJSONRequestBody{
		Options: convertTransactionOptions(options),
	})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}

	var bTx apiHTTP.BeginTransactionResponse
	if err = respDecode(resp.Body, &bTx); err != nil {
		return nil, err
	}

	if bTx.TxCtx == nil || bTx.TxCtx.Id == nil || *bTx.TxCtx.Id == "" {
		return nil, HTTPError(fmt.Errorf("empty transaction context in response"), nil)
	}

	return &httpCRUD{db: db, api: c.api, txCtx: *bTx.TxCtx}, nil
}

func (c *httpCRUD) Commit(ctx context.Context) error {
	resp, err := c.api.TigrisCommitTransaction(ctx, c.db, apiHTTP.TigrisCommitTransactionJSONRequestBody{
		TxCtx: &c.txCtx,
	})
	err = HTTPError(err, resp)
	if err == nil {
		c.committed = true
	}
	return err
}

func (c *httpCRUD) Rollback(ctx context.Context) error {
	if c.committed {
		return nil
	}
	resp, err := c.api.TigrisRollbackTransaction(ctx, c.db, apiHTTP.TigrisRollbackTransactionJSONRequestBody{
		TxCtx: &c.txCtx,
	})
	return HTTPError(err, resp)
}

type httpCRUD struct {
	db    string
	api   *apiHTTP.ClientWithResponses
	txCtx apiHTTP.TransactionCtx

	committed bool
}

func (c *httpCRUD) convertWriteOptions(_ *WriteOptions) *apiHTTP.WriteOptions {
	opts := apiHTTP.WriteOptions{}
	if c.txCtx.Id != nil && *c.txCtx.Id != "" {
		opts.TxCtx = &apiHTTP.TransactionCtx{Id: c.txCtx.Id, Origin: c.txCtx.Origin}
	}
	return &opts
}

func (c *httpCRUD) convertInsertOptions(_ *InsertOptions) *apiHTTP.InsertRequestOptions {
	return &apiHTTP.InsertRequestOptions{WriteOptions: c.convertWriteOptions(&WriteOptions{})}
}

func (c *httpCRUD) convertReplaceOptions(_ *ReplaceOptions) *apiHTTP.ReplaceRequestOptions {
	return &apiHTTP.ReplaceRequestOptions{WriteOptions: c.convertWriteOptions(&WriteOptions{})}
}

func (c *httpCRUD) convertUpdateOptions(_ *UpdateOptions) *apiHTTP.UpdateRequestOptions {
	return &apiHTTP.UpdateRequestOptions{WriteOptions: c.convertWriteOptions(&WriteOptions{})}
}

func (c *httpCRUD) convertDeleteOptions(_ *DeleteOptions) *apiHTTP.DeleteRequestOptions {
	return &apiHTTP.DeleteRequestOptions{WriteOptions: c.convertWriteOptions(&WriteOptions{})}
}

func (c *httpCRUD) convertReadOptions(_ *ReadOptions) *apiHTTP.ReadRequestOptions {
	opts := apiHTTP.ReadRequestOptions{}
	if c.txCtx.Id != nil && *c.txCtx.Id != "" {
		opts.TxCtx = &apiHTTP.TransactionCtx{Id: c.txCtx.Id, Origin: c.txCtx.Origin}
	}
	return &opts
}

func (c *httpCRUD) convertStreamOptions(_ *StreamOptions) *apiHTTP.StreamRequestOptions {
	return &apiHTTP.StreamRequestOptions{}
}

func (c *httpCRUD) listCollectionsWithOptions(ctx context.Context, options *CollectionOptions) ([]string, error) {
	resp, err := c.api.TigrisListCollections(ctx, c.db, apiHTTP.TigrisListCollectionsJSONRequestBody{
		Options: c.convertCollectionOptions(options),
	})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	var l apiHTTP.ListCollectionsResponse
	if err := respDecode(resp.Body, &l); err != nil {
		return nil, err
	}
	if l.Collections == nil {
		return nil, nil
	}

	var collections []string
	for _, c := range *l.Collections {
		if c.Collection != nil {
			collections = append(collections, *c.Collection)
		}
	}
	return collections, nil
}

func (c *httpCRUD) describeCollectionWithOptions(ctx context.Context, collection string, _ *CollectionOptions) (*DescribeCollectionResponse, error) {
	resp, err := c.api.TigrisDescribeCollection(ctx, c.db, collection, apiHTTP.TigrisDescribeCollectionJSONRequestBody{})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}
	var d apiHTTP.DescribeCollectionResponse
	if err := respDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	r := &DescribeCollectionResponse{
		Schema:     d.Schema,
		Collection: ToString(d.Collection),
	}

	return r, nil
}

func (c *httpCRUD) createOrUpdateCollectionWithOptions(ctx context.Context, collection string, schema Schema, options *CollectionOptions) error {
	resp, err := c.api.TigrisCreateOrUpdateCollection(ctx, c.db, collection, apiHTTP.TigrisCreateOrUpdateCollectionJSONRequestBody{
		Schema:  json.RawMessage(schema),
		Options: c.convertCollectionOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpCRUD) dropCollectionWithOptions(ctx context.Context, collection string, options *CollectionOptions) error {
	resp, err := c.api.TigrisDropCollection(ctx, c.db, collection, apiHTTP.TigrisDropCollectionJSONRequestBody{
		Options: c.convertCollectionOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpCRUD) insertWithOptions(ctx context.Context, collection string, docs []Document, options *InsertOptions) (*InsertResponse, error) {
	resp, err := c.api.TigrisInsert(ctx, c.db, collection, apiHTTP.TigrisInsertJSONRequestBody{
		Documents: (*[]json.RawMessage)(unsafe.Pointer(&docs)),
		Options:   c.convertInsertOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d InsertResponse
	if err := respDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *httpCRUD) replaceWithOptions(ctx context.Context, collection string, docs []Document, options *ReplaceOptions) (*ReplaceResponse, error) {
	resp, err := c.api.TigrisReplace(ctx, c.db, collection, apiHTTP.TigrisReplaceJSONRequestBody{
		Documents: (*[]json.RawMessage)(unsafe.Pointer(&docs)),
		Options:   c.convertReplaceOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d ReplaceResponse
	if err := respDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *httpCRUD) updateWithOptions(ctx context.Context, collection string, filter Filter, fields Update, options *UpdateOptions) (*UpdateResponse, error) {
	resp, err := c.api.TigrisUpdate(ctx, c.db, collection, apiHTTP.TigrisUpdateJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Fields:  json.RawMessage(fields),
		Options: c.convertUpdateOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d UpdateResponse
	if err := respDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *httpCRUD) deleteWithOptions(ctx context.Context, collection string, filter Filter, options *DeleteOptions) (*DeleteResponse, error) {
	resp, err := c.api.TigrisDelete(ctx, c.db, collection, apiHTTP.TigrisDeleteJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Options: c.convertDeleteOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d DeleteResponse
	if err := respDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *httpCRUD) readWithOptions(ctx context.Context, collection string, filter Filter, fields Projection, options *ReadOptions) (Iterator, error) {
	resp, err := c.api.TigrisRead(ctx, c.db, collection, apiHTTP.TigrisReadJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Fields:  json.RawMessage(fields),
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
	var res struct {
		Result apiHTTP.ReadResponse
	}
	if err := g.stream.Decode(&res); err != nil {
		return nil, HTTPError(err, nil)
	}

	return Document(res.Result.Data), nil
}

func (g *httpStreamReader) close() error {
	return g.closer.Close()
}

func (c *httpCRUD) streamWithOptions(ctx context.Context, collection string, options *StreamOptions) (EventIterator, error) {
	resp, err := c.api.TigrisStream(ctx, c.db, apiHTTP.TigrisStreamJSONRequestBody{
		Collection: &collection,
		Options:    c.convertStreamOptions(options),
	})

	if err != nil {
		return nil, HTTPError(err, nil)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, HTTPError(fmt.Errorf(resp.Status), nil)
	}

	dec := json.NewDecoder(resp.Body)

	return &eventReadIterator{eventStreamReader: &httpEventStreamReader{stream: dec, closer: resp.Body}}, nil
}

type httpEventStreamReader struct {
	closer io.Closer
	stream *json.Decoder
}

func (g *httpEventStreamReader) read() (Event, error) {
	var res struct {
		Result apiHTTP.ReadResponse
	}
	if err := g.stream.Decode(&res); err != nil {
		return nil, HTTPError(err, nil)
	}

	return Event(res.Result.Data), nil
}

func (g *httpEventStreamReader) close() error {
	return g.closer.Close()
}

func ToString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
