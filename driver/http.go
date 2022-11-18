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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unsafe"

	apiHTTP "github.com/tigrisdata/tigris-client-go/api/client/v1/api"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	"golang.org/x/net/context/ctxhttp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	DefaultHTTPSPort   = 443
	DefaultHTTPPort    = 80
	SetCookieHeaderKey = "Set-Cookie"
	CookieHeaderKey    = "Cookie"

	grantTypeRefreshToken      = "refresh_token"
	grantTypeClientCredentials = "client_credentials"
	scope                      = "offline_access openid"
)

type (
	txCtxKey                        struct{}
	additionalOutboundHeadersCtxKey struct{}
)

// HTTPError parses HTTP error into TigrisError
// Returns nil, if HTTP status is OK.
func HTTPError(err error, resp *http.Response) error {
	if err != nil {
		var terr *api.TigrisError
		if errors.As(err, &terr) {
			//		if terr, ok := err.(*api.TigrisError); ok {
			return &Error{TigrisError: terr}
		}

		if errors.Is(err, io.EOF) {
			return err
		}

		return err
	}

	if resp == nil {
		return nil
	}

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	defer func() {
		if resp != nil {
			_ = resp.Body.Close()
		}
	}()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if !strings.Contains(resp.Header.Get("Content-Type"), "json") {
		return &Error{&api.TigrisError{Code: api.Code_UNKNOWN, Message: string(b)}}
	}

	te := api.UnmarshalStatus(b)

	return &Error{TigrisError: te}
}

type httpDriver struct {
	api *apiHTTP.ClientWithResponses

	tokenURL string
	cfg      *config.Driver
}

func respDecode(body io.ReadCloser, v interface{}) error {
	defer func() {
		_ = body.Close()
	}()

	if err := json.NewDecoder(body).Decode(v); err != nil {
		return err
	}
	return nil
}

type metadata struct {
	CreatedAt *time.Time `json:"created_at,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

func newRespMetadata(m *metadata) *api.ResponseMetadata {
	r := &api.ResponseMetadata{}

	if m.CreatedAt != nil {
		r.CreatedAt = timestamppb.New(*m.CreatedAt)
	}

	if m.UpdatedAt != nil {
		r.UpdatedAt = timestamppb.New(*m.UpdatedAt)
	}

	if m.DeletedAt != nil {
		r.DeletedAt = timestamppb.New(*m.DeletedAt)
	}

	return r
}

// convert timestamps from response metadata to timestamppb.Timestamp.
func dmlRespDecode(body io.ReadCloser, v interface{}) error {
	r := struct {
		Metadata      metadata
		Status        string `json:"status,omitempty"`
		ModifiedCount int32  `json:"modified_count,omitempty"`
	}{}

	if err := respDecode(body, &r); err != nil {
		return err
	}

	switch v := v.(type) {
	case *InsertResponse:
		v.Status = r.Status
		v.Metadata = newRespMetadata(&r.Metadata)
	case *ReplaceResponse:
		v.Status = r.Status
		v.Metadata = newRespMetadata(&r.Metadata)
	case *UpdateResponse:
		v.Status = r.Status
		v.ModifiedCount = r.ModifiedCount
		v.Metadata = newRespMetadata(&r.Metadata)
	case *DeleteResponse:
		v.Status = r.Status
		v.Metadata = newRespMetadata(&r.Metadata)
	case *PublishResponse:
		v.Status = r.Status
		v.Metadata = newRespMetadata(&r.Metadata)
	default:
		return fmt.Errorf("unkknown response type")
	}

	return nil
}

func setHeaders(ctx context.Context, req *http.Request) error {
	req.Header["Host"] = []string{req.Host}
	req.Header["User-Agent"] = []string{UserAgent}
	req.Header["Accept"] = []string{"*/*"}

	if v := ctx.Value(txCtxKey{}); v != nil {
		txCtx := v.(*api.TransactionCtx)
		req.Header[api.HeaderTxID] = []string{txCtx.Id}
		req.Header[api.HeaderTxOrigin] = []string{txCtx.Origin}
	}

	if v := ctx.Value(additionalOutboundHeadersCtxKey{}); v != nil {
		cookies := v.([]*http.Cookie)
		for _, cookie := range cookies {
			req.AddCookie(cookie)
		}
	}
	return nil
}

func setHTTPTxCtx(ctx context.Context, txCtx *api.TransactionCtx, cookies []*http.Cookie) context.Context {
	result := ctx

	if txCtx != nil && txCtx.Id != "" {
		result = context.WithValue(result, txCtxKey{}, txCtx)
	}

	if cookies != nil {
		result = context.WithValue(result, additionalOutboundHeadersCtxKey{}, cookies)
	}

	return result
}

// newHTTPClient return Driver interface implementation using HTTP transport protocol.
func newHTTPClient(_ context.Context, config *config.Driver) (*httpDriver, error) {
	if !strings.Contains(config.URL, ":") {
		if config.TLS != nil {
			config.URL = fmt.Sprintf("%s:%d", config.URL, DefaultHTTPSPort)
		} else {
			config.URL = fmt.Sprintf("%s:%d", config.URL, DefaultHTTPPort)
		}
	}

	if config.TLS != nil {
		config.URL = "https://" + config.URL
	} else {
		config.URL = "http://" + config.URL
	}

	_, httpClient, tokenURL := configAuth(config)

	if httpClient == nil {
		httpClient = &http.Client{Transport: &http.Transport{TLSClientConfig: config.TLS}}
	}

	c, err := apiHTTP.NewClientWithResponses(config.URL, apiHTTP.WithHTTPClient(httpClient), apiHTTP.WithRequestEditorFn(setHeaders))
	if err != nil {
		return nil, err
	}
	return &httpDriver{api: c, tokenURL: tokenURL, cfg: config}, nil
}

func (c *httpDriver) Close() error {
	return nil
}

func (c *httpDriver) UseDatabase(name string) Database {
	return &driverCRUD{&httpCRUD{db: name, api: c.api}}
}

func (c *httpDriver) Info(ctx context.Context) (*InfoResponse, error) {
	resp, err := c.api.ObservabilityGetInfo(ctx)
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}

	var i InfoResponse

	if err := respDecode(resp.Body, &i); err != nil {
		return nil, err
	}

	return &i, nil
}

func (c *httpDriver) Health(ctx context.Context) (*HealthResponse, error) {
	resp, err := c.api.HealthAPIHealth(ctx)
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}

	var i HealthResponse

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

	databases := make([]string, 0, len(l.Databases))

	for _, nm := range l.Databases {
		databases = append(databases, nm.Db)
	}

	return databases, nil
}

func (c *httpDriver) describeDatabaseWithOptions(ctx context.Context, db string, options *DescribeDatabaseOptions) (*DescribeDatabaseResponse, error) {
	resp, err := c.api.TigrisDescribeDatabase(ctx, db, apiHTTP.TigrisDescribeDatabaseJSONRequestBody{
		SchemaFormat: &options.SchemaFormat,
	})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d apiHTTP.DescribeDatabaseResponse

	if err := respDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	var r DescribeDatabaseResponse

	r.Db = PtrToString(d.Db)
	r.Size = PtrToInt64(d.Size)

	if d.Collections == nil {
		return &r, nil
	}

	for _, v := range *d.Collections {
		r.Collections = append(r.Collections, &api.CollectionDescription{
			Collection: PtrToString(v.Collection),
			Schema:     v.Schema,
			Size:       PtrToInt64(v.Size),
		})
	}

	return &r, nil
}

func convertDatabaseOptions(_ *DatabaseOptions) *apiHTTP.DatabaseOptions {
	return &apiHTTP.DatabaseOptions{}
}

func (c *httpCRUD) convertCollectionOptions(_ *CollectionOptions) *apiHTTP.CollectionOptions {
	return &apiHTTP.CollectionOptions{}
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
	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var bTx apiHTTP.BeginTransactionResponse

	if err = respDecode(resp.Body, &bTx); err != nil {
		return nil, err
	}

	if bTx.TxCtx == nil || bTx.TxCtx.Id == nil || *bTx.TxCtx.Id == "" {
		return nil, HTTPError(fmt.Errorf("empty transaction context in response"), nil)
	}

	var outboundCookies []*http.Cookie

	outboundCookies = append(outboundCookies, resp.Cookies()...)
	return &httpCRUD{db: db, api: c.api, txCtx: &api.TransactionCtx{Id: PtrToString(bTx.TxCtx.Id), Origin: PtrToString(bTx.TxCtx.Origin)}, cookies: outboundCookies}, nil
}

func (c *httpCRUD) Commit(ctx context.Context) error {
	ctx = setHTTPTxCtx(ctx, c.txCtx, c.cookies)

	resp, err := c.api.TigrisCommitTransaction(ctx, c.db, apiHTTP.TigrisCommitTransactionJSONRequestBody{})

	if err = HTTPError(err, resp); err == nil {
		c.committed = true
	}

	return err
}

func (c *httpCRUD) Rollback(ctx context.Context) error {
	ctx = setHTTPTxCtx(ctx, c.txCtx, c.cookies)

	if c.committed {
		return nil
	}

	resp, err := c.api.TigrisRollbackTransaction(ctx, c.db, apiHTTP.TigrisRollbackTransactionJSONRequestBody{})

	return HTTPError(err, resp)
}

type httpCRUD struct {
	db      string
	api     *apiHTTP.ClientWithResponses
	txCtx   *api.TransactionCtx
	cookies []*http.Cookie

	committed bool
}

func (c *httpCRUD) convertWriteOptions(o *api.WriteOptions) *apiHTTP.WriteOptions {
	if o != nil {
		return &apiHTTP.WriteOptions{}
	}
	return nil
}

func (c *httpCRUD) convertInsertOptions(o *InsertOptions) *apiHTTP.InsertRequestOptions {
	return &apiHTTP.InsertRequestOptions{WriteOptions: c.convertWriteOptions(o.WriteOptions)}
}

func (c *httpCRUD) convertReplaceOptions(o *ReplaceOptions) *apiHTTP.ReplaceRequestOptions {
	return &apiHTTP.ReplaceRequestOptions{WriteOptions: c.convertWriteOptions(o.WriteOptions)}
}

func (c *httpCRUD) convertUpdateOptions(o *UpdateOptions) *apiHTTP.UpdateRequestOptions {
	return &apiHTTP.UpdateRequestOptions{WriteOptions: c.convertWriteOptions(o.WriteOptions)}
}

func (c *httpCRUD) convertDeleteOptions(o *DeleteOptions) *apiHTTP.DeleteRequestOptions {
	return &apiHTTP.DeleteRequestOptions{WriteOptions: c.convertWriteOptions(o.WriteOptions)}
}

func (c *httpCRUD) convertReadOptions(i *ReadOptions) *apiHTTP.ReadRequestOptions {
	opts := apiHTTP.ReadRequestOptions{
		Skip:   &i.Skip,
		Limit:  &i.Limit,
		Offset: &i.Offset,
	}
	if i.Collation != nil {
		opts.Collation = &apiHTTP.Collation{Case: &i.Collation.Case}
	}
	return &opts
}

func (c *httpCRUD) convertPublishOptions(o *PublishOptions) *apiHTTP.PublishRequestOptions {
	return &apiHTTP.PublishRequestOptions{Partition: o.Partition}
}

func (c *httpCRUD) convertSubscribeOptions(o *SubscribeOptions) *apiHTTP.SubscribeRequestOptions {
	return &apiHTTP.SubscribeRequestOptions{Partitions: &o.Partitions}
}

func (c *httpCRUD) listCollectionsWithOptions(ctx context.Context, options *CollectionOptions) ([]string, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx, c.cookies)

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

	collections := make([]string, 0, len(*l.Collections))

	for _, c := range *l.Collections {
		if c.Collection != nil {
			collections = append(collections, *c.Collection)
		}
	}

	return collections, nil
}

func (c *httpCRUD) describeCollectionWithOptions(ctx context.Context, collection string, options *DescribeCollectionOptions) (*DescribeCollectionResponse, error) {
	resp, err := c.api.TigrisDescribeCollection(ctx, c.db, collection, apiHTTP.TigrisDescribeCollectionJSONRequestBody{
		SchemaFormat: &options.SchemaFormat,
	})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d apiHTTP.DescribeCollectionResponse

	if err := respDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	r := &DescribeCollectionResponse{
		Schema:     d.Schema,
		Collection: PtrToString(d.Collection),
		Size:       PtrToInt64(d.Size),
	}

	return r, nil
}

func (c *httpCRUD) createOrUpdateCollectionWithOptions(ctx context.Context, collection string, schema Schema, options *CreateCollectionOptions) error {
	ctx = setHTTPTxCtx(ctx, c.txCtx, c.cookies)

	resp, err := c.api.TigrisCreateOrUpdateCollection(ctx, c.db, collection, apiHTTP.TigrisCreateOrUpdateCollectionJSONRequestBody{
		Schema:     json.RawMessage(schema),
		OnlyCreate: &options.OnlyCreate,
		Options:    c.convertCollectionOptions(nil),
	})
	return HTTPError(err, resp)
}

func (c *httpCRUD) dropCollectionWithOptions(ctx context.Context, collection string, options *CollectionOptions) error {
	ctx = setHTTPTxCtx(ctx, c.txCtx, c.cookies)

	resp, err := c.api.TigrisDropCollection(ctx, c.db, collection, apiHTTP.TigrisDropCollectionJSONRequestBody{
		Options: c.convertCollectionOptions(options),
	})
	return HTTPError(err, resp)
}

func (c *httpCRUD) insertWithOptions(ctx context.Context, collection string, docs []Document, options *InsertOptions) (*InsertResponse, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx, c.cookies)

	resp, err := c.api.TigrisInsert(ctx, c.db, collection, apiHTTP.TigrisInsertJSONRequestBody{
		Documents: (*[]json.RawMessage)(unsafe.Pointer(&docs)),
		Options:   c.convertInsertOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d InsertResponse
	if err := dmlRespDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *httpCRUD) replaceWithOptions(ctx context.Context, collection string, docs []Document, options *ReplaceOptions) (*ReplaceResponse, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx, c.cookies)

	resp, err := c.api.TigrisReplace(ctx, c.db, collection, apiHTTP.TigrisReplaceJSONRequestBody{
		Documents: (*[]json.RawMessage)(unsafe.Pointer(&docs)),
		Options:   c.convertReplaceOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d ReplaceResponse
	if err := dmlRespDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *httpCRUD) updateWithOptions(ctx context.Context, collection string, filter Filter, fields Update, options *UpdateOptions) (*UpdateResponse, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx, c.cookies)

	resp, err := c.api.TigrisUpdate(ctx, c.db, collection, apiHTTP.TigrisUpdateJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Fields:  json.RawMessage(fields),
		Options: c.convertUpdateOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d UpdateResponse
	if err := dmlRespDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *httpCRUD) deleteWithOptions(ctx context.Context, collection string, filter Filter, options *DeleteOptions) (*DeleteResponse, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx, c.cookies)

	resp, err := c.api.TigrisDelete(ctx, c.db, collection, apiHTTP.TigrisDeleteJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Options: c.convertDeleteOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d DeleteResponse
	if err := dmlRespDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *httpCRUD) readWithOptions(ctx context.Context, collection string, filter Filter, fields Projection, options *ReadOptions) (Iterator, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx, c.cookies)

	resp, err := c.api.TigrisRead(ctx, c.db, collection, apiHTTP.TigrisReadJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Fields:  json.RawMessage(fields),
		Options: c.convertReadOptions(options),
	})

	err = HTTPError(err, resp)

	e := &readIterator{err: err, eof: err != nil}

	if err == nil {
		e.streamReader = &httpStreamReader{stream: json.NewDecoder(resp.Body), closer: resp.Body}
	}

	return e, nil
}

type httpStreamReader struct {
	closer io.Closer
	stream *json.Decoder
}

func (g *httpStreamReader) read() (Document, error) {
	var res struct {
		Result *apiHTTP.ReadResponse
		Error  *api.ErrorDetails
	}

	if err := g.stream.Decode(&res); err != nil {
		return nil, HTTPError(err, nil)
	}

	if res.Error != nil {
		return nil, &Error{TigrisError: api.FromErrorDetails(res.Error)}
	}

	return Document(res.Result.Data), nil
}

func (g *httpStreamReader) close() error {
	return g.closer.Close()
}

func (c *httpCRUD) search(ctx context.Context, collection string, req *SearchRequest) (SearchResultIterator, error) {
	if req.SearchFields == nil {
		req.SearchFields = []string{}
	}

	if req.IncludeFields == nil {
		req.IncludeFields = []string{}
	}

	if req.ExcludeFields == nil {
		req.ExcludeFields = []string{}
	}

	resp, err := c.api.TigrisSearch(ctx, c.db, collection, apiHTTP.TigrisSearchJSONRequestBody{
		Q:             &req.Q,
		SearchFields:  &req.SearchFields,
		Filter:        json.RawMessage(req.Filter),
		Facet:         json.RawMessage(req.Facet),
		Sort:          json.RawMessage(req.Sort),
		IncludeFields: &req.IncludeFields,
		ExcludeFields: &req.ExcludeFields,
		Page:          &req.Page,
		PageSize:      &req.PageSize,
	})

	err = HTTPError(err, resp)

	e := &searchResultIterator{err: err, eof: err != nil}

	if err == nil {
		e.searchStreamReader = &httpSearchReader{stream: json.NewDecoder(resp.Body), closer: resp.Body}
	}

	return e, nil
}

type httpSearchReader struct {
	closer io.Closer
	stream *json.Decoder
}

func (g *httpSearchReader) read() (SearchResponse, error) {
	var res struct {
		Result struct {
			Hits   []*api.SearchHit
			Facets map[string]*api.SearchFacet
			Meta   *api.SearchMetadata
		}
		Error *api.ErrorDetails
	}

	if err := g.stream.Decode(&res); err != nil {
		return nil, HTTPError(err, nil)
	}

	if res.Error != nil {
		return nil, &Error{TigrisError: api.FromErrorDetails(res.Error)}
	}

	return &api.SearchResponse{
		Hits:   res.Result.Hits,
		Facets: res.Result.Facets,
		Meta:   res.Result.Meta,
	}, nil
}

func (g *httpSearchReader) close() error {
	return g.closer.Close()
}

func (c *httpDriver) CreateApplication(ctx context.Context, project string, name string, description string) (*Application, error) {
	resp, err := c.api.ManagementCreateApplication(ctx, apiHTTP.ManagementCreateApplicationJSONRequestBody{Name: &name, Description: &description, Project: &project})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}

	var app struct {
		CreatedApplication Application `json:"created_application"`
	}

	if err := respDecode(resp.Body, &app); err != nil {
		return nil, err
	}

	return &app.CreatedApplication, nil
}

func (c *httpDriver) DeleteApplication(ctx context.Context, id string) error {
	resp, err := c.api.ManagementDeleteApplication(ctx, apiHTTP.ManagementDeleteApplicationJSONRequestBody{Id: &id})

	return HTTPError(err, resp)
}

func (c *httpDriver) UpdateApplication(ctx context.Context, id string, name string, description string) (*Application, error) {
	resp, err := c.api.ManagementUpdateApplication(ctx, apiHTTP.ManagementUpdateApplicationJSONRequestBody{Id: &id, Name: &name, Description: &description})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}

	var app struct {
		UpdatedApplication Application `json:"updated_application"`
	}

	if err := respDecode(resp.Body, &app); err != nil {
		return nil, err
	}

	return &app.UpdatedApplication, nil
}

func (c *httpDriver) ListApplications(ctx context.Context, project string) ([]*Application, error) {
	resp, err := c.api.ManagementListApplications(ctx, apiHTTP.ManagementListApplicationsJSONRequestBody{Project: &project})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}

	var apps struct {
		Applications []*Application
	}

	if err := respDecode(resp.Body, &apps); err != nil {
		return nil, err
	}

	return apps.Applications, nil
}

func (c *httpDriver) RotateClientSecret(ctx context.Context, id string) (*Application, error) {
	resp, err := c.api.ManagementRotateApplicationSecret(ctx, apiHTTP.ManagementRotateApplicationSecretJSONRequestBody{Id: &id})
	if err := HTTPError(err, resp); err != nil {
		return nil, err
	}

	var app struct {
		Application Application
	}

	if err := respDecode(resp.Body, &app); err != nil {
		return nil, err
	}

	return &app.Application, nil
}

func (c *httpDriver) GetAccessToken(ctx context.Context, clientID string, clientSecret string, refreshToken string) (*TokenResponse, error) {
	return getAccessToken(ctx, c.tokenURL, c.cfg, clientID, clientSecret, refreshToken)
}

func getAccessToken(ctx context.Context, tokenURL string, cfg *config.Driver, clientID string, clientSecret string, refreshToken string) (*TokenResponse, error) {
	data := url.Values{
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"grant_type":    {grantTypeClientCredentials},
		"scope":         {scope},
	}
	if refreshToken != "" {
		data = url.Values{
			"refresh_token": {refreshToken},
			"client_id":     {clientID},
			"grant_type":    {grantTypeRefreshToken},
			"scope":         {scope},
		}
	}

	t, ok := ctx.Deadline()
	if !ok {
		t = time.Now().Add(tokenRequestTimeout)
	}

	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: cfg.TLS,
		},
		Timeout: time.Until(t),
	}

	resp, err := ctxhttp.PostForm(ctx, &client, tokenURL, data)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "failed to get access token: reason = %s", err.Error())
	}

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "failed to get access token: reason = %s", err.Error())
	}

	if resp.StatusCode != http.StatusOK {
		return nil, api.Errorf(api.Code_INTERNAL, "failed to get access token: reason = %s", string(body))
	}

	var tr TokenResponse

	err = json.Unmarshal(body, &tr)
	if err != nil {
		return nil, api.Errorf(api.Code_INTERNAL, "failed to parse external response: reason = %s", err.Error())
	}

	return &tr, nil
}

func (c *httpDriver) CreateNamespace(ctx context.Context, name string) error {
	resp, err := c.api.ManagementCreateNamespace(ctx, apiHTTP.ManagementCreateNamespaceJSONRequestBody{Name: &name})
	if err := HTTPError(err, resp); err != nil {
		return err
	}

	return nil
}

func (c *httpDriver) ListNamespaces(ctx context.Context) ([]*Namespace, error) {
	resp, err := c.api.ManagementListNamespaces(ctx)
	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var nss struct {
		Namespaces []*Namespace
	}

	if err = respDecode(resp.Body, &nss); err != nil {
		return nil, err
	}

	return nss.Namespaces, nil
}

func (c *httpDriver) QuotaLimits(ctx context.Context) (*QuotaLimits, error) {
	resp, err := c.api.ObservabilityQuotaLimits(ctx, apiHTTP.ObservabilityQuotaLimitsJSONRequestBody{})
	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var limits QuotaLimits
	if err = respDecode(resp.Body, &limits); err != nil {
		return nil, err
	}

	return &limits, nil
}

func (c *httpDriver) QuotaUsage(ctx context.Context) (*QuotaUsage, error) {
	resp, err := c.api.ObservabilityQuotaUsage(ctx, apiHTTP.ObservabilityQuotaUsageJSONRequestBody{})
	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var usage QuotaUsage
	if err = respDecode(resp.Body, &usage); err != nil {
		return nil, err
	}

	return &usage, nil
}

func (c *httpCRUD) publishWithOptions(ctx context.Context, collection string, msgs []Message, options *PublishOptions) (*PublishResponse, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx, c.cookies)

	resp, err := c.api.TigrisPublish(ctx, c.db, collection, apiHTTP.TigrisPublishJSONRequestBody{
		Messages: (*[]json.RawMessage)(unsafe.Pointer(&msgs)),
		Options:  c.convertPublishOptions(options),
	})

	if err = HTTPError(err, resp); err != nil {
		return nil, err
	}

	var d PublishResponse
	if err := dmlRespDecode(resp.Body, &d); err != nil {
		return nil, err
	}

	return &d, nil
}

type subscribeStreamReader struct {
	closer io.Closer
	stream *json.Decoder
}

func (g *subscribeStreamReader) read() (Document, error) {
	var res struct {
		Result *apiHTTP.SubscribeResponse
		Error  *api.ErrorDetails
	}

	if err := g.stream.Decode(&res); err != nil {
		return nil, HTTPError(err, nil)
	}

	if res.Error != nil {
		return nil, &Error{TigrisError: api.FromErrorDetails(res.Error)}
	}

	return Document(res.Result.Message), nil
}

func (g *subscribeStreamReader) close() error {
	return g.closer.Close()
}

func (c *httpCRUD) subscribeWithOptions(ctx context.Context, collection string, filter Filter, options *SubscribeOptions) (Iterator, error) {
	ctx = setHTTPTxCtx(ctx, c.txCtx, c.cookies)

	resp, err := c.api.TigrisSubscribe(ctx, c.db, collection, apiHTTP.TigrisSubscribeJSONRequestBody{
		Filter:  json.RawMessage(filter),
		Options: c.convertSubscribeOptions(options),
	})

	err = HTTPError(err, resp)

	e := &readIterator{err: err, eof: err != nil}

	if err == nil {
		e.streamReader = &subscribeStreamReader{stream: json.NewDecoder(resp.Body), closer: resp.Body}
	}

	return e, nil
}
