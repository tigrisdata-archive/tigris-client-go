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
	"errors"
	"fmt"
	"io"
	"strings"
	"unsafe"

	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
	meta "google.golang.org/grpc/metadata"
)

const (
	DefaultGRPCPort = 443
)

type grpcDriver struct {
	api    api.TigrisClient
	mgmt   api.ManagementClient
	auth   api.AuthClient
	o11y   api.ObservabilityClient
	health api.HealthAPIClient
	conn   *grpc.ClientConn
	cfg    *config.Driver
}

func GRPCError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, io.EOF) {
		return err
	}

	return &Error{api.FromStatusError(err)}
}

// newGRPCClient return Driver interface implementation using GRPC transport protocol.
func newGRPCClient(ctx context.Context, config *config.Driver) (*grpcDriver, error) {
	if !strings.Contains(config.URL, ":") {
		config.URL = fmt.Sprintf("%s:%d", config.URL, DefaultGRPCPort)
	}

	tokenSource, _, _ := configAuth(config)

	opts := []grpc.DialOption{
		grpc.FailOnNonTempDialError(true),
		grpc.WithReturnConnectionError(),
		grpc.WithUserAgent(UserAgent),
		grpc.WithBlock(),
	}

	if config.TLS != nil || tokenSource != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(config.TLS)))

		if tokenSource != nil {
			opts = append(opts, grpc.WithPerRPCCredentials(oauth.TokenSource{TokenSource: tokenSource}))
		}
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.DialContext(ctx, config.URL, opts...)
	if err != nil {
		return nil, GRPCError(err)
	}

	return &grpcDriver{
		conn:   conn,
		api:    api.NewTigrisClient(conn),
		mgmt:   api.NewManagementClient(conn),
		auth:   api.NewAuthClient(conn),
		o11y:   api.NewObservabilityClient(conn),
		health: api.NewHealthAPIClient(conn),
		cfg:    config,
	}, nil
}

func (c *grpcDriver) Close() error {
	if c.conn == nil {
		return nil
	}
	return GRPCError(c.conn.Close())
}

func (c *grpcDriver) UseDatabase(project string) Database {
	return &driverCRUD{&grpcCRUD{db: project, api: c.api}}
}

func (c *grpcDriver) ListProjects(ctx context.Context) ([]string, error) {
	r, err := c.api.ListProjects(ctx, &api.ListProjectsRequest{})
	if err != nil {
		return nil, GRPCError(err)
	}

	projects := make([]string, 0, len(r.GetProjects()))
	for _, c := range r.GetProjects() {
		projects = append(projects, c.GetProject())
	}

	return projects, nil
}

func (c *grpcDriver) Info(ctx context.Context) (*InfoResponse, error) {
	r, err := c.o11y.GetInfo(ctx, &api.GetInfoRequest{})
	if err != nil {
		return nil, GRPCError(err)
	}

	return (*InfoResponse)(r), nil
}

func (c *grpcDriver) Health(ctx context.Context) (*HealthResponse, error) {
	r, err := c.health.Health(ctx, &api.HealthCheckInput{})
	if err != nil {
		return nil, GRPCError(err)
	}
	return (*HealthResponse)(r), nil
}

func (c *grpcDriver) createProjectWithOptions(ctx context.Context, project string, _ *CreateProjectOptions) (*CreateProjectResponse, error) {
	r, err := c.api.CreateProject(ctx, &api.CreateProjectRequest{
		Project: project,
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return (*CreateProjectResponse)(r), nil
}

func (c *grpcDriver) describeProjectWithOptions(ctx context.Context, project string, options *DescribeProjectOptions) (*DescribeDatabaseResponse, error) {
	r, err := c.api.DescribeDatabase(ctx, &api.DescribeDatabaseRequest{
		Project:      project,
		SchemaFormat: options.SchemaFormat,
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return (*DescribeDatabaseResponse)(r), nil
}

func (c *grpcDriver) deleteProjectWithOptions(ctx context.Context, project string, options *DeleteProjectOptions) (*DeleteProjectResponse, error) {
	r, err := c.api.DeleteProject(ctx, &api.DeleteProjectRequest{
		Project: project,
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return (*DeleteProjectResponse)(r), nil
}

func setGRPCTxCtx(ctx context.Context, txCtx *api.TransactionCtx, additionalMetadata meta.MD) context.Context {
	if txCtx == nil || txCtx.Id == "" {
		return ctx
	}

	outgoingMd := meta.Pairs(api.HeaderTxID, txCtx.Id, api.HeaderTxOrigin, txCtx.Origin)

	if additionalMetadata != nil {
		outgoingMd = meta.Join(outgoingMd, additionalMetadata)
	}

	return meta.NewOutgoingContext(ctx, outgoingMd)
}

func (c *grpcCRUD) Commit(ctx context.Context) error {
	ctx = setGRPCTxCtx(ctx, c.txCtx, c.additionalMetadata)

	_, err := c.api.CommitTransaction(ctx, &api.CommitTransactionRequest{
		Project: c.db,
	})

	if err = GRPCError(err); err == nil {
		c.committed = true
	}

	return err
}

func (c *grpcCRUD) Rollback(ctx context.Context) error {
	ctx = setGRPCTxCtx(ctx, c.txCtx, c.additionalMetadata)

	if c.committed {
		return nil
	}

	_, err := c.api.RollbackTransaction(ctx, &api.RollbackTransactionRequest{
		Project: c.db,
	})

	return GRPCError(err)
}

type grpcCRUD struct {
	db                 string
	api                api.TigrisClient
	txCtx              *api.TransactionCtx
	additionalMetadata meta.MD

	committed bool
}

func (c *grpcCRUD) beginTxWithOptions(ctx context.Context, options *TxOptions) (txWithOptions, error) {
	var respHeaders meta.MD // variable to store header and trailer

	resp, err := c.api.BeginTransaction(ctx, &api.BeginTransactionRequest{
		Project: c.db,
		Options: (*api.TransactionOptions)(options),
	}, grpc.Header(&respHeaders))
	if err != nil {
		return nil, GRPCError(err)
	}

	if resp.GetTxCtx() == nil {
		return nil, GRPCError(fmt.Errorf("empty transaction context in response"))
	}

	additionalHeaders := meta.New(map[string]string{})

	if respHeaders.Get(SetCookieHeaderKey) != nil {
		for _, incomingCookie := range respHeaders.Get(SetCookieHeaderKey) {
			additionalHeaders = meta.Join(additionalHeaders, meta.Pairs(CookieHeaderKey, incomingCookie))
		}
	}

	return &grpcCRUD{db: c.db, api: c.api, txCtx: resp.GetTxCtx(), additionalMetadata: additionalHeaders}, nil
}

func (c *grpcCRUD) listCollectionsWithOptions(ctx context.Context, _ *CollectionOptions) ([]string, error) {
	ctx = setGRPCTxCtx(ctx, c.txCtx, c.additionalMetadata)

	r, err := c.api.ListCollections(ctx, &api.ListCollectionsRequest{
		Project: c.db,
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	collections := make([]string, 0, len(r.GetCollections()))
	for _, c := range r.GetCollections() {
		collections = append(collections, c.GetCollection())
	}

	return collections, nil
}

func (c *grpcCRUD) describeCollectionWithOptions(ctx context.Context, collection string, options *DescribeCollectionOptions) (*DescribeCollectionResponse, error) {
	r, err := c.api.DescribeCollection(ctx, &api.DescribeCollectionRequest{
		Project:      c.db,
		Collection:   collection,
		SchemaFormat: options.SchemaFormat,
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return (*DescribeCollectionResponse)(r), nil
}

func (c *grpcCRUD) createOrUpdateCollectionWithOptions(ctx context.Context, collection string, schema Schema, options *CreateCollectionOptions) error {
	ctx = setGRPCTxCtx(ctx, c.txCtx, c.additionalMetadata)

	_, err := c.api.CreateOrUpdateCollection(ctx, &api.CreateOrUpdateCollectionRequest{
		Project:    c.db,
		Collection: collection,
		Schema:     schema,
		OnlyCreate: options.OnlyCreate,
		Options:    &api.CollectionOptions{},
	})
	return GRPCError(err)
}

func (c *grpcCRUD) dropCollectionWithOptions(ctx context.Context, collection string, options *CollectionOptions) error {
	ctx = setGRPCTxCtx(ctx, c.txCtx, c.additionalMetadata)
	_, err := c.api.DropCollection(ctx, &api.DropCollectionRequest{
		Project:    c.db,
		Collection: collection,
		Options:    (*api.CollectionOptions)(options),
	})
	return GRPCError(err)
}

func (c *grpcCRUD) insertWithOptions(ctx context.Context, collection string, docs []Document, options *InsertOptions) (*InsertResponse, error) {
	ctx = setGRPCTxCtx(ctx, c.txCtx, c.additionalMetadata)

	resp, err := c.api.Insert(ctx, &api.InsertRequest{
		Project:    c.db,
		Collection: collection,
		Documents:  *(*[][]byte)(unsafe.Pointer(&docs)),
		Options:    (*api.InsertRequestOptions)(options),
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return (*InsertResponse)(resp), nil
}

func (c *grpcCRUD) replaceWithOptions(ctx context.Context, collection string, docs []Document, options *ReplaceOptions) (*ReplaceResponse, error) {
	ctx = setGRPCTxCtx(ctx, c.txCtx, c.additionalMetadata)

	resp, err := c.api.Replace(ctx, &api.ReplaceRequest{
		Project:    c.db,
		Collection: collection,
		Documents:  *(*[][]byte)(unsafe.Pointer(&docs)),
		Options:    (*api.ReplaceRequestOptions)(options),
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return (*ReplaceResponse)(resp), nil
}

func (c *grpcCRUD) updateWithOptions(ctx context.Context, collection string, filter Filter, fields Update, options *UpdateOptions) (*UpdateResponse, error) {
	ctx = setGRPCTxCtx(ctx, c.txCtx, c.additionalMetadata)

	resp, err := c.api.Update(ctx, &api.UpdateRequest{
		Project:    c.db,
		Collection: collection,
		Filter:     filter,
		Fields:     fields,
		Options:    (*api.UpdateRequestOptions)(options),
	})

	return (*UpdateResponse)(resp), GRPCError(err)
}

func (c *grpcCRUD) deleteWithOptions(ctx context.Context, collection string, filter Filter, options *DeleteOptions) (*DeleteResponse, error) {
	ctx = setGRPCTxCtx(ctx, c.txCtx, c.additionalMetadata)

	resp, err := c.api.Delete(ctx, &api.DeleteRequest{
		Project:    c.db,
		Collection: collection,
		Filter:     filter,
		Options:    (*api.DeleteRequestOptions)(options),
	})

	return (*DeleteResponse)(resp), GRPCError(err)
}

func (c *grpcCRUD) readWithOptions(ctx context.Context, collection string, filter Filter, fields Projection, options *ReadOptions) (Iterator, error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(setGRPCTxCtx(ctx, c.txCtx, c.additionalMetadata))

	resp, err := c.api.Read(ctx, &api.ReadRequest{
		Project:    c.db,
		Collection: collection,
		Filter:     filter,
		Fields:     fields,
		Options:    (*api.ReadRequestOptions)(options),
	})
	if err != nil {
		cancel()

		return nil, GRPCError(err)
	}

	return &readIterator{streamReader: &grpcStreamReader{stream: resp, cancel: cancel}}, nil
}

type grpcStreamReader struct {
	stream api.Tigris_ReadClient
	cancel context.CancelFunc
}

func (g *grpcStreamReader) read() (Document, error) {
	resp, err := g.stream.Recv()
	if err != nil {
		return nil, GRPCError(err)
	}

	return resp.Data, nil
}

func (g *grpcStreamReader) close() error {
	g.cancel()

	return nil
}

func (c *grpcCRUD) search(ctx context.Context, collection string, req *SearchRequest) (SearchResultIterator, error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)

	resp, err := c.api.Search(ctx, &api.SearchRequest{
		Project:       c.db,
		Collection:    collection,
		Q:             req.Q,
		SearchFields:  req.SearchFields,
		Filter:        req.Filter,
		Facet:         req.Facet,
		Sort:          req.Sort,
		IncludeFields: req.IncludeFields,
		ExcludeFields: req.ExcludeFields,
		PageSize:      req.PageSize,
		Page:          req.Page,
	})
	if err != nil {
		cancel()

		return nil, GRPCError(err)
	}

	return &searchResultIterator{
		searchStreamReader: &grpcSearchReader{stream: resp, cancel: cancel},
	}, nil
}

type grpcSearchReader struct {
	stream api.Tigris_SearchClient
	cancel context.CancelFunc
}

func (g *grpcSearchReader) read() (SearchResponse, error) {
	resp, err := g.stream.Recv()
	if err != nil {
		return nil, GRPCError(err)
	}
	return resp, nil
}

func (g *grpcSearchReader) close() error {
	g.cancel()

	return nil
}

func (c *grpcDriver) CreateAppKey(ctx context.Context, project string, name string, description string) (*AppKey, error) {
	r, err := c.api.CreateAppKey(ctx, &api.CreateAppKeyRequest{Project: project, Name: name, Description: description})
	if err != nil {
		return nil, GRPCError(err)
	}

	if r.CreatedAppKey == nil {
		return nil, Error{TigrisError: api.Errorf(api.Code_INTERNAL, "empty response")}
	}

	return (*AppKey)(r.CreatedAppKey), nil
}

func (c *grpcDriver) DeleteAppKey(ctx context.Context, id string, project string) error {
	_, err := c.api.DeleteAppKey(ctx, &api.DeleteAppKeyRequest{Id: id, Project: project})

	return GRPCError(err)
}

func (c *grpcDriver) UpdateAppKey(ctx context.Context, id string, name string, description string, project string) (*AppKey, error) {
	r, err := c.api.UpdateAppKey(ctx, &api.UpdateAppKeyRequest{Id: id, Project: project, Name: name, Description: description})
	if err != nil {
		return nil, GRPCError(err)
	}

	if r.UpdatedAppKey == nil {
		return nil, Error{TigrisError: api.Errorf(api.Code_INTERNAL, "empty response")}
	}

	return (*AppKey)(r.UpdatedAppKey), nil
}

func (c *grpcDriver) ListAppKeys(ctx context.Context, project string) ([]*AppKey, error) {
	r, err := c.api.ListAppKeys(ctx, &api.ListAppKeysRequest{Project: project})
	if err != nil {
		return nil, GRPCError(err)
	}

	applications := make([]*AppKey, 0, len(r.AppKeys))
	for _, a := range r.GetAppKeys() {
		applications = append(applications, (*AppKey)(a))
	}
	return applications, nil
}

func (c *grpcDriver) RotateAppKeySecret(ctx context.Context, id string, project string) (*AppKey, error) {
	r, err := c.api.RotateAppKeySecret(ctx, &api.RotateAppKeyRequest{Id: id, Project: project})
	if err != nil {
		return nil, GRPCError(err)
	}

	if r.AppKey == nil {
		return nil, Error{TigrisError: api.Errorf(api.Code_INTERNAL, "empty response")}
	}

	return (*AppKey)(r.AppKey), nil
}

func (c *grpcDriver) GetAccessToken(ctx context.Context, clientID string, clientSecret string,
	refreshToken string,
) (*TokenResponse, error) {
	tp := api.GrantType_CLIENT_CREDENTIALS
	if refreshToken != "" {
		tp = api.GrantType_REFRESH_TOKEN
	}

	r, err := c.auth.GetAccessToken(ctx, &api.GetAccessTokenRequest{
		GrantType:    tp,
		RefreshToken: refreshToken,
		ClientId:     clientID,
		ClientSecret: clientSecret,
	})
	if err != nil {
		return nil, GRPCError(err)
	}

	return (*TokenResponse)(r), nil
}

func (c *grpcDriver) CreateNamespace(ctx context.Context, name string) error {
	_, err := c.mgmt.CreateNamespace(ctx, &api.CreateNamespaceRequest{Name: name})
	if err != nil {
		return GRPCError(err)
	}

	return nil
}

func (c *grpcDriver) ListNamespaces(ctx context.Context) ([]*Namespace, error) {
	r, err := c.mgmt.ListNamespaces(ctx, &api.ListNamespacesRequest{})
	if err != nil {
		return nil, GRPCError(err)
	}

	ns := make([]*Namespace, 0, len(r.Namespaces))
	for _, a := range r.GetNamespaces() {
		ns = append(ns, (*Namespace)(a))
	}

	return ns, nil
}

func (c *grpcDriver) QuotaLimits(ctx context.Context) (*QuotaLimits, error) {
	r, err := c.o11y.QuotaLimits(ctx, &api.QuotaLimitsRequest{})
	if err != nil {
		return nil, GRPCError(err)
	}

	return (*QuotaLimits)(r), nil
}

func (c *grpcDriver) QuotaUsage(ctx context.Context) (*QuotaUsage, error) {
	r, err := c.o11y.QuotaUsage(ctx, &api.QuotaUsageRequest{})
	if err != nil {
		return nil, GRPCError(err)
	}

	return (*QuotaUsage)(r), nil
}
