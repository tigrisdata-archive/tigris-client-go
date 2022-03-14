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
	"fmt"
	"unsafe"

	api "github.com/tigrisdata/tigrisdb-client-go/api/server/v1"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
)

type grpcDriver struct {
	*grpcCRUD
	conn *grpc.ClientConn
}

func NewGRPCClient(_ context.Context, url string, config *Config) (Driver, error) {
	rpcCreds := oauth.NewOauthAccess(&oauth2.Token{AccessToken: getAuthToken(config)})
	conn, err := grpc.Dial(url,
		grpc.WithTransportCredentials(credentials.NewTLS(config.TLS)),
		grpc.WithPerRPCCredentials(rpcCreds),
		//grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.FailOnNonTempDialError(true),
		grpc.WithReturnConnectionError(),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}

	return &driver{driverWithOptions: &grpcDriver{grpcCRUD: &grpcCRUD{api: api.NewTigrisDBClient(conn)}, conn: conn}}, nil
}

func (c *grpcDriver) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *grpcDriver) ListDatabases(ctx context.Context) ([]string, error) {
	r, err := c.api.ListDatabases(ctx, &api.ListDatabasesRequest{})
	if err != nil {
		return nil, err
	}
	return r.GetDbs(), nil
}

func (c *grpcDriver) ListCollections(ctx context.Context, db string) ([]string, error) {
	r, err := c.api.ListCollections(ctx, &api.ListCollectionsRequest{
		Db: db,
	})
	if err != nil {
		return nil, err
	}
	return r.GetCollections(), nil
}

func (c *grpcDriver) createDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error {
	_, err := c.api.CreateDatabase(ctx, &api.CreateDatabaseRequest{
		Db:      db,
		Options: (*api.DatabaseOptions)(options),
	})
	return err
}

func (c *grpcDriver) dropDatabaseWithOptions(ctx context.Context, db string, options *DatabaseOptions) error {
	_, err := c.api.DropDatabase(ctx, &api.DropDatabaseRequest{
		Db:      db,
		Options: (*api.DatabaseOptions)(options),
	})
	return err
}

func (c *grpcDriver) createCollectionWithOptions(ctx context.Context, db string, collection string, schema Schema, options *CollectionOptions) error {
	_, err := c.api.CreateCollection(ctx, &api.CreateCollectionRequest{
		Db:         db,
		Collection: collection,
		Schema:     schema,
		Options:    (*api.CollectionOptions)(options),
	})
	return err
}

func (c *grpcDriver) alterCollectionWithOptions(ctx context.Context, db string, collection string, schema Schema, options *CollectionOptions) error {
	_, err := c.api.AlterCollection(ctx, &api.AlterCollectionRequest{
		Db:         db,
		Collection: collection,
		Schema:     schema,
		Options:    (*api.CollectionOptions)(options),
	})
	return err
}

func (c *grpcDriver) dropCollectionWithOptions(ctx context.Context, db string, collection string, options *CollectionOptions) error {
	_, err := c.api.DropCollection(ctx, &api.DropCollectionRequest{
		Db:         db,
		Collection: collection,
		Options:    (*api.CollectionOptions)(options),
	})
	return err
}

func (c *grpcDriver) beginTxWithOptions(ctx context.Context, db string, options *TxOptions) (txWithOptions, error) {
	resp, err := c.api.BeginTransaction(ctx, &api.BeginTransactionRequest{
		Db:      db,
		Options: (*api.TransactionOptions)(options),
	})
	if err != nil {
		return nil, err
	}
	if resp.GetTxCtx() == nil {
		return nil, fmt.Errorf("empty transaction context in response")
	}
	return &grpcTx{db: db, grpcCRUD: &grpcCRUD{api: c.api, txCtx: *resp.GetTxCtx()}}, nil
}

type grpcTx struct {
	db string
	*grpcCRUD
}

func (c *grpcTx) Commit(ctx context.Context) error {
	_, err := c.api.CommitTransaction(ctx, &api.CommitTransactionRequest{
		Db:    c.db,
		TxCtx: &c.txCtx,
	})
	return err
}

func (c *grpcTx) Rollback(ctx context.Context) error {
	_, err := c.api.RollbackTransaction(ctx, &api.RollbackTransactionRequest{
		Db:    c.db,
		TxCtx: &c.txCtx,
	})
	return err
}

func (c *grpcTx) insertWithOptions(ctx context.Context, collection string, docs []Document, options *InsertOptions) (InsertResponse, error) {
	return c.grpcCRUD.insertWithOptions(ctx, c.db, collection, docs, options)
}

func (c *grpcTx) updateWithOptions(ctx context.Context, collection string, filter Filter, fields Fields, options *UpdateOptions) (UpdateResponse, error) {
	return c.grpcCRUD.updateWithOptions(ctx, c.db, collection, filter, fields, options)
}

func (c *grpcTx) deleteWithOptions(ctx context.Context, collection string, filter Filter, options *DeleteOptions) (DeleteResponse, error) {
	return c.grpcCRUD.deleteWithOptions(ctx, c.db, collection, filter, options)
}

func (c *grpcTx) readWithOptions(ctx context.Context, collection string, filter Filter, options *ReadOptions) (Iterator, error) {
	return c.grpcCRUD.readWithOptions(ctx, c.db, collection, filter, options)
}

func setGRPCTxCtx(txCtx *api.TransactionCtx, options *api.WriteOptions) {
	if txCtx == nil || txCtx.Id == "" {
		return
	}
	options.TxCtx = &api.TransactionCtx{Id: txCtx.Id, Origin: txCtx.Origin}
}

type grpcCRUD struct {
	api   api.TigrisDBClient
	txCtx api.TransactionCtx
}

func (c *grpcCRUD) insertWithOptions(ctx context.Context, db string, collection string, docs []Document, options *InsertOptions) (InsertResponse, error) {
	options.WriteOptions = &api.WriteOptions{}
	setGRPCTxCtx(&c.txCtx, options.WriteOptions)

	resp, err := c.api.Insert(ctx, &api.InsertRequest{
		Db:         db,
		Collection: collection,
		Documents:  *(*[][]byte)(unsafe.Pointer(&docs)),
		Options:    (*api.InsertRequestOptions)(options),
	})

	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *grpcCRUD) updateWithOptions(ctx context.Context, db string, collection string, filter Filter, fields Fields, options *UpdateOptions) (UpdateResponse, error) {
	options.WriteOptions = &api.WriteOptions{}
	setGRPCTxCtx(&c.txCtx, options.WriteOptions)

	resp, err := c.api.Update(ctx, &api.UpdateRequest{
		Db:         db,
		Collection: collection,
		Filter:     filter,
		Fields:     fields,
		Options:    (*api.UpdateRequestOptions)(options),
	})

	return resp, err
}

func (c *grpcCRUD) deleteWithOptions(ctx context.Context, db string, collection string, filter Filter, options *DeleteOptions) (DeleteResponse, error) {
	options.WriteOptions = &api.WriteOptions{}
	setGRPCTxCtx(&c.txCtx, options.WriteOptions)

	resp, err := c.api.Delete(ctx, &api.DeleteRequest{
		Db:         db,
		Collection: collection,
		Filter:     filter,
		Options:    (*api.DeleteRequestOptions)(options),
	})

	return resp, err
}

func (c *grpcCRUD) readWithOptions(ctx context.Context, db string, collection string, filter Filter, options *ReadOptions) (Iterator, error) {
	if c.txCtx.Id != "" {
		options.TxCtx = &api.TransactionCtx{Id: c.txCtx.Id, Origin: c.txCtx.Origin}
	}

	resp, err := c.api.Read(ctx, &api.ReadRequest{
		Db:         db,
		Collection: collection,
		Filter:     filter,
		Options:    (*api.ReadRequestOptions)(options),
	})
	if err != nil {
		return nil, err
	}

	return &readIterator{streamReader: &grpcStreamReader{resp}}, nil
}

type grpcStreamReader struct {
	stream api.TigrisDB_ReadClient
}

func (g *grpcStreamReader) read() (Document, error) {
	resp, err := g.stream.Recv()
	if err != nil {
		return nil, err
	}

	return resp.Doc, nil
}
