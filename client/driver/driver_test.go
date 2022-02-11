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
	"net/http"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/golang/mock/gomock"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/require"
	userHTTP "github.com/tigrisdata/tigrisdb-client-go/api/client/v1/user"
	api "github.com/tigrisdata/tigrisdb-client-go/api/server/v1"
	"github.com/tigrisdata/tigrisdb-client-go/mock"
	"google.golang.org/protobuf/proto"
)

var (
	apiPathPrefix         = "/api/v1"
	databasePathPattern   = "/databases/*"
	collectionPathPattern = "/collections/*"
	documentPathPattern   = "/documents/*"
)

type protoMatcher struct {
	message proto.Message
}

func (matcher *protoMatcher) Matches(actual interface{}) bool {
	message, ok := actual.(proto.Message)
	if !ok {
		return false
	}

	return proto.Equal(message, matcher.message)
}

func (matcher *protoMatcher) String() string {
	return fmt.Sprintf("protoMatcher: %v", matcher.message)
}

func pm(m proto.Message) gomock.Matcher {
	return &protoMatcher{m}
}

func testTxCRUDClientBasic(t *testing.T, c Tx, mc *mockServer) {
	ctx := context.TODO()

	doc1 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`)}

	options := &api.WriteOptions{}
	setGRPCTxCtx(&api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}, options)

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc1)),
			Options:    &api.InsertRequestOptions{WriteOptions: options},
		})).Return(&api.InsertResponse{}, nil)

	_, err := c.Insert(ctx, "c1", doc1, &InsertOptions{WriteOptions: options})
	require.NoError(t, err)

	doc123 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`), Document(`{"K1":"vK1","K2":2,"D1":"vD2"}`), Document(`{"K1":"vK2","K2":1,"D1":"vD3"}`)}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.InsertRequestOptions{WriteOptions: options},
		})).Return(&api.InsertResponse{}, nil)

	_, err = c.Insert(ctx, "c1", doc123, &InsertOptions{})
	require.NoError(t, err)

	mc.EXPECT().Update(gomock.Any(),
		pm(&api.UpdateRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":1}`),
			Options:    &api.UpdateRequestOptions{WriteOptions: options},
		})).Return(&api.UpdateResponse{}, nil)

	_, err = c.Update(ctx, "c1", Filter(`{"filter":"value"}`), Fields(`{"fields":1}`), &UpdateOptions{})
	require.NoError(t, err)

	roptions := &api.ReadRequestOptions{}
	roptions.TxCtx = &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().Read(
		pm(&api.ReadRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    roptions,
		}), gomock.Any()).Return(nil)

	it, err := c.Read(ctx, "c1", Filter(`{"filter":"value"}`), &ReadOptions{})
	require.NoError(t, err)

	require.False(t, it.More())

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{WriteOptions: options},
		})).Return(&api.DeleteResponse{}, nil)

	_, err = c.Delete(ctx, "c1", Filter(`{"filter":"value"}`), &DeleteOptions{})
	require.NoError(t, err)
}

func testCRUDClientBasic(t *testing.T, c Driver, mc *mockServer) {
	ctx := context.TODO()

	doc1 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`)}

	options := &api.WriteOptions{}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc1)),
			Options:    &api.InsertRequestOptions{WriteOptions: options},
		})).Return(&api.InsertResponse{}, nil)

	_, err := c.Insert(ctx, "db1", "c1", doc1, &InsertOptions{WriteOptions: options})
	require.NoError(t, err)

	doc123 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`), Document(`{"K1":"vK1","K2":2,"D1":"vD2"}`), Document(`{"K1":"vK2","K2":1,"D1":"vD3"}`)}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.InsertRequestOptions{WriteOptions: options},
		})).Return(&api.InsertResponse{}, nil)

	_, err = c.Insert(ctx, "db1", "c1", doc123, &InsertOptions{})
	require.NoError(t, err)

	mc.EXPECT().Update(gomock.Any(),
		pm(&api.UpdateRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":1}`),
			Options:    &api.UpdateRequestOptions{WriteOptions: options},
		})).Return(&api.UpdateResponse{}, nil)

	_, err = c.Update(ctx, "db1", "c1", Filter(`{"filter":"value"}`), Fields(`{"fields":1}`), &UpdateOptions{})
	require.NoError(t, err)

	roptions := &api.ReadRequestOptions{}

	mc.EXPECT().Read(
		pm(&api.ReadRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    roptions,
		}), gomock.Any()).Return(nil)

	it, err := c.Read(ctx, "db1", "c1", Filter(`{"filter":"value"}`), &ReadOptions{})
	require.NoError(t, err)

	require.False(t, it.More())

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{WriteOptions: options},
		})).Return(&api.DeleteResponse{}, nil)

	_, err = c.Delete(ctx, "db1", "c1", Filter(`{"filter":"value"}`), &DeleteOptions{})
	require.NoError(t, err)
}

func testClientBasic(t *testing.T, c Driver, mc *mockServer) {
	ctx := context.TODO()

	mc.EXPECT().ListDatabases(gomock.Any(),
		pm(&api.ListDatabasesRequest{})).Return(&api.ListDatabasesResponse{Dbs: []string{"ldb1", "ldb2"}}, nil)

	dbs, err := c.ListDatabases(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"ldb1", "ldb2"}, dbs)

	mc.EXPECT().ListCollections(gomock.Any(),
		pm(&api.ListCollectionsRequest{
			Db: "db1",
		})).Return(&api.ListCollectionsResponse{Collections: []string{"lc1", "lc2"}}, nil)

	colls, err := c.ListCollections(ctx, "db1")
	require.NoError(t, err)
	require.Equal(t, []string{"lc1", "lc2"}, colls)

	mc.EXPECT().CreateDatabase(gomock.Any(),
		pm(&api.CreateDatabaseRequest{
			Db:      "db1",
			Options: &api.DatabaseOptions{},
		})).Return(&api.CreateDatabaseResponse{}, nil)

	err = c.CreateDatabase(ctx, "db1", &DatabaseOptions{})
	require.NoError(t, err)

	sch := `{"schema":"field"}`
	mc.EXPECT().CreateCollection(gomock.Any(),
		pm(&api.CreateCollectionRequest{
			Db:         "db1",
			Collection: "c1",
			Schema:     []byte(sch),
			Options:    &api.CollectionOptions{},
		})).Return(&api.CreateCollectionResponse{}, nil)

	err = c.CreateCollection(ctx, "db1", "c1", Schema(sch), &CollectionOptions{})
	require.NoError(t, err)

	testCRUDClientBasic(t, c, mc)
}

func testTxClientBasic(t *testing.T, c Driver, mc *mockServer) {
	ctx := context.TODO()

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Db:      "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	tx, err := c.BeginTx(ctx, "db1", &TxOptions{})
	require.NoError(t, err)

	testTxCRUDClientBasic(t, tx, mc)

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Db:    "db1",
			TxCtx: txCtx,
		})).Return(&api.CommitTransactionResponse{}, nil)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	mc.EXPECT().RollbackTransaction(gomock.Any(),
		pm(&api.RollbackTransactionRequest{
			Db:    "db1",
			TxCtx: txCtx,
		})).Return(&api.RollbackTransactionResponse{}, nil)

	err = tx.Rollback(ctx)
	require.NoError(t, err)
}

func TestGRPCClient(t *testing.T) {
	c, mockServer, cancel := setupTests(t)
	defer cancel()

	client := newMockGRPCClient(t, c)
	defer func() { _ = client.Close() }()

	testClientBasic(t, client, mockServer)
}

func TestHTTPClient(t *testing.T) {
	_, mockServer, cancel := setupTests(t)
	defer cancel()

	client := newMockHTTPClient(t)
	defer func() { _ = client.Close() }()

	//FIXME: implement proper wait for HTTP server to start
	time.Sleep(10 * time.Millisecond)

	testClientBasic(t, client, mockServer)
}

func TestTxHTTPClient(t *testing.T) {
	_, mockServer, cancel := setupTests(t)
	defer cancel()

	client := newMockHTTPClient(t)
	defer func() { _ = client.Close() }()

	testTxClientBasic(t, client, mockServer)
}

func TestTxGRPCClient(t *testing.T) {
	c, mockServer, cancel := setupTests(t)
	defer cancel()

	client := newMockGRPCClient(t, c)
	defer func() { _ = client.Close() }()

	testTxClientBasic(t, client, mockServer)
}

func newMockGRPCClient(_ *testing.T, client api.TigrisDBClient) *driver {
	return &driver{driverWithOptions: &grpcDriver{grpcCRUD: &grpcCRUD{api: client}}}
}

func newMockHTTPClient(t *testing.T) *driver {
	c, err := userHTTP.NewClientWithResponses("http://localhost:33333")
	require.NoError(t, err)
	return &driver{driverWithOptions: &httpDriver{httpCRUD: &httpCRUD{api: c}}}
}

type mockServer struct {
	*mock.MockTigrisDBServer
}

func setupTests(t *testing.T) (api.TigrisDBClient, *mockServer, func()) {
	c := gomock.NewController(t)

	m := mockServer{
		MockTigrisDBServer: mock.NewMockTigrisDBServer(c),
	}

	inproc := &inprocgrpc.Channel{}

	client := api.NewTigrisDBClient(inproc)

	mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONBuiltin{}))

	err := api.RegisterTigrisDBHandlerClient(context.TODO(), mux, client)
	require.NoError(t, err)

	api.RegisterTigrisDBServer(inproc, &m)

	r := chi.NewRouter()

	r.HandleFunc(apiPathPrefix+databasePathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	r.HandleFunc(apiPathPrefix+collectionPathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
	r.HandleFunc(apiPathPrefix+documentPathPattern, func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})

	var wg sync.WaitGroup
	server := &http.Server{Addr: "localhost:33333", Handler: r}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = server.ListenAndServe()
	}()

	return client, &m, func() { _ = server.Shutdown(context.Background()); wg.Wait() }
}
