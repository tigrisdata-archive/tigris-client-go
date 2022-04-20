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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/fullstorydev/grpchan/inprocgrpc"
	"github.com/go-chi/chi/v5"
	"github.com/golang/mock/gomock"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigrisdb-client-go/api/server/v1"
	"github.com/tigrisdata/tigrisdb-client-go/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	TestGRPCURL = "dns:localhost:33334"
	TestHTTPURL = "https://localhost:33333"
)

var (
	apiPathPrefix         = "/api/v1"
	databasePathPattern   = "/databases/*"
	collectionPathPattern = "/collections/*"
	documentPathPattern   = "/documents/*"
)

func testError(t *testing.T, d Driver, mc *mock.MockTigrisDBServer, in error, exp error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var r *api.DeleteResponse
	if in == nil {
		r = &api.DeleteResponse{}
	}
	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{WriteOptions: &api.WriteOptions{}},
		})).Return(r, in)

	_, err := d.Delete(ctx, "db1", "c1", Filter(`{"filter":"value"}`), &DeleteOptions{})

	require.Equal(t, exp, err)
}

func testErrors(t *testing.T, d Driver, mc *mock.MockTigrisDBServer) {
	cases := []struct {
		name string
		in   error
		exp  error
	}{
		{"tigrisdb_error", &api.TigrisDBError{Code: codes.Unauthenticated, Message: "some error"},
			&api.TigrisDBError{Code: codes.Unauthenticated, Message: "some error"}},
		{"error", fmt.Errorf("some error 1"),
			&api.TigrisDBError{Code: codes.Unknown, Message: "some error 1"}},
		{"grpc_error", status.Error(codes.PermissionDenied, "some error 1"),
			&api.TigrisDBError{Code: codes.PermissionDenied, Message: "some error 1"}},
		{"no_error", nil, nil},
	}

	for _, c := range cases {
		testError(t, d, mc, c.in, c.exp)
	}
}

func TestGRPCError(t *testing.T) {
	client, mockServer, cancel := setupGRPCTests(t, &Config{Token: "aaa"})
	defer cancel()
	testErrors(t, client, mockServer)
}

func TestHTTPError(t *testing.T) {
	client, mockServer, cancel := setupHTTPTests(t, &Config{Token: "aaa"})
	defer cancel()
	testErrors(t, client, mockServer)
}

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

func testTxCRUDBasic(t *testing.T, c Tx, mc *mock.MockTigrisDBServer) {
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

	mc.EXPECT().Replace(gomock.Any(),
		pm(&api.ReplaceRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.ReplaceRequestOptions{WriteOptions: options},
		})).Return(&api.ReplaceResponse{}, nil)

	_, err = c.Replace(ctx, "c1", doc123, &ReplaceOptions{})
	require.NoError(t, err)

	mc.EXPECT().Update(gomock.Any(),
		pm(&api.UpdateRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":1}`),
			Options:    &api.UpdateRequestOptions{WriteOptions: options},
		})).Return(&api.UpdateResponse{}, nil)

	_, err = c.Update(ctx, "c1", Filter(`{"filter":"value"}`), Update(`{"fields":1}`), &UpdateOptions{})
	require.NoError(t, err)

	roptions := &api.ReadRequestOptions{}
	roptions.TxCtx = &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().Read(
		pm(&api.ReadRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":"value"}`),
			Options:    roptions,
		}), gomock.Any()).Return(nil)

	it, err := c.Read(ctx, "c1", Filter(`{"filter":"value"}`), Projection(`{"fields":"value"}`))
	require.NoError(t, err)

	require.False(t, it.Next(nil))

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{WriteOptions: options},
		})).Return(&api.DeleteResponse{}, nil)

	_, err = c.Delete(ctx, "c1", Filter(`{"filter":"value"}`))
	require.NoError(t, err)

	coptions := &api.CollectionOptions{}
	coptions.TxCtx = &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().ListCollections(gomock.Any(),
		pm(&api.ListCollectionsRequest{
			Db:      "db1",
			Options: coptions,
		})).Return(&api.ListCollectionsResponse{Collections: []*api.CollectionInfo{{Name: "lc1"}, {Name: "lc2"}}}, nil)

	colls, err := c.ListCollections(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"lc1", "lc2"}, colls)

	sch := `{"schema":"field"}`
	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Db:         "db1",
			Collection: "c1",
			Schema:     []byte(sch),
			Options:    coptions,
		})).Return(&api.CreateOrUpdateCollectionResponse{}, nil)

	err = c.CreateOrUpdateCollection(ctx, "c1", Schema(sch))
	require.NoError(t, err)

	mc.EXPECT().DropCollection(gomock.Any(),
		pm(&api.DropCollectionRequest{
			Db:         "db1",
			Collection: "c1",
			Options:    coptions,
		})).Return(&api.DropCollectionResponse{}, nil)

	err = c.DropCollection(ctx, "c1")
	require.NoError(t, err)
}

func testCRUDBasic(t *testing.T, c Driver, mc *mock.MockTigrisDBServer) {
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

	mc.EXPECT().Replace(gomock.Any(),
		pm(&api.ReplaceRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc1)),
			Options:    &api.ReplaceRequestOptions{WriteOptions: options},
		})).Return(&api.ReplaceResponse{}, nil)

	_, err = c.Replace(ctx, "db1", "c1", doc1, &ReplaceOptions{WriteOptions: options})
	require.NoError(t, err)

	doc123 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`), Document(`{"K1":"vK1","K2":2,"D1":"vD2"}`), Document(`{"K1":"vK2","K2":1,"D1":"vD3"}`)}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.InsertRequestOptions{WriteOptions: options},
		})).Return(&api.InsertResponse{}, nil)

	_, err = c.Insert(ctx, "db1", "c1", doc123)
	require.NoError(t, err)

	mc.EXPECT().Update(gomock.Any(),
		pm(&api.UpdateRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":1}`),
			Options:    &api.UpdateRequestOptions{WriteOptions: options},
		})).Return(&api.UpdateResponse{}, nil)

	_, err = c.Update(ctx, "db1", "c1", Filter(`{"filter":"value"}`), Update(`{"fields":1}`), &UpdateOptions{})
	require.NoError(t, err)

	roptions := &api.ReadRequestOptions{}

	mc.EXPECT().Read(
		pm(&api.ReadRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":"value"}`),
			Options:    roptions,
		}), gomock.Any()).Return(nil)

	it, err := c.Read(ctx, "db1", "c1", Filter(`{"filter":"value"}`), Projection(`{"fields":"value"}`))
	require.NoError(t, err)

	require.False(t, it.Next(nil))

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

func testDriverBasic(t *testing.T, c Driver, mc *mock.MockTigrisDBServer) {
	ctx := context.TODO()

	// Test empty list response
	mc.EXPECT().ListDatabases(gomock.Any(),
		pm(&api.ListDatabasesRequest{})).Return(&api.ListDatabasesResponse{Databases: nil}, nil)

	dbs, err := c.ListDatabases(ctx)
	require.NoError(t, err)
	require.Equal(t, []string(nil), dbs)

	mc.EXPECT().ListDatabases(gomock.Any(),
		pm(&api.ListDatabasesRequest{})).Return(&api.ListDatabasesResponse{Databases: []*api.DatabaseInfo{{Name: "ldb1"}, {Name: "ldb2"}}}, nil)

	dbs, err = c.ListDatabases(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"ldb1", "ldb2"}, dbs)

	// Test empty list response
	mc.EXPECT().ListCollections(gomock.Any(),
		pm(&api.ListCollectionsRequest{
			Db:      "db1",
			Options: &api.CollectionOptions{},
		})).Return(&api.ListCollectionsResponse{Collections: nil}, nil)

	colls, err := c.ListCollections(ctx, "db1", &CollectionOptions{})
	require.NoError(t, err)
	require.Equal(t, []string(nil), colls)

	mc.EXPECT().ListCollections(gomock.Any(),
		pm(&api.ListCollectionsRequest{
			Db:      "db1",
			Options: &api.CollectionOptions{},
		})).Return(&api.ListCollectionsResponse{Collections: []*api.CollectionInfo{{Name: "lc1"}, {Name: "lc2"}}}, nil)

	colls, err = c.ListCollections(ctx, "db1")
	require.NoError(t, err)
	require.Equal(t, []string{"lc1", "lc2"}, colls)

	mc.EXPECT().CreateDatabase(gomock.Any(),
		pm(&api.CreateDatabaseRequest{
			Db:      "db1",
			Options: &api.DatabaseOptions{},
		})).Return(&api.CreateDatabaseResponse{}, nil)

	err = c.CreateDatabase(ctx, "db1", &DatabaseOptions{})
	require.NoError(t, err)

	mc.EXPECT().DropDatabase(gomock.Any(),
		pm(&api.DropDatabaseRequest{
			Db:      "db1",
			Options: &api.DatabaseOptions{},
		})).Return(&api.DropDatabaseResponse{}, nil)

	err = c.DropDatabase(ctx, "db1", &DatabaseOptions{})
	require.NoError(t, err)

	sch := `{"schema":"field"}`
	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Db:         "db1",
			Collection: "c1",
			Schema:     []byte(sch),
			Options:    &api.CollectionOptions{},
		})).Return(&api.CreateOrUpdateCollectionResponse{}, nil)

	err = c.CreateOrUpdateCollection(ctx, "db1", "c1", Schema(sch), &CollectionOptions{})
	require.NoError(t, err)

	mc.EXPECT().DropCollection(gomock.Any(),
		pm(&api.DropCollectionRequest{
			Db:         "db1",
			Collection: "c1",
			Options:    &api.CollectionOptions{},
		})).Return(&api.DropCollectionResponse{}, nil)

	err = c.DropCollection(ctx, "db1", "c1", &CollectionOptions{})
	require.NoError(t, err)

	testCRUDBasic(t, c, mc)
}

func testTxBasic(t *testing.T, c Driver, mc *mock.MockTigrisDBServer) {
	ctx := context.TODO()

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Db:      "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	tx, err := c.BeginTx(ctx, "db1", &TxOptions{})
	require.NoError(t, err)

	testTxCRUDBasic(t, tx, mc)

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

func TestGRPCDriver(t *testing.T) {
	client, mockServer, cancel := setupGRPCTests(t, &Config{Token: "aaa"})
	defer cancel()
	testDriverBasic(t, client, mockServer)
}

func TestHTTPDriver(t *testing.T) {
	client, mockServer, cancel := setupHTTPTests(t, &Config{Token: "aaa"})
	defer cancel()
	testDriverBasic(t, client, mockServer)
}

func TestTxGRPCDriver(t *testing.T) {
	client, mockServer, cancel := setupGRPCTests(t, &Config{Token: "aaa"})
	defer cancel()
	testTxBasic(t, client, mockServer)
}

func TestTxHTTPDriver(t *testing.T) {
	client, mockServer, cancel := setupHTTPTests(t, &Config{Token: "aaa"})
	defer cancel()
	testTxBasic(t, client, mockServer)
}

func testTxCRUDBasicNegative(t *testing.T, c Tx, mc *mock.MockTigrisDBServer) {
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
		})).Return(nil, fmt.Errorf("error"))

	_, err := c.Insert(ctx, "c1", doc1, &InsertOptions{WriteOptions: options})
	require.Error(t, err)

	doc123 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`), Document(`{"K1":"vK1","K2":2,"D1":"vD2"}`), Document(`{"K1":"vK2","K2":1,"D1":"vD3"}`)}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.InsertRequestOptions{WriteOptions: options},
		})).Return(nil, fmt.Errorf("error"))

	_, err = c.Insert(ctx, "c1", doc123, &InsertOptions{})
	require.Error(t, err)

	mc.EXPECT().Replace(gomock.Any(),
		pm(&api.ReplaceRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.ReplaceRequestOptions{WriteOptions: options},
		})).Return(nil, fmt.Errorf("error"))

	_, err = c.Replace(ctx, "c1", doc123, &ReplaceOptions{})
	require.Error(t, err)

	mc.EXPECT().Update(gomock.Any(),
		pm(&api.UpdateRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":1}`),
			Options:    &api.UpdateRequestOptions{WriteOptions: options},
		})).Return(nil, fmt.Errorf("error"))

	_, err = c.Update(ctx, "c1", Filter(`{"filter":"value"}`), Update(`{"fields":1}`), &UpdateOptions{})
	require.Error(t, err)

	roptions := &api.ReadRequestOptions{}
	roptions.TxCtx = &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{WriteOptions: options},
		})).Return(nil, fmt.Errorf("error"))

	_, err = c.Delete(ctx, "c1", Filter(`{"filter":"value"}`))
	require.Error(t, err)

	coptions := &api.CollectionOptions{}
	coptions.TxCtx = &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().ListCollections(gomock.Any(),
		pm(&api.ListCollectionsRequest{
			Db:      "db1",
			Options: coptions,
		})).Return(nil, fmt.Errorf("error"))

	_, err = c.ListCollections(ctx)
	require.Error(t, err)

	sch := `{"schema":"field"}`
	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Db:         "db1",
			Collection: "c1",
			Schema:     []byte(sch),
			Options:    coptions,
		})).Return(nil, fmt.Errorf("error"))

	err = c.CreateOrUpdateCollection(ctx, "c1", Schema(sch))
	require.Error(t, err)

	mc.EXPECT().DropCollection(gomock.Any(),
		pm(&api.DropCollectionRequest{
			Db:         "db1",
			Collection: "c1",
			Options:    coptions,
		})).Return(nil, fmt.Errorf("error"))

	err = c.DropCollection(ctx, "c1")
	require.Error(t, err)
}

func testTxBasicNegative(t *testing.T, c Driver, mc *mock.MockTigrisDBServer) {
	ctx := context.TODO()

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Db:      "db1",
			Options: &api.TransactionOptions{},
		})).Return(nil, fmt.Errorf("error"))

	_, err := c.BeginTx(ctx, "db1", &TxOptions{})
	require.Error(t, err)

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Db:      "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	tx, err := c.BeginTx(ctx, "db1", &TxOptions{})
	require.NoError(t, err)

	testTxCRUDBasicNegative(t, tx, mc)

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Db:    "db1",
			TxCtx: txCtx,
		})).Return(nil, fmt.Errorf("error"))

	err = tx.Commit(ctx)
	require.Error(t, err)

	mc.EXPECT().RollbackTransaction(gomock.Any(),
		pm(&api.RollbackTransactionRequest{
			Db:    "db1",
			TxCtx: txCtx,
		})).Return(nil, fmt.Errorf("error"))

	err = tx.Rollback(ctx)
	require.Error(t, err)
}

func TestTxGRPCDriverNegative(t *testing.T) {
	client, mockServer, cancel := setupGRPCTests(t, &Config{Token: "aaa"})
	defer cancel()
	testTxBasicNegative(t, client, mockServer)
}

func TestTxHTTPDriverNegative(t *testing.T) {
	client, mockServer, cancel := setupHTTPTests(t, &Config{Token: "aaa"})
	defer cancel()
	testTxBasicNegative(t, client, mockServer)
}

func TestNewDriver(t *testing.T) {
	_, cancel := setupTests(t)
	defer cancel()

	DefaultProtocol = HTTP
	client, err := NewDriver(context.Background(), TestHTTPURL, nil)
	require.NoError(t, err)
	_ = client.Close()

	DefaultProtocol = GRPC

	certPool := x509.NewCertPool()
	require.True(t, certPool.AppendCertsFromPEM([]byte(testCaCert)))

	config := Config{TLS: &tls.Config{RootCAs: certPool, ServerName: "localhost"}}
	client, err = NewDriver(context.Background(), TestGRPCURL, &config)
	require.NoError(t, err)
	_ = client.Close()

	DefaultProtocol = 11111
	_, err = NewDriver(context.Background(), TestGRPCURL, nil)
	require.Error(t, err)
}

func setupGRPCTests(t *testing.T, config *Config) (Driver, *mock.MockTigrisDBServer, func()) {
	mockServer, cancel := setupTests(t)

	certPool := x509.NewCertPool()
	require.True(t, certPool.AppendCertsFromPEM([]byte(testCaCert)))

	config.TLS = &tls.Config{RootCAs: certPool, ServerName: "localhost"}

	client, err := NewGRPCClient(context.Background(), TestGRPCURL, config)
	require.NoError(t, err)

	return client, mockServer, func() { cancel(); _ = client.Close() }
}

func setupHTTPTests(t *testing.T, config *Config) (Driver, *mock.MockTigrisDBServer, func()) {
	mockServer, cancel := setupTests(t)

	certPool := x509.NewCertPool()
	require.True(t, certPool.AppendCertsFromPEM([]byte(testCaCert)))

	config.TLS = &tls.Config{RootCAs: certPool}

	client, err := NewHTTPClient(context.Background(), TestHTTPURL, config)
	require.NoError(t, err)

	//FIXME: implement proper wait for HTTP server to start
	time.Sleep(10 * time.Millisecond)

	return client, mockServer, func() { cancel(); _ = client.Close() }
}

func setupTests(t *testing.T) (*mock.MockTigrisDBServer, func()) {
	c := gomock.NewController(t)

	m := mock.NewMockTigrisDBServer(c)

	inproc := &inprocgrpc.Channel{}
	client := api.NewTigrisDBClient(inproc)

	mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONBuiltin{}))
	err := api.RegisterTigrisDBHandlerClient(context.TODO(), mux, client)
	require.NoError(t, err)
	api.RegisterTigrisDBServer(inproc, m)

	cert, err := tls.X509KeyPair([]byte(testServerCert), []byte(testServerKey))
	require.NoError(t, err)

	tlsConfig := &tls.Config{
		ServerName:   "localhost",
		Certificates: []tls.Certificate{cert},
	}

	s := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	api.RegisterTigrisDBServer(s, m)

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
	r.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, []byte(`grant_type=refresh_token&refresh_token=refresh_token_123`), b)
		_, err = w.Write([]byte(`access_token=refreshed_token_config_123`))
		require.NoError(t, err)
	})

	var wg sync.WaitGroup

	l, err := net.Listen("tcp", "localhost:33334")
	require.NoError(t, err)

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Serve(l)
	}()

	server := &http.Server{Addr: "localhost:33333", TLSConfig: tlsConfig, Handler: r}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = server.ListenAndServeTLS("", "")
	}()

	return m, func() {
		_ = server.Shutdown(context.Background())
		s.Stop()
		wg.Wait()
	}
}

func testDriverAuth(t *testing.T, d Driver, mc *mock.MockTigrisDBServer, token string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{WriteOptions: &api.WriteOptions{}},
		})).Do(func(ctx context.Context, r *api.DeleteRequest) {
		assert.Equal(t, "Bearer "+token, metautils.ExtractIncoming(ctx).Get("authorization"))
		ua := "grpcgateway-user-agent"
		if metautils.ExtractIncoming(ctx).Get(ua) == "" {
			ua = "user-agent"
		}
		assert.True(t, strings.Contains(metautils.ExtractIncoming(ctx).Get(ua), UserAgent))
	}).Return(&api.DeleteResponse{}, nil)

	_, err := d.Delete(ctx, "db1", "c1", Filter(`{"filter":"value"}`), &DeleteOptions{})
	require.NoError(t, err)
}

func TestGRPCDriverAuth(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		client, mockServer, cancel := setupGRPCTests(t, &Config{Token: "token_config_123"})
		defer cancel()

		testDriverAuth(t, client, mockServer, "token_config_123")
	})

	t.Run("env", func(t *testing.T) {
		err := os.Setenv(TokenEnv, "token_env_567")
		require.NoError(t, err)

		client, mockServer, cancel := setupGRPCTests(t, &Config{})
		defer cancel()

		testDriverAuth(t, client, mockServer, "token_env_567")

		err = os.Unsetenv(TokenEnv)
		require.NoError(t, err)
	})
}

func TestHTTPDriverAuth(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		client, mockServer, cancel := setupHTTPTests(t, &Config{Token: "token_config_123"})
		defer cancel()

		testDriverAuth(t, client, mockServer, "token_config_123")
	})

	t.Run("env", func(t *testing.T) {
		err := os.Setenv(TokenEnv, "token_env_567")
		require.NoError(t, err)

		client, mockServer, cancel := setupHTTPTests(t, &Config{})
		defer cancel()

		testDriverAuth(t, client, mockServer, "token_env_567")

		err = os.Unsetenv(TokenEnv)
		require.NoError(t, err)
	})
}

func TestGRPCTokenRefresh(t *testing.T) {
	ToekenRefreshURL = TestHTTPURL + "/token"

	client, mockServer, cancel := setupGRPCTests(t, &Config{Token: "token_config_123:refresh_token_123"})
	defer cancel()

	testDriverAuth(t, client, mockServer, "refreshed_token_config_123")
}

func TestHTTPTokenRefresh(t *testing.T) {
	ToekenRefreshURL = TestHTTPURL + "/token"

	client, mockServer, cancel := setupHTTPTests(t, &Config{Token: "token_config_123:refresh_token_123"})
	defer cancel()

	testDriverAuth(t, client, mockServer, "refreshed_token_config_123")
}

func TestInvalidDriverAPIOptions(t *testing.T) {
	c, mc, cancel := setupGRPCTests(t, &Config{Token: "aaa"})
	defer cancel()

	ctx := context.TODO()

	_, err := c.ListCollections(ctx, "db1", &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
	_, err = c.Insert(ctx, "db1", "coll1", nil, &InsertOptions{}, &InsertOptions{})
	require.Error(t, err)
	_, err = c.Replace(ctx, "db1", "coll1", nil, &ReplaceOptions{}, &ReplaceOptions{})
	require.Error(t, err)
	_, err = c.Update(ctx, "db1", "coll1", nil, nil, &UpdateOptions{}, &UpdateOptions{})
	require.Error(t, err)
	_, err = c.Delete(ctx, "db1", "coll1", nil, nil, &DeleteOptions{}, &DeleteOptions{})
	require.Error(t, err)
	_, err = c.BeginTx(ctx, "db1", &TxOptions{}, &TxOptions{})
	require.Error(t, err)
	err = c.CreateOrUpdateCollection(ctx, "db1", "coll1", nil, &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
	err = c.DropCollection(ctx, "db1", "coll1", &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
	err = c.CreateDatabase(ctx, "db1", &DatabaseOptions{}, &DatabaseOptions{})
	require.Error(t, err)
	err = c.DropDatabase(ctx, "db1", &DatabaseOptions{}, &DatabaseOptions{})
	require.Error(t, err)
	_, err = c.Read(ctx, "db1", "coll1", nil, nil, &ReadOptions{}, &ReadOptions{})
	require.Error(t, err)

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Db:      "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	tx, err := c.BeginTx(ctx, "db1")
	require.NoError(t, err)
	_, err = tx.ListCollections(ctx, &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
	_, err = tx.Insert(ctx, "coll1", nil, &InsertOptions{}, &InsertOptions{})
	require.Error(t, err)
	_, err = tx.Replace(ctx, "coll1", nil, &ReplaceOptions{}, &ReplaceOptions{})
	require.Error(t, err)
	_, err = tx.Update(ctx, "coll1", nil, nil, &UpdateOptions{}, &UpdateOptions{})
	require.Error(t, err)
	_, err = tx.Delete(ctx, "coll1", nil, nil, &DeleteOptions{}, &DeleteOptions{})
	require.Error(t, err)
	_, err = tx.Read(ctx, "coll1", nil, nil, &ReadOptions{}, &ReadOptions{})
	require.Error(t, err)
	err = tx.CreateOrUpdateCollection(ctx, "coll1", nil, &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
	err = tx.DropCollection(ctx, "coll1", &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
}

var testServerKey = `
-----BEGIN PRIVATE KEY-----
MIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBALmad2p/jWwu6Wbc
ehfuOggJy+Ic/QGmFMMELplDwEFzPedlwU+OZsDsyqwK9jKltRlwCIBdI5+XtyN/
pOOuShmfYzkXHCx1XdyxFX7YmlPubALvgzw/9YIt/L4uVvH3h3o0k/7El9jSc3pT
3NPO+nIhbbRFlwVfJAOf9kye5dpvAgMBAAECgYA/GQxP4F0r0ib3GS1IxWxlHy95
B3HcBaI5SkqtQCM0HQGGkUlOypKUM+wS4Qch4MPYigXZ3dAmiWVxZAuie7Yku50X
IlNYTpSh4BKyDggr0BCL92I8xfQkHbuxp8ZmZgUBVuD+6W58WvTTh8vUF8u7XVVO
2mvdFES7G5Nv4SbqwQJBANpdCptrKkELurhjwBSf3p2bC9QgWwSjQT6eKuzulgHL
YWDZpbwpj7gdT9npd9CqXFCkwvvJ+XsTLpLFMB0/850CQQDZl+/MvL6HRMyF1Rw9
p/vipRKXl5g+hy0mhjNABzg+Z7iKiC8uVQhfuS6m0MCa8r/D6IPnyC4tB709jxtZ
xKZ7AkAoZkBZIsmNgTsJdEMMTculAxN8KoRMZlvi1uaAMWAFcvhQL9RO7K2PVbT5
Tw2AyJQNw33jkambkJ/0PZE6SCOtAkApid7GZ/W7Xv/oQKGuh4YHY1nkRJVUwnt1
EkNwYrBzAVvyXkMbhjIeC/0C7XEHY3YGUTn1InrmL8cJnGstPORHAkEAz+LcAUA6
3FP5O2vJmEIh9cC08WWmfTXM6y3t0tBrRq4bp7Xl2xEuzq4NO3UlY4J+1T8AG4iR
wxs1YNDMMc+6+w==
-----END PRIVATE KEY-----
`

var testServerCert = `
-----BEGIN CERTIFICATE-----
MIICtDCCAh2gAwIBAgIUKkjf/qcFf3NjowTHfRTVtiM8t0kwDQYJKoZIhvcNAQEL
BQAwgYcxCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJDQTESMBAGA1UEBwwJUGFsbyBB
bHRvMRAwDgYDVQQKDAdUZXN0T3JnMRIwEAYDVQQLDAlFZHVjYXRpb24xEjAQBgNV
BAMMCWxvY2FsaG9zdDEdMBsGCSqGSIb3DQEJARYOcm9vdEBsb2NhbGhvc3QwHhcN
MjIwMzExMjA1NTQ1WhcNMzIwMzA4MjA1NTQ1WjB4MQswCQYDVQQGEwJVUzELMAkG
A1UECAwCQ0ExCzAJBgNVBAcMAk1WMQswCQYDVQQKDAJQQzEPMA0GA1UECwwGTGFw
dG9wMRIwEAYDVQQDDAlsb2NhbGhvc3QxHTAbBgkqhkiG9w0BCQEWDnJvb3RAbG9j
YWxob3N0MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQC5mndqf41sLulm3HoX
7joICcviHP0BphTDBC6ZQ8BBcz3nZcFPjmbA7MqsCvYypbUZcAiAXSOfl7cjf6Tj
rkoZn2M5FxwsdV3csRV+2JpT7mwC74M8P/WCLfy+Llbx94d6NJP+xJfY0nN6U9zT
zvpyIW20RZcFXyQDn/ZMnuXabwIDAQABoyswKTAnBgNVHREEIDAegglsb2NhbGhv
c3SCCyoubG9jYWxob3N0hwR/AAABMA0GCSqGSIb3DQEBCwUAA4GBAK/pK2D6QU8V
mpje8CP4jhfhDk3GSATNxWJu6oPrk+fRERRXMSO5gjq4+P9ZjztbOJ8r0BvnLWUh
XCJcAUG578CGZU9iiwL8lhpfvT9HFPLR6YCFDqqExjxi3uMZ7/DT/a/LB0c6pMUk
pKxnN2NqLuAiGQB7Bekk0rVTxMxcKTJb
-----END CERTIFICATE-----
`

var testCaCert = `
-----BEGIN CERTIFICATE-----
MIIDAjCCAmugAwIBAgIUDl7q+G0GxV4JYWlPZQJrruwlb4AwDQYJKoZIhvcNAQEL
BQAwgYcxCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJDQTESMBAGA1UEBwwJUGFsbyBB
bHRvMRAwDgYDVQQKDAdUZXN0T3JnMRIwEAYDVQQLDAlFZHVjYXRpb24xEjAQBgNV
BAMMCWxvY2FsaG9zdDEdMBsGCSqGSIb3DQEJARYOcm9vdEBsb2NhbGhvc3QwHhcN
MjIwMzExMjA1NTQ1WhcNMzIwMzA4MjA1NTQ1WjCBhzELMAkGA1UEBhMCVVMxCzAJ
BgNVBAgMAkNBMRIwEAYDVQQHDAlQYWxvIEFsdG8xEDAOBgNVBAoMB1Rlc3RPcmcx
EjAQBgNVBAsMCUVkdWNhdGlvbjESMBAGA1UEAwwJbG9jYWxob3N0MR0wGwYJKoZI
hvcNAQkBFg5yb290QGxvY2FsaG9zdDCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkC
gYEA4PoLX/qB+r4bxcDCuazk7eSseGy6Amxg0Wn+muUAcQCC39sv3XK3DbOn/WYO
5K7D2XccIlKP7pBBeBnmPkdczCWiXWgBXPyWJJGNUjEbAHaykoNnDnDvoL83T+Qo
rECdw70RWk7hZVtak/kx+X/iRi82iHeKfpJ4skmKrHYXAWECAwEAAaNpMGcwHQYD
VR0OBBYEFEwr6009qnQENHnTMcbfRF4r2vrcMB8GA1UdIwQYMBaAFEwr6009qnQE
NHnTMcbfRF4r2vrcMA8GA1UdEwEB/wQFMAMBAf8wFAYDVR0RBA0wC4IJbG9jYWxo
b3N0MA0GCSqGSIb3DQEBCwUAA4GBABbCdSWWKEL+LIGvav2LpHpmfgI2GDWP9spS
uFm22fXbhhxaqzVSrfL49paBWC+DKZ3BvkyynY/6a/tpYI4b5T8HP/4NgYrQ/QeT
FiQWUL55eZ7qYvwq9LRBUk5QeSjgPi1tAVJz9c7VC2e+p3hXVwEusibCNSP9y2/S
aYWrWBUT
-----END CERTIFICATE-----
`

/*
#Above certificates and keys generated using the following script:

#!/bin/bash

HOSTNAME=localhost
EMAIL=root@localhost

rm -f -- *.pem

# 1. Generate CA's private key and self-signed certificate
openssl req -x509 -newkey rsa:1024 -days 3650 -nodes -keyout ca-key.pem -out ca-cert.pem \
	-subj "/C=US/ST=CA/L=Palo Alto/O=TestOrg/OU=Dev/CN=$HOSTNAME/emailAddress=$EMAIL" \
	-addext "subjectAltName = DNS:$HOSTNAME"

echo "----------------------------"
echo "CA's self-signed certificate"
echo "----------------------------"
openssl x509 -in ca-cert.pem -noout -text
echo "----------------------------"

# 2. Generate web server's private key and certificate signing request (CSR)
openssl req -newkey rsa:1024 -nodes -keyout server-key.pem -out server-req.pem \
	-subj "/C=US/ST=CA/L=MV/O=PC/OU=Laptop/CN=$HOSTNAME/emailAddress=$EMAIL" \
	-addext "subjectAltName = DNS:$HOSTNAME"

# 3. Use CA's private key to sign web server's CSR and get back the signed certificate
echo "subjectAltName=DNS:localhost,DNS:*.localhost,IP:127.0.0.1" >/tmp/server-ext.cnf
openssl x509 -req -in server-req.pem -days 3650 -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem -extfile /tmp/server-ext.cnf
#openssl x509 -req -in server-req.pem -days 60 -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem -extfile server-ext.cnf

echo "----------------------------"
echo "Server's signed certificate"
echo "----------------------------"
openssl x509 -in server-cert.pem -noout -text
echo "----------------------------"
*/
