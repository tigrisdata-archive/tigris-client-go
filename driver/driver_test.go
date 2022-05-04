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
	"os"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/golang/mock/gomock"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	mock "github.com/tigrisdata/tigris-client-go/mock/api"
	"github.com/tigrisdata/tigris-client-go/test"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func SetupGRPCTests(t *testing.T, config *config.Driver) (Driver, *mock.MockTigrisServer, func()) {
	mockServer, cancel := test.SetupTests(t, 0)
	config.TLS = test.SetupTLS(t)
	client, err := NewGRPCClient(context.Background(), test.GRPCURL(0), config)
	require.NoError(t, err)

	return client, mockServer, func() { cancel(); _ = client.Close() }
}

func SetupHTTPTests(t *testing.T, config *config.Driver) (Driver, *mock.MockTigrisServer, func()) {
	mockServer, cancel := test.SetupTests(t, 0)
	config.TLS = test.SetupTLS(t)
	client, err := NewHTTPClient(context.Background(), test.HTTPURL(0), config)
	require.NoError(t, err)

	//FIXME: implement proper wait for HTTP server to start
	time.Sleep(10 * time.Millisecond)

	return client, mockServer, func() { cancel(); _ = client.Close() }
}

func testError(t *testing.T, d Driver, mc *mock.MockTigrisServer, in error, exp error) {
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

	_, err := d.UseDatabase("db1").Delete(ctx, "c1", Filter(`{"filter":"value"}`), &DeleteOptions{})

	require.Equal(t, exp, err)
}

func testErrors(t *testing.T, d Driver, mc *mock.MockTigrisServer) {
	cases := []struct {
		name string
		in   error
		exp  error
	}{
		{"tigris_error", &api.TigrisError{Code: codes.Unauthenticated, Message: "some error"},
			&api.TigrisError{Code: codes.Unauthenticated, Message: "some error"}},
		{"error", fmt.Errorf("some error 1"),
			&api.TigrisError{Code: codes.Unknown, Message: "some error 1"}},
		{"grpc_error", status.Error(codes.PermissionDenied, "some error 1"),
			&api.TigrisError{Code: codes.PermissionDenied, Message: "some error 1"}},
		{"no_error", nil, nil},
	}

	for _, c := range cases {
		testError(t, d, mc, c.in, c.exp)
	}
}

func TestGRPCError(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{Token: "aaa"})
	defer cancel()
	testErrors(t, client, mockServer)
}

func TestHTTPError(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{Token: "aaa"})
	defer cancel()
	testErrors(t, client, mockServer)
}

func pm(m proto.Message) gomock.Matcher {
	return &test.ProtoMatcher{Message: m}
}

func testTxCRUDBasic(t *testing.T, c Tx, mc *mock.MockTigrisServer) {
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

	_, err = c.Insert(ctx, "c1", doc123)
	require.NoError(t, err)

	mc.EXPECT().Replace(gomock.Any(),
		pm(&api.ReplaceRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.ReplaceRequestOptions{WriteOptions: options},
		})).Return(&api.ReplaceResponse{}, nil)

	_, err = c.Replace(ctx, "c1", doc123)
	require.NoError(t, err)

	mc.EXPECT().Update(gomock.Any(),
		pm(&api.UpdateRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":1}`),
			Options:    &api.UpdateRequestOptions{WriteOptions: options},
		})).Return(&api.UpdateResponse{}, nil)

	_, err = c.Update(ctx, "c1", Filter(`{"filter":"value"}`), Update(`{"fields":1}`))
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
		})).Return(&api.ListCollectionsResponse{Collections: []*api.CollectionInfo{{Collection: "lc1"}, {Collection: "lc2"}}}, nil)

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

func testCRUDBasic(t *testing.T, c Driver, mc *mock.MockTigrisServer) {
	ctx := context.TODO()

	db := c.UseDatabase("db1")

	doc1 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`)}

	options := &api.WriteOptions{}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc1)),
			Options:    &api.InsertRequestOptions{WriteOptions: options},
		})).Return(&api.InsertResponse{Status: "inserted"}, nil)

	insResp, err := db.Insert(ctx, "c1", doc1, &InsertOptions{WriteOptions: options})
	require.NoError(t, err)
	require.Equal(t, "inserted", insResp.Status)

	mc.EXPECT().Replace(gomock.Any(),
		pm(&api.ReplaceRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc1)),
			Options:    &api.ReplaceRequestOptions{WriteOptions: options},
		})).Return(&api.ReplaceResponse{Status: "replaced"}, nil)

	repResp, err := db.Replace(ctx, "c1", doc1, &ReplaceOptions{WriteOptions: options})
	require.NoError(t, err)
	require.Equal(t, "replaced", repResp.Status)

	doc123 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`), Document(`{"K1":"vK1","K2":2,"D1":"vD2"}`), Document(`{"K1":"vK2","K2":1,"D1":"vD3"}`)}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Db:         "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.InsertRequestOptions{WriteOptions: options},
		})).Return(&api.InsertResponse{}, nil)

	_, err = db.Insert(ctx, "c1", doc123)
	require.NoError(t, err)

	mc.EXPECT().Update(gomock.Any(),
		pm(&api.UpdateRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":1}`),
			Options:    &api.UpdateRequestOptions{WriteOptions: options},
		})).Return(&api.UpdateResponse{Status: "updated"}, nil)

	updResp, err := db.Update(ctx, "c1", Filter(`{"filter":"value"}`), Update(`{"fields":1}`))
	require.NoError(t, err)
	require.Equal(t, "updated", updResp.Status)

	roptions := &api.ReadRequestOptions{}

	mc.EXPECT().Read(
		pm(&api.ReadRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":"value"}`),
			Options:    roptions,
		}), gomock.Any()).Return(nil)

	it, err := db.Read(ctx, "c1", Filter(`{"filter":"value"}`), Projection(`{"fields":"value"}`))
	require.NoError(t, err)

	require.False(t, it.Next(nil))

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Db:         "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{WriteOptions: options},
		})).Return(&api.DeleteResponse{Status: "deleted"}, nil)

	delResp, err := db.Delete(ctx, "c1", Filter(`{"filter":"value"}`))
	require.NoError(t, err)
	require.Equal(t, "deleted", delResp.Status)
}

func testDriverBasic(t *testing.T, c Driver, mc *mock.MockTigrisServer) {
	ctx := context.TODO()

	// Test empty list response
	mc.EXPECT().ListDatabases(gomock.Any(),
		pm(&api.ListDatabasesRequest{})).Return(&api.ListDatabasesResponse{Databases: nil}, nil)

	dbs, err := c.ListDatabases(ctx)
	require.NoError(t, err)
	require.Equal(t, []string(nil), dbs)

	mc.EXPECT().ListDatabases(gomock.Any(),
		pm(&api.ListDatabasesRequest{})).Return(&api.ListDatabasesResponse{Databases: []*api.DatabaseInfo{{Db: "ldb1"}, {Db: "ldb2"}}}, nil)

	dbs, err = c.ListDatabases(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"ldb1", "ldb2"}, dbs)

	db := c.UseDatabase("db1")

	// Test empty list response
	mc.EXPECT().ListCollections(gomock.Any(),
		pm(&api.ListCollectionsRequest{
			Db:      "db1",
			Options: &api.CollectionOptions{},
		})).Return(&api.ListCollectionsResponse{Collections: nil}, nil)

	colls, err := db.ListCollections(ctx, &CollectionOptions{})
	require.NoError(t, err)
	require.Equal(t, []string(nil), colls)

	mc.EXPECT().ListCollections(gomock.Any(),
		pm(&api.ListCollectionsRequest{
			Db:      "db1",
			Options: &api.CollectionOptions{},
		})).Return(&api.ListCollectionsResponse{Collections: []*api.CollectionInfo{{Collection: "lc1"}, {Collection: "lc2"}}}, nil)

	colls, err = db.ListCollections(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"lc1", "lc2"}, colls)

	descExp := api.DescribeCollectionResponse{
		Collection: "coll1",
		Schema:     []byte(`{"a":"b"}`),
	}

	mc.EXPECT().DescribeCollection(gomock.Any(),
		pm(&api.DescribeCollectionRequest{
			Db:         "db1",
			Collection: "coll1",
		})).Return(&descExp, nil)

	desc, err := db.DescribeCollection(ctx, "coll1")
	require.NoError(t, err)
	require.Equal(t, descExp.Collection, desc.Collection)
	require.Equal(t, descExp.Schema, desc.Schema)

	descDbExp := api.DescribeDatabaseResponse{
		Collections: []*api.CollectionDescription{
			{Collection: "coll1",
				Schema: []byte(`{"a":"b"}`),
			},
			{Collection: "coll2",
				Schema: []byte(`{"c":"d"}`),
			},
		},
	}

	mc.EXPECT().DescribeDatabase(gomock.Any(),
		pm(&api.DescribeDatabaseRequest{
			Db: "db1",
		})).Return(&descDbExp, nil)

	descDb, err := c.DescribeDatabase(ctx, "db1")
	require.NoError(t, err)
	require.Equal(t, descDbExp.Collections[0].Collection, descDb.Collections[0].Collection)
	require.Equal(t, descDbExp.Collections[0].Schema, descDb.Collections[0].Schema)
	require.Equal(t, descDbExp.Collections[1].Collection, descDb.Collections[1].Collection)
	require.Equal(t, descDbExp.Collections[1].Schema, descDb.Collections[1].Schema)

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

	err = db.CreateOrUpdateCollection(ctx, "c1", Schema(sch), &CollectionOptions{})
	require.NoError(t, err)

	mc.EXPECT().DropCollection(gomock.Any(),
		pm(&api.DropCollectionRequest{
			Db:         "db1",
			Collection: "c1",
			Options:    &api.CollectionOptions{},
		})).Return(&api.DropCollectionResponse{}, nil)

	err = db.DropCollection(ctx, "c1", &CollectionOptions{})
	require.NoError(t, err)

	testCRUDBasic(t, c, mc)
}

func testTxBasic(t *testing.T, c Driver, mc *mock.MockTigrisServer) {
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

	err = tx.Rollback(ctx)
	require.NoError(t, err)
}

func testGetInfo(t *testing.T, c Driver, mc *mock.MockTigrisServer) {
	ctx := context.TODO()

	mc.EXPECT().GetInfo(gomock.Any(),
		pm(&api.GetInfoRequest{})).Return(&api.GetInfoResponse{ServerVersion: "some version"}, nil)

	info, err := c.Info(ctx)
	require.NoError(t, err)
	require.Equal(t, "some version", info.ServerVersion)

	mc.EXPECT().GetInfo(gomock.Any(),
		pm(&api.GetInfoRequest{})).Return(nil, fmt.Errorf("some error"))

	_, err = c.Info(ctx)
	require.Error(t, err)
}

func TestGRPCDriver(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{Token: "aaa"})
	defer cancel()
	testDriverBasic(t, client, mockServer)
	testGetInfo(t, client, mockServer)
}

func TestHTTPDriver(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{Token: "aaa"})
	defer cancel()
	testDriverBasic(t, client, mockServer)
	testGetInfo(t, client, mockServer)
}

func TestTxGRPCDriver(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{Token: "aaa"})
	defer cancel()
	testTxBasic(t, client, mockServer)
}

func TestTxHTTPDriver(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{Token: "aaa"})
	defer cancel()
	testTxBasic(t, client, mockServer)
}

func testTxCRUDBasicNegative(t *testing.T, c Tx, mc *mock.MockTigrisServer) {
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

func testTxBasicNegative(t *testing.T, c Driver, mc *mock.MockTigrisServer) {
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
	client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{Token: "aaa"})
	defer cancel()
	testTxBasicNegative(t, client, mockServer)
}

func TestTxHTTPDriverNegative(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{Token: "aaa"})
	defer cancel()
	testTxBasicNegative(t, client, mockServer)
}

func TestNewDriver(t *testing.T) {
	_, cancel := test.SetupTests(t, 0)
	defer cancel()

	DefaultProtocol = HTTP
	cfg := config.Driver{URL: test.HTTPURL(0)}
	client, err := NewDriver(context.Background(), &cfg)
	require.NoError(t, err)
	_ = client.Close()

	DefaultProtocol = GRPC

	certPool := x509.NewCertPool()
	require.True(t, certPool.AppendCertsFromPEM([]byte(test.CaCert)))

	cfg = config.Driver{URL: test.GRPCURL(0), TLS: &tls.Config{RootCAs: certPool, ServerName: "localhost"}}
	client, err = NewDriver(context.Background(), &cfg)
	require.NoError(t, err)
	_ = client.Close()

	DefaultProtocol = 11111
	_, err = NewDriver(context.Background(), nil)
	require.Error(t, err)
}

func testDriverAuth(t *testing.T, d Driver, mc *mock.MockTigrisServer, token string) {
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

	db := d.UseDatabase("db1")

	_, err := db.Delete(ctx, "c1", Filter(`{"filter":"value"}`), &DeleteOptions{})
	require.NoError(t, err)
}

func TestGRPCDriverAuth(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{Token: "token_config_123"})
		defer cancel()

		testDriverAuth(t, client, mockServer, "token_config_123")
	})

	t.Run("env", func(t *testing.T) {
		err := os.Setenv(TokenEnv, "token_env_567")
		require.NoError(t, err)

		client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{})
		defer cancel()

		testDriverAuth(t, client, mockServer, "token_env_567")

		err = os.Unsetenv(TokenEnv)
		require.NoError(t, err)
	})
}

func TestHTTPDriverAuth(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{Token: "token_config_123"})
		defer cancel()

		testDriverAuth(t, client, mockServer, "token_config_123")
	})

	t.Run("env", func(t *testing.T) {
		err := os.Setenv(TokenEnv, "token_env_567")
		require.NoError(t, err)

		client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{})
		defer cancel()

		testDriverAuth(t, client, mockServer, "token_env_567")

		err = os.Unsetenv(TokenEnv)
		require.NoError(t, err)
	})
}

func TestGRPCTokenRefresh(t *testing.T) {
	TokenRefreshURL = test.HTTPURL(0) + "/token"

	client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{Token: "token_config_123:refresh_token_123"})
	defer cancel()

	testDriverAuth(t, client, mockServer, "refreshed_token_config_123")
}

func TestHTTPTokenRefresh(t *testing.T) {
	TokenRefreshURL = test.HTTPURL(0) + "/token"

	client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{Token: "token_config_123:refresh_token_123"})
	defer cancel()

	testDriverAuth(t, client, mockServer, "refreshed_token_config_123")
}

func TestInvalidDriverAPIOptions(t *testing.T) {
	c, mc, cancel := SetupGRPCTests(t, &config.Driver{Token: "aaa"})
	defer cancel()

	ctx := context.TODO()

	db := c.UseDatabase("db1")

	_, err := db.ListCollections(ctx, &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
	_, err = db.Insert(ctx, "coll1", nil, &InsertOptions{}, &InsertOptions{})
	require.Error(t, err)
	_, err = db.Replace(ctx, "coll1", nil, &ReplaceOptions{}, &ReplaceOptions{})
	require.Error(t, err)
	_, err = db.Update(ctx, "coll1", nil, nil, &UpdateOptions{}, &UpdateOptions{})
	require.Error(t, err)
	_, err = db.Delete(ctx, "coll1", nil, nil, &DeleteOptions{}, &DeleteOptions{})
	require.Error(t, err)
	_, err = c.BeginTx(ctx, "db1", &TxOptions{}, &TxOptions{})
	require.Error(t, err)
	err = db.CreateOrUpdateCollection(ctx, "coll1", nil, &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
	err = db.DropCollection(ctx, "coll1", &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
	err = c.CreateDatabase(ctx, "db1", &DatabaseOptions{}, &DatabaseOptions{})
	require.Error(t, err)
	err = c.DropDatabase(ctx, "db1", &DatabaseOptions{}, &DatabaseOptions{})
	require.Error(t, err)
	_, err = db.Read(ctx, "coll1", nil, nil, &ReadOptions{}, &ReadOptions{})
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
