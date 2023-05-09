// Copyright 2022-2023 Tigris Data, Inc.
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

//go:build tigris_grpc || (!tigris_grpc && !tigris_http)

package driver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	mock "github.com/tigrisdata/tigris-client-go/mock/api"
	"github.com/tigrisdata/tigris-client-go/test"
	metadata2 "google.golang.org/grpc/metadata"
)

func setupGRPCTests(t *testing.T, config *config.Driver) (Driver, *grpcDriver, *test.MockServers, func()) {
	mockServers, cancel := test.SetupTests(t, 0)
	config.TLS = test.SetupTLS(t)
	config.URL = test.GRPCURL(0)
	client, _, _, err := newGRPCClient(context.Background(), config)
	require.NoError(t, err)

	return &driver{driverWithOptions: client, cfg: config}, client.(*grpcDriver), mockServers, func() { cancel(); _ = client.Close() }
}

func SetupO11yGRPCTests(t *testing.T, config *config.Driver) (Driver, Observability, *test.MockServers, func()) {
	return setupGRPCTests(t, config)
}

func SetupMgmtGRPCTests(t *testing.T, config *config.Driver) (Driver, Management, *test.MockServers, func()) {
	return setupGRPCTests(t, config)
}

func SetupGRPCTests(t *testing.T, config *config.Driver) (Driver, *mock.MockTigrisServer, func()) {
	setMetadata = func(ctx context.Context, txCtx *api.TransactionCtx, md metadata2.MD) {
		setGRPCMetadata(ctx, txCtx, md)
	}
	d, _, s, f := setupGRPCTests(t, config)
	return d, s.API, f
}

func TestGRPCError(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{})
	defer cancel()
	testErrors(t, client, mockServer)
	t.Run("read_stream_error", func(t *testing.T) {
		testReadStreamError(t, client, mockServer)
	})
	t.Run("search_stream_error", func(t *testing.T) {
		testSearchStreamError(t, client, mockServer)
	})
	t.Run("branch_crud_errors", func(t *testing.T) {
		testBranchCrudErrors(t, client, mockServer)
	})
}

func TestGRPCDriver(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{})
	defer cancel()
	testDriverBasic(t, client, mockServer)
	testResponseMetadata(t, client, mockServer)
}

func TestTxGRPCDriver(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{})
	defer cancel()
	testTxBasic(t, client, mockServer)
}

func TestTxGRPCDriverNegative(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{})
	defer cancel()
	testTxBasicNegative(t, client, mockServer)
}

func TestGRPCHeaders(t *testing.T) {
	c, mc, cancel := SetupGRPCTests(t, &config.Driver{SkipSchemaValidation: true})
	defer cancel()

	testSchemaSignOffHeader(t, mc, c)
	testSchemaVersion(t, mc, c)
}

func TestNewDriverGRPC(t *testing.T) {
	_, cancel := test.SetupTests(t, 4)
	defer cancel()

	DefaultProtocol = GRPC

	certPool := x509.NewCertPool()
	require.True(t, certPool.AppendCertsFromPEM([]byte(test.CaCert)))

	cfg := config.Driver{URL: test.GRPCURL(4), TLS: &tls.Config{
		RootCAs: certPool, ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	}}
	client, err := NewDriver(context.Background(), &cfg)
	require.NoError(t, err)

	_ = client.Close()

	DefaultProtocol = "SOMETHING"
	_, err = NewDriver(context.Background(), nil)
	require.Error(t, err)

	DefaultProtocol = GRPC
	cfg1 := &config.Driver{URL: test.GRPCURL(4), ClientSecret: "aaaa"}
	cfg1, err = initConfig(cfg1)
	require.NoError(t, err)
	require.NotNil(t, cfg1.TLS)
}

func TestInvalidDriverAPIOptions(t *testing.T) {
	c, mc, cancel := SetupGRPCTests(t, &config.Driver{})
	defer cancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
	_, err = c.UseDatabase("db1").BeginTx(ctx, &TxOptions{}, &TxOptions{})
	require.Error(t, err)
	err = db.CreateOrUpdateCollection(ctx, "coll1", nil, &CreateCollectionOptions{}, &CreateCollectionOptions{})
	require.Error(t, err)
	err = db.DropCollection(ctx, "coll1", &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
	_, err = db.Read(ctx, "coll1", nil, nil, &ReadOptions{}, &ReadOptions{})
	require.Error(t, err)
	_, err = db.Search(ctx, "coll1", nil)
	require.Error(t, err)

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Project: "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	tx, err := c.UseDatabase("db1").BeginTx(ctx)
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
	err = tx.CreateOrUpdateCollection(ctx, "coll1", nil, &CreateCollectionOptions{}, &CreateCollectionOptions{})
	require.Error(t, err)
	err = tx.DropCollection(ctx, "coll1", &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
}
