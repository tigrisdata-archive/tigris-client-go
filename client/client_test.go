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

package client

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/schema"
	"github.com/tigrisdata/tigris-client-go/test"
	"google.golang.org/protobuf/proto"
)

func pm(m proto.Message) gomock.Matcher {
	return &test.ProtoMatcher{Message: m}
}

func TestClientSchemaMigration(t *testing.T) {
	mc, cancel := test.SetupTests(t)
	defer cancel()

	ctx, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	cfg := &config.Config{URL: "localhost:33334"}
	cfg.TLS = test.SetupTLS(t)

	driver.DefaultProtocol = driver.GRPC
	s, err := NewClient(ctx, cfg)
	require.NoError(t, err)

	db := s.Database("db1")

	type testSchema1 struct {
		Key1 string `json:"key_1" tigris:"primary_key"`
	}

	type testSchema2 struct {
		Key2 string `json:"key_2" tigris:"primary_key"`
	}

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Db:      "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Db: "db1", Collection: "testSchema1",
			Schema:  []byte(`{"title":"testSchema1","properties":[{"title":"key_1","type":"string"}],"primary_key":["key_1"]}`),
			Options: &api.CollectionOptions{TxCtx: txCtx},
		})).Do(func(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) {
	}).Return(&api.CreateOrUpdateCollectionResponse{}, nil)

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Db:    "db1",
			TxCtx: txCtx,
		})).Return(&api.CommitTransactionResponse{}, nil)

	mc.EXPECT().RollbackTransaction(gomock.Any(),
		pm(&api.RollbackTransactionRequest{
			Db:    "db1",
			TxCtx: txCtx,
		})).Return(&api.RollbackTransactionResponse{}, nil)

	sch, err := schema.FromCollectionModels(&testSchema1{})
	require.NoError(t, err)
	err = db.CreateCollections(ctx, "db1", sch)
	require.NoError(t, err)

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Db:      "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Db: "db1", Collection: "testSchema1",
			Schema:  []byte(`{"title":"testSchema1","properties":[{"title":"key_1","type":"string"}],"primary_key":["key_1"]}`),
			Options: &api.CollectionOptions{TxCtx: txCtx},
		})).Do(func(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) {
	}).Return(&api.CreateOrUpdateCollectionResponse{}, nil)

	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Db: "db1", Collection: "testSchema2",
			Schema:  []byte(`{"title":"testSchema2","properties":[{"title":"key_2","type":"string"}],"primary_key":["key_2"]}`),
			Options: &api.CollectionOptions{TxCtx: txCtx},
		})).Do(func(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) {
	}).Return(&api.CreateOrUpdateCollectionResponse{}, nil)

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Db:    "db1",
			TxCtx: txCtx,
		})).Return(&api.CommitTransactionResponse{}, nil)

	mc.EXPECT().RollbackTransaction(gomock.Any(),
		pm(&api.RollbackTransactionRequest{
			Db:    "db1",
			TxCtx: txCtx,
		})).Return(&api.RollbackTransactionResponse{}, nil)

	sch, err = schema.FromCollectionModels(&testSchema1{}, testSchema2{})
	require.NoError(t, err)
	err = db.CreateCollections(ctx, "db1", sch)
	require.NoError(t, err)

	var m map[string]string
	_, err = schema.FromCollectionModels(&m)
	require.Error(t, err)
	_, _, err = schema.FromDatabaseModel(&m)
	require.Error(t, err)

	var i int
	_, err = schema.FromCollectionModels(&i)
	require.Error(t, err)
	_, _, err = schema.FromDatabaseModel(&i)
	require.Error(t, err)
}
