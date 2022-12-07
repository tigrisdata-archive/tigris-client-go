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

package tigris

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/test"
)

func TestClient(t *testing.T) {
	ms, cancel := test.SetupTests(t, 8)
	defer cancel()

	mc := ms.API

	ctx, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	cfg := &Config{URL: test.GRPCURL(8), Project: "db1"}
	cfg.TLS = test.SetupTLS(t)

	type Coll1 struct {
		Key1 string `tigris:"primary_key"`
	}

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Project: "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Project: "db1", Collection: "coll_1",
			Schema:  []byte(`{"title":"coll_1","properties":{"Key1":{"type":"string"}},"primary_key":["Key1"],"collection_type":"documents"}`),
			Options: &api.CollectionOptions{},
		})).Do(func(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) {
	}).Return(&api.CreateOrUpdateCollectionResponse{}, nil)

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Project: "db1",
		})).Return(&api.CommitTransactionResponse{}, nil)

	c, err := NewClient(ctx, cfg)
	require.NoError(t, err)

	db, err := c.OpenDatabase(ctx, &Coll1{})
	require.NoError(t, err)
	require.NotNil(t, db)

	_, err = c.OpenDatabase(setTxCtx(ctx, &Tx{}), &Coll1{})
	require.Error(t, err)

	err = c.Close()
	require.NoError(t, err)

	cfg.URL = "http:++//invalid"
	_, err = NewClient(ctx, cfg)
	require.Error(t, err)

	cfg.URL = ""
	_, err = NewClient(ctx, cfg, cfg)
	require.Error(t, err)
}
