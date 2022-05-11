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
	"encoding/json"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/mock"
	"github.com/tigrisdata/tigris-client-go/projection"
	"github.com/tigrisdata/tigris-client-go/schema"
	"github.com/tigrisdata/tigris-client-go/test"
	"github.com/tigrisdata/tigris-client-go/update"
	"google.golang.org/protobuf/proto"
)

type JSONMatcher struct {
	T        *testing.T
	Expected []byte
}

func (matcher *JSONMatcher) Matches(actual interface{}) bool {
	return assert.JSONEq(matcher.T, string(matcher.Expected), string(actual.(driver.Schema)))
}

func (matcher *JSONMatcher) String() string {
	return fmt.Sprintf("JSONMatcher: %v", string(matcher.Expected))
}

func (matcher *JSONMatcher) Got(actual interface{}) string {
	ptr := unsafe.Pointer(&actual)
	return fmt.Sprintf("JSONMatcher: %v", string(*(*[]byte)(ptr)))
}

func jm(t *testing.T, expected string) gomock.Matcher {
	j := &JSONMatcher{T: t, Expected: []byte(expected)}
	return gomock.GotFormatterAdapter(j, j)
}

func toDocument(t *testing.T, doc interface{}) driver.Document {
	b, err := json.Marshal(doc)
	require.NoError(t, err)
	return b
}

func TestCollectionBasic(t *testing.T) {
	ctx := context.TODO()

	ctrl := gomock.NewController(t)
	m := mock.NewMockDriver(ctrl)

	m.EXPECT().CreateDatabase(gomock.Any(), "db1")

	type Coll1 struct {
		Key1   string `tigris:"primary_key"`
		Field1 int64
	}

	type Coll2 struct {
		Key1   string `tigris:"primary_key"`
		Field1 int64
	}

	mdb := mock.NewMockDatabase(ctrl)
	mtx := mock.NewMockTx(ctrl)

	m.EXPECT().BeginTx(gomock.Any(), "db1").Return(mtx, nil)

	mtx.EXPECT().CreateOrUpdateCollection(gomock.Any(), "coll_1", jm(t, `{"title":"coll_1","properties":{"Key1":{"type":"string"},"Field1":{"type":"integer"}},"primary_key":["Key1"]}`))
	mtx.EXPECT().CreateOrUpdateCollection(gomock.Any(), "coll_2", jm(t, `{"title":"coll_2","properties":{"Key1":{"type":"string"},"Field1":{"type":"integer"}},"primary_key":["Key1"]}`))
	mtx.EXPECT().Commit(ctx)
	mtx.EXPECT().Rollback(ctx)

	db, err := openDatabaseFromModels(ctx, m, &config.Database{}, "db1", &Coll1{}, &Coll2{})
	require.NoError(t, err)

	m.EXPECT().UseDatabase("db1").Return(mdb)

	c := GetCollection[Coll1](db)

	d1 := &Coll1{Key1: "aaa", Field1: 123}
	d2 := &Coll1{Key1: "bbb", Field1: 123}

	mdb.EXPECT().Insert(ctx, "coll_1", []driver.Document{toDocument(t, d1), toDocument(t, d2)})

	_, err = c.Insert(ctx, d1, d2)
	require.NoError(t, err)

	mdb.EXPECT().Replace(ctx, "coll_1", []driver.Document{toDocument(t, d2)})

	_, err = c.InsertOrReplace(ctx, d2)
	require.NoError(t, err)

	mdb.EXPECT().Update(ctx, "coll_1",
		driver.Filter(`{"$or":[{"Key1":{"$eq":"aaa"}},{"Key1":{"$eq":"bbb"}}]}`),
		driver.Update(`{"$set":{"Field1":345}}`))

	_, err = c.Update(ctx, filter.Or(
		filter.Eq("Key1", "aaa"),
		filter.Eq("Key1", "bbb")),
		update.Set("Field1", 345),
	)
	require.NoError(t, err)

	mit := mock.NewMockIterator(ctrl)

	mdb.EXPECT().Read(ctx, "coll_1",
		driver.Filter(`{"$or":[{"Key1":{"$eq":"aaa"}},{"Key1":{"$eq":"ccc"}}]}`),
		driver.Projection(`{"Field1":true,"Key1":false}`),
	).Return(mit, nil)

	it, err := c.Read(ctx, filter.Or(
		filter.Eq("Key1", "aaa"),
		filter.Eq("Key1", "ccc")),
		projection.Exclude("Key1").
			Include("Field1"),
	)
	require.NoError(t, err)

	var d Coll1
	var dd driver.Document
	var dd1 driver.Document

	mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, d1)).Return(true)
	mit.EXPECT().Next(&dd1).Return(false)
	mit.EXPECT().Err().Return(nil)

	for it.Next(&d) {
		require.Equal(t, *d1, d)
	}

	require.NoError(t, it.Err())

	mit.EXPECT().Close()
	it.Close()

	mdb.EXPECT().Read(ctx, "coll_1", driver.Filter(`{}`), driver.Projection(nil)).Return(mit, nil)

	it, err = c.ReadAll(ctx, projection.All)
	require.NoError(t, err)

	mdb.EXPECT().Delete(ctx, "coll_1", driver.Filter(`{}`))

	_, err = c.DeleteAll(ctx)
	require.NoError(t, err)

	mdb.EXPECT().Delete(ctx, "coll_1", driver.Filter(`{"$or":[{"Key1":{"$eq":"aaa"}},{"Key1":{"$eq":"ccc"}}]}`))

	_, err = c.Delete(ctx, filter.Or(
		filter.Eq("Key1", "aaa"),
		filter.Eq("Key1", "ccc")))
	require.NoError(t, err)

	mdb.EXPECT().Read(ctx, "coll_1",
		driver.Filter(`{"Key1":{"$eq":"aaa"}}`),
		driver.Projection(nil),
	).Return(mit, nil)

	mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, d1)).Return(true)

	pd, err := c.ReadOne(ctx, filter.Eq("Key1", "aaa"))
	require.NoError(t, err)
	require.Equal(t, d1, pd)

	mdb.EXPECT().DropCollection(ctx, "coll_1")

	err = c.Drop(ctx)
	require.NoError(t, err)

}

func TestCollectionTx(t *testing.T) {
	ctx := context.TODO()

	ctrl := gomock.NewController(t)
	m := mock.NewMockDriver(ctrl)
	mdb := mock.NewMockDatabase(ctrl)
	mtx := mock.NewMockTx(ctrl)

	type Coll1 struct {
		Key1   string `tigris:"primary_key"`
		Field1 int64
	}

	db := newDatabase("db1", m)

	m.EXPECT().BeginTx(gomock.Any(), "db1").Return(mtx, nil)

	err := db.Tx(ctx, func(ctx context.Context) error {
		m.EXPECT().UseDatabase("db1").Return(mdb)
		c := GetCollection[Coll1](db)

		d1 := &Coll1{Key1: "aaa", Field1: 123}
		d2 := &Coll1{Key1: "bbb", Field1: 123}

		mtx.EXPECT().Insert(ctx, "coll_1", []driver.Document{toDocument(t, d1), toDocument(t, d2)})

		_, err := c.Insert(ctx, d1, d2)
		require.NoError(t, err)

		mtx.EXPECT().Replace(ctx, "coll_1", []driver.Document{toDocument(t, d2)})

		_, err = c.InsertOrReplace(ctx, d2)
		require.NoError(t, err)

		mtx.EXPECT().Update(ctx, "coll_1",
			driver.Filter(`{"$or":[{"Key1":{"$eq":"aaa"}},{"Key1":{"$eq":"bbb"}}]}`),
			driver.Update(`{"$set":{"Field1":345}}`))

		_, err = c.Update(ctx, filter.Or(
			filter.Eq("Key1", "aaa"),
			filter.Eq("Key1", "bbb")),
			update.Set("Field1", 345),
		)
		require.NoError(t, err)

		mit := mock.NewMockIterator(ctrl)

		mtx.EXPECT().Read(ctx, "coll_1",
			driver.Filter(`{"$or":[{"Key1":{"$eq":"aaa"}},{"Key1":{"$eq":"ccc"}}]}`),
			driver.Projection(`{"Field1":true,"Key1":false}`),
		).Return(mit, nil)

		it, err := c.Read(ctx, filter.Or(
			filter.Eq("Key1", "aaa"),
			filter.Eq("Key1", "ccc")),
			projection.Exclude("Key1").
				Include("Field1"),
		)
		require.NoError(t, err)

		var d Coll1
		var dd driver.Document
		var dd1 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, d1)).Return(true)
		mit.EXPECT().Next(&dd1).Return(false)
		mit.EXPECT().Err().Return(nil)

		for it.Next(&d) {
			require.Equal(t, *d1, d)
		}

		require.NoError(t, it.Err())

		mit.EXPECT().Close()
		it.Close()

		mtx.EXPECT().Read(ctx, "coll_1", driver.Filter(`{}`), driver.Projection(nil)).Return(mit, nil)

		it, err = c.ReadAll(ctx, projection.All)
		require.NoError(t, err)

		mtx.EXPECT().Delete(ctx, "coll_1", driver.Filter(`{}`))

		_, err = c.DeleteAll(ctx)
		require.NoError(t, err)

		mtx.EXPECT().Delete(ctx, "coll_1", driver.Filter(`{"$or":[{"Key1":{"$eq":"aaa"}},{"Key1":{"$eq":"ccc"}}]}`))

		_, err = c.Delete(ctx, filter.Or(
			filter.Eq("Key1", "aaa"),
			filter.Eq("Key1", "ccc")))
		require.NoError(t, err)

		mtx.EXPECT().Read(ctx, "coll_1",
			driver.Filter(`{"Key1":{"$eq":"aaa"}}`),
			driver.Projection(nil),
		).Return(mit, nil)

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, d1)).Return(true)

		pd, err := c.ReadOne(ctx, filter.Eq("Key1", "aaa"))
		require.NoError(t, err)
		require.Equal(t, d1, pd)

		mtx.EXPECT().DropCollection(ctx, "coll_1")

		err = c.Drop(ctx)
		require.NoError(t, err)

		mtx.EXPECT().Insert(ctx, "coll_1", []driver.Document{toDocument(t, d1)})

		_, err = c.Insert(ctx, &Coll1{Key1: "aaa", Field1: 123})
		require.NoError(t, err)

		mtx.EXPECT().DropCollection(ctx, "coll_1")

		err = c.Drop(ctx)
		require.NoError(t, err)

		mtx.EXPECT().Commit(context.Background())
		mtx.EXPECT().Rollback(context.Background())

		return nil
	})
	require.NoError(t, err)
}

func TestCollectionNegative(t *testing.T) {
	ctx := context.TODO()

	ctrl := gomock.NewController(t)
	m := mock.NewMockDriver(ctrl)
	mdb := mock.NewMockDatabase(ctrl)
	mit := mock.NewMockIterator(ctrl)

	m.EXPECT().UseDatabase("db1").Return(mdb)

	db := newDatabase("db1", m)

	type Coll1 struct {
		Key1 string `tigris:"primary_key"`
	}

	c := GetCollection[Coll1](db)

	// Test too many projection arguments in all Read API
	_, err := c.Read(ctx, nil, projection.All, projection.All)
	require.Error(t, err)

	_, err = c.ReadOne(ctx, nil, projection.All, projection.All)
	require.Error(t, err)

	_, err = c.ReadAll(ctx, projection.All, projection.All)
	require.Error(t, err)

	// Iterator error
	var dd driver.Document
	mdb.EXPECT().Read(ctx, "coll_1", driver.Filter(nil), driver.Projection(nil)).Return(mit, nil)
	mit.EXPECT().Next(&dd).Return(false)
	mit.EXPECT().Err().Return(fmt.Errorf("error0"))
	mit.EXPECT().Err().Return(fmt.Errorf("error0"))
	mit.EXPECT().Err().Return(fmt.Errorf("error0"))
	mit.EXPECT().Err().Return(fmt.Errorf("error0"))

	_, err = c.ReadOne(ctx, nil, projection.All)
	require.Error(t, err)

	mdb.EXPECT().Read(ctx, "coll_1", driver.Filter(nil), driver.Projection(nil)).Return(mit, nil)

	it, err := c.Read(ctx, nil, projection.All)
	require.NoError(t, err)

	mit.EXPECT().Err().Return(fmt.Errorf("error1"))
	mit.EXPECT().Err().Return(fmt.Errorf("error1"))

	require.Error(t, it.Err())

	it.err = fmt.Errorf("error2")
	require.False(t, it.Next(nil))

	mdb.EXPECT().Read(ctx, "coll_1", driver.Filter(nil), driver.Projection(nil)).Return(mit, nil)
	mit.EXPECT().Next(&dd).Return(false)
	mit.EXPECT().Err().Return(nil)

	_, err = c.ReadOne(ctx, nil, projection.All)
	require.Equal(t, errNotFound, err)

	mdb.EXPECT().Delete(ctx, "coll_1", driver.Filter(`{"all":{"$eq":"b"}}`)).Return(nil, fmt.Errorf("error"))
	_, err = c.Delete(ctx, filter.Eq("all", "b"))
	require.Error(t, err)

	mdb.EXPECT().Delete(ctx, "coll_1", driver.Filter(`{}`)).Return(nil, fmt.Errorf("error"))
	_, err = c.DeleteAll(ctx)
	require.Error(t, err)

	mdb.EXPECT().Update(ctx, "coll_1", driver.Filter(`{"all":{"$eq":"b"}}`), driver.Update(`{"$set":{"a":123}}`)).Return(nil, fmt.Errorf("error"))
	_, err = c.Update(ctx, filter.Eq("all", "b"), update.Set("a", 123))
	require.Error(t, err)

	var doc = Coll1{Key1: "aaa"}
	mdb.EXPECT().Insert(ctx, "coll_1", []driver.Document{driver.Document(`{"Key1":"aaa"}`)}).Return(nil, fmt.Errorf("error"))
	_, err = c.Insert(ctx, &doc)
	require.Error(t, err)

	mdb.EXPECT().Replace(ctx, "coll_1", []driver.Document{driver.Document(`{"Key1":"aaa"}`)}).Return(nil, fmt.Errorf("error"))
	_, err = c.InsertOrReplace(ctx, &doc)
	require.Error(t, err)
}

func TestCollectionReadOmitEmpty(t *testing.T) {
	ctx := context.TODO()

	ctrl := gomock.NewController(t)
	m := mock.NewMockDriver(ctrl)

	db := newDatabase("db1", m)

	type Coll1 struct {
		Key1   string `tigris:"primary_key"`
		Field1 int64  `json:",omitempty"`
	}

	mdb := mock.NewMockDatabase(ctrl)

	m.EXPECT().UseDatabase("db1").Return(mdb)

	c := GetCollection[Coll1](db)

	d1 := &Coll1{Key1: "aaa", Field1: 123}
	d2 := &Coll1{Key1: "bbb"}

	mit := mock.NewMockIterator(ctrl)

	mdb.EXPECT().Read(ctx, "coll_1",
		driver.Filter(`{}`),
		driver.Projection(nil),
	).Return(mit, nil)

	it, err := c.ReadAll(ctx)
	require.NoError(t, err)

	var d Coll1
	var dd driver.Document

	mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, d1)).Return(true)
	mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, d2)).Return(true)
	mit.EXPECT().Next(&dd).Return(false)
	mit.EXPECT().Err().Return(nil)

	require.True(t, it.Next(&d))
	require.Equal(t, *d1, d)

	require.True(t, it.Next(&d))
	require.Equal(t, *d2, d)

	require.False(t, it.Next(&d))

	require.NoError(t, it.Err())
}

func pm(m proto.Message) gomock.Matcher {
	return &test.ProtoMatcher{Message: m}
}

func TestClientSchemaMigration(t *testing.T) {
	mc, cancel := test.SetupTests(t, 1)
	defer cancel()

	ctx, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	cfg := &config.Database{Driver: config.Driver{URL: test.GRPCURL(1)}}
	cfg.TLS = test.SetupTLS(t)

	driver.DefaultProtocol = driver.GRPC
	drv, err := driver.NewDriver(ctx, &cfg.Driver)
	require.NoError(t, err)

	type testSchema1 struct {
		Key1 string `json:"key_1" tigris:"primary_key"`
	}

	type testSchema2 struct {
		Key2 string `json:"key_2" tigris:"primary_key"`
	}

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().CreateDatabase(gomock.Any(),
		pm(&api.CreateDatabaseRequest{
			Db:      "db1",
			Options: &api.DatabaseOptions{},
		})).Return(&api.CreateDatabaseResponse{}, nil)

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Db:      "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Db: "db1", Collection: "test_schema_1",
			Schema:  []byte(`{"title":"test_schema_1","properties":{"key_1":{"type":"string"}},"primary_key":["key_1"]}`),
			Options: &api.CollectionOptions{TxCtx: txCtx},
		})).Do(func(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) {
	}).Return(&api.CreateOrUpdateCollectionResponse{}, nil)

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Db:    "db1",
			TxCtx: txCtx,
		})).Return(&api.CommitTransactionResponse{}, nil)

	_, err = openDatabaseFromModels(ctx, drv, &config.Database{}, "db1", &testSchema1{})
	require.NoError(t, err)

	mc.EXPECT().CreateDatabase(gomock.Any(),
		pm(&api.CreateDatabaseRequest{
			Db:      "db1",
			Options: &api.DatabaseOptions{},
		})).Return(&api.CreateDatabaseResponse{}, nil)

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Db:      "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Db: "db1", Collection: "test_schema_2",
			Schema:  []byte(`{"title":"test_schema_2","properties":{"key_2":{"type":"string"}},"primary_key":["key_2"]}`),
			Options: &api.CollectionOptions{TxCtx: txCtx},
		})).Do(func(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) {
	}).Return(&api.CreateOrUpdateCollectionResponse{}, nil)

	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Db: "db1", Collection: "test_schema_1",
			Schema:  []byte(`{"title":"test_schema_1","properties":{"key_1":{"type":"string"}},"primary_key":["key_1"]}`),
			Options: &api.CollectionOptions{TxCtx: txCtx},
		})).Do(func(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) {
	}).Return(&api.CreateOrUpdateCollectionResponse{}, nil)

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Db:    "db1",
			TxCtx: txCtx,
		})).Return(&api.CommitTransactionResponse{}, nil)

	_, err = openDatabaseFromModels(ctx, drv, &config.Database{}, "db1", &testSchema1{}, &testSchema2{})
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

	type Coll1 struct {
		Key1 string `tigris:"primary_key"`
	}

	mc.EXPECT().CreateDatabase(gomock.Any(),
		pm(&api.CreateDatabaseRequest{
			Db:      "db1",
			Options: &api.DatabaseOptions{},
		})).Return(&api.CreateDatabaseResponse{}, nil)

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Db:      "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Db: "db1", Collection: "coll_1",
			Schema:  []byte(`{"title":"coll_1","properties":{"Key1":{"type":"string"}},"primary_key":["Key1"]}`),
			Options: &api.CollectionOptions{TxCtx: txCtx},
		})).Do(func(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) {
	}).Return(&api.CreateOrUpdateCollectionResponse{}, nil)

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Db:    "db1",
			TxCtx: txCtx,
		})).Return(&api.CommitTransactionResponse{}, nil)

	db, err := OpenDatabase(ctx, cfg, "db1", &Coll1{})
	require.NoError(t, err)
	require.NotNil(t, db)

	mc.EXPECT().DropDatabase(gomock.Any(),
		pm(&api.DropDatabaseRequest{
			Db:      "db1",
			Options: &api.DatabaseOptions{},
		})).Return(&api.DropDatabaseResponse{}, nil)

	err = db.Drop(ctx)
	require.NoError(t, err)
}
