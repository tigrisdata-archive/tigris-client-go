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
	"errors"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tigrisdata/tigris-client-go/search"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/fields"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/mock"
	"github.com/tigrisdata/tigris-client-go/schema"
	"github.com/tigrisdata/tigris-client-go/sort"
	"github.com/tigrisdata/tigris-client-go/test"
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

func createSearchResponse(t *testing.T, doc interface{}) driver.SearchResponse {
	d, err := json.Marshal(doc)
	require.NoError(t, err)
	tm := time.Now()
	return &api.SearchResponse{
		Hits: []*api.SearchHit{{
			Data: d,
			Metadata: &api.SearchHitMeta{
				CreatedAt: timestamppb.New(tm),
			},
		}},
		Facets: nil,
		Meta: &api.SearchMetadata{
			Found:      30,
			TotalPages: 2,
			Page: &api.Page{
				Current: 2,
				Size:    15,
			},
		},
	}
}

func TestGetSearchRequest(t *testing.T) {
	t.Run("with all params", func(t *testing.T) {
		in := search.NewRequestBuilder().
			WithQuery("search query").
			WithSearchFields("field_1").
			WithFilter(filter.Eq("field_2", "some value")).
			WithFacet(search.NewFacetQueryBuilder().WithFields("field_3").Build()).
			WithExcludeFields("field_5").
			WithOptions(&search.DefaultSearchOptions).
			Build()
		out, err := getSearchRequest(in)
		assert.Nil(t, err)
		assert.NotNil(t, out)
		assert.Equal(t, in.Q, out.Q)
		assert.Equal(t, in.SearchFields, out.SearchFields)
		assert.Equal(t, driver.Filter(`{"field_2":{"$eq":"some value"}}`), out.Filter)
		assert.Equal(t, driver.Facet(`{"field_3":{"size":10}}`), out.Facet)
		assert.Equal(t, in.ExcludeFields, out.ExcludeFields)
		assert.Empty(t, out.IncludeFields)
		assert.Equal(t, in.Options.Page, out.Page)
		assert.Equal(t, in.Options.PageSize, out.PageSize)
	})

	t.Run("with nil request", func(t *testing.T) {
		out, err := getSearchRequest(nil)
		assert.Nil(t, out)
		assert.NotNil(t, err)
	})

	t.Run("with nil fields", func(t *testing.T) {
		in := search.NewRequestBuilder().
			WithSearchFields("field_1").
			Build()
		out, err := getSearchRequest(in)
		assert.Nil(t, err)
		assert.NotNil(t, out)
		assert.Equal(t, "", out.Q)
		assert.Equal(t, []string{"field_1"}, out.SearchFields)
		assert.Equal(t, int32(0), out.Page)
		assert.Equal(t, int32(0), out.PageSize)
		assert.Nil(t, out.Filter)
		assert.Nil(t, out.Facet)
		assert.Empty(t, out.IncludeFields)
		assert.Empty(t, out.ExcludeFields)
	})
}

func TestCollectionBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
		fields.Set("Field1", 345),
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
		fields.Exclude("Key1").
			Include("Field1"),
	)
	require.NoError(t, err)

	var d Coll1
	var dd driver.Document

	mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, d1)).Return(true)
	mit.EXPECT().Next(&dd).Return(false)
	mit.EXPECT().Err().Return(nil)

	for it.Next(&d) {
		require.Equal(t, *d1, d)
	}

	require.NoError(t, it.Err())

	mit.EXPECT().Close()
	it.Close()

	mdb.EXPECT().Read(ctx, "coll_1", driver.Filter(`{}`), driver.Projection(`{}`)).Return(mit, nil)

	_, err = c.ReadAll(ctx, fields.All)
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
	mit.EXPECT().Close()

	pd, err := c.ReadOne(ctx, filter.Eq("Key1", "aaa"))
	require.NoError(t, err)
	require.Equal(t, d1, pd)

	mdb.EXPECT().DropCollection(ctx, "coll_1")

	err = c.Drop(ctx)
	require.NoError(t, err)
}

func TestCollection_Search(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	m := mock.NewMockDriver(ctrl)

	m.EXPECT().CreateDatabase(gomock.Any(), "db1")

	type Coll1 struct {
		Key1   string `tigris:"primary_key"`
		Field1 int64
	}

	mdb := mock.NewMockDatabase(ctrl)
	mtx := mock.NewMockTx(ctrl)

	m.EXPECT().BeginTx(gomock.Any(), "db1").Return(mtx, nil)

	mtx.EXPECT().CreateOrUpdateCollection(gomock.Any(), "coll_1", jm(t, `{"title":"coll_1","properties":{"Key1":{"type":"string"},"Field1":{"type":"integer"}},"primary_key":["Key1"]}`))
	mtx.EXPECT().Commit(ctx)
	mtx.EXPECT().Rollback(ctx)

	db, err := openDatabaseFromModels(ctx, m, &config.Database{}, "db1", &Coll1{})
	require.NoError(t, err)

	m.EXPECT().UseDatabase("db1").Return(mdb)

	c := GetCollection[Coll1](db)

	// search with all params parses completely
	t.Run("with all request params", func(t *testing.T) {
		rit := mock.NewMockSearchResultIterator(ctrl)
		sr := search.NewRequestBuilder().
			WithQuery("search query").
			WithSearchFields("field_1").
			WithFilter(filter.Eq("field_2", "some value")).
			WithSorting(sort.Ascending("field_1"), sort.Descending("field_2")).
			WithFacet(search.NewFacetQueryBuilder().WithFields("field_3").Build()).
			WithIncludeFields("field_4").
			WithOptions(&search.DefaultSearchOptions).
			Build()
		mdb.EXPECT().Search(ctx, "coll_1", &driver.SearchRequest{
			Q:             sr.Q,
			SearchFields:  sr.SearchFields,
			Filter:        driver.Filter(`{"field_2":{"$eq":"some value"}}`),
			Facet:         driver.Facet(`{"field_3":{"size":10}}`),
			Sort:          driver.SortOrder(`[{"field_1":"$asc"},{"field_2":"$desc"}]`),
			IncludeFields: []string{"field_4"},
			ExcludeFields: nil,
			Page:          sr.Options.Page,
			PageSize:      sr.Options.PageSize,
		}).Return(rit, nil)
		searchIter, err := c.Search(ctx, sr)
		require.NoError(t, err)
		require.NotNil(t, searchIter)

		// mock search response and validate iterator conversion
		var r driver.SearchResponse
		d1 := &Coll1{Key1: "aaa", Field1: 123}
		rit.EXPECT().Next(&r).SetArg(0, createSearchResponse(t, d1)).Return(true)
		rit.EXPECT().Next(&r).Return(false)
		rit.EXPECT().Err().Return(nil)

		var rs search.Result[Coll1]
		for searchIter.Next(&rs) {
			require.Equal(t, d1, rs.Hits[0].Document)
		}
		require.Nil(t, rit.Err())
	})

	t.Run("with partial request params", func(t *testing.T) {
		rit := mock.NewMockSearchResultIterator(ctrl)
		sr := search.NewRequestBuilder().Build()
		mdb.EXPECT().Search(ctx, "coll_1", &driver.SearchRequest{
			Q:             sr.Q,
			SearchFields:  []string{},
			Filter:        nil,
			Facet:         nil,
			Sort:          nil,
			IncludeFields: nil,
			ExcludeFields: nil,
			Page:          int32(0),
			PageSize:      int32(0),
		}).Return(rit, nil)
		searchIter, err := c.Search(ctx, sr)
		require.NoError(t, err)
		require.NotNil(t, searchIter)
	})

	// search with nil req
	t.Run("with nil req", func(t *testing.T) {
		searchIter, err := c.Search(ctx, nil)
		require.NotNil(t, err)
		require.ErrorContains(t, err, "cannot be null")
		require.Nil(t, searchIter)
	})

	// with marshalling failure
	t.Run("when response unmarshalling fails", func(t *testing.T) {
		rit := mock.NewMockSearchResultIterator(ctrl)
		sr := search.NewRequestBuilder().Build()
		mdb.EXPECT().Search(ctx, "coll_1", gomock.Any()).Return(rit, nil)
		searchIter, err := c.Search(ctx, sr)
		require.NoError(t, err)

		var r driver.SearchResponse
		// conversion will fail as Field1 is supposed to be int
		d1 := `{Key1: "aaa", Field1: "123"}`
		rit.EXPECT().Next(&r).SetArg(0, createSearchResponse(t, d1)).Return(true)
		rit.EXPECT().Close()

		var rs search.Result[Coll1]
		require.Nil(t, searchIter.err)
		require.False(t, searchIter.Next(&rs))
		require.NotNil(t, searchIter.err)
		require.ErrorContains(t, searchIter.err, "cannot unmarshal string")
	})
}

func TestCollectionNegative(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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

	// Test too many fields arguments in all Read API
	_, err := c.Read(ctx, nil, fields.All, fields.All)
	require.Error(t, err)

	mit.EXPECT().Close()
	_, err = c.ReadOne(ctx, nil, fields.All, fields.All)
	require.Error(t, err)

	_, err = c.ReadAll(ctx, fields.All, fields.All)
	require.Error(t, err)

	// Iterator error
	var dd driver.Document
	mdb.EXPECT().Read(ctx, "coll_1", driver.Filter(nil), driver.Projection(`{}`)).Return(mit, nil)
	mit.EXPECT().Next(&dd).Return(false)
	mit.EXPECT().Err().Return(fmt.Errorf("error0"))
	mit.EXPECT().Err().Return(fmt.Errorf("error0"))
	mit.EXPECT().Err().Return(fmt.Errorf("error0"))
	mit.EXPECT().Err().Return(fmt.Errorf("error0"))

	mit.EXPECT().Close()
	_, err = c.ReadOne(ctx, nil, fields.All)
	require.Error(t, err)

	mdb.EXPECT().Read(ctx, "coll_1", driver.Filter(nil), driver.Projection(`{}`)).Return(mit, nil)

	it, err := c.Read(ctx, nil, fields.All)
	require.NoError(t, err)

	mit.EXPECT().Err().Return(fmt.Errorf("error1"))
	mit.EXPECT().Err().Return(fmt.Errorf("error1"))

	require.Error(t, it.Err())

	it.err = fmt.Errorf("error2")
	require.False(t, it.Next(nil))

	mdb.EXPECT().Read(ctx, "coll_1", driver.Filter(nil), driver.Projection(`{}`)).Return(mit, nil)
	mit.EXPECT().Next(&dd).Return(false)
	mit.EXPECT().Err().Return(nil)

	_, err = c.ReadOne(ctx, nil, fields.All)
	require.Equal(t, errNotFound, err)

	// Test that driver.Error is converted to tigris.Error
	// by using driver.Error.As and tigris.Error.AsTigrisError interfaces
	mdb.EXPECT().Delete(ctx, "coll_1", driver.Filter(`{"all":{"$eq":"b"}}`)).Return(nil, &driver.Error{TigrisError: &api.TigrisError{Code: api.Code_CONFLICT}})
	_, err = c.Delete(ctx, filter.Eq("all", "b"))
	require.Error(t, err)
	var te Error
	require.True(t, errors.As(err, &te))

	mdb.EXPECT().Delete(ctx, "coll_1", driver.Filter(`{}`)).Return(nil, fmt.Errorf("error"))
	_, err = c.DeleteAll(ctx)
	require.Error(t, err)

	mdb.EXPECT().Update(ctx, "coll_1", driver.Filter(`{"all":{"$eq":"b"}}`), driver.Update(`{"$set":{"a":123}}`)).Return(nil, fmt.Errorf("error"))
	_, err = c.Update(ctx, filter.Eq("all", "b"), fields.Set("a", 123))
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
	ms, cancel := test.SetupTests(t, 6)
	defer cancel()

	mc := ms.Api

	ctx, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	cfg := &config.Database{Driver: config.Driver{URL: test.GRPCURL(6)}}
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
			Options: &api.CollectionOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) (*api.CreateOrUpdateCollectionResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))
			return &api.CreateOrUpdateCollectionResponse{}, nil
		})

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Db: "db1",
		})).DoAndReturn(
		func(ctx context.Context, r *api.CommitTransactionRequest) (*api.CommitTransactionResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))
			return &api.CommitTransactionResponse{}, nil
		})

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
			Options: &api.CollectionOptions{},
		})).DoAndReturn(
		func(ctx context.Context, _ *api.CreateOrUpdateCollectionRequest) (*api.CreateOrUpdateCollectionResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))
			return &api.CreateOrUpdateCollectionResponse{}, nil
		})

	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Db: "db1", Collection: "test_schema_1",
			Schema:  []byte(`{"title":"test_schema_1","properties":{"key_1":{"type":"string"}},"primary_key":["key_1"]}`),
			Options: &api.CollectionOptions{},
		})).DoAndReturn(
		func(ctx context.Context, _ *api.CreateOrUpdateCollectionRequest) (*api.CreateOrUpdateCollectionResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))
			return &api.CreateOrUpdateCollectionResponse{}, nil
		})

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Db: "db1",
		})).DoAndReturn(
		func(ctx context.Context, _ *api.CommitTransactionRequest) (*api.CommitTransactionResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))
			return &api.CommitTransactionResponse{}, nil
		})

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
			Options: &api.CollectionOptions{},
		})).Do(func(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) {
	}).Return(&api.CreateOrUpdateCollectionResponse{}, nil)

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Db: "db1",
		})).Return(&api.CommitTransactionResponse{}, nil)

	db, err := OpenDatabase(ctx, cfg, "db1", &Coll1{})
	require.NoError(t, err)
	require.NotNil(t, db)

	mc.EXPECT().DropDatabase(gomock.Any(),
		pm(&api.DropDatabaseRequest{
			Db:      "db1",
			Options: &api.DatabaseOptions{},
		})).Return(&api.DropDatabaseResponse{}, nil)

	err = DropDatabase(ctx, cfg, "db1")
	require.NoError(t, err)
}

func TestCollection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	m := mock.NewMockDriver(ctrl)
	mdb := mock.NewMockDatabase(ctrl)

	mit := mock.NewMockIterator(ctrl)

	db := newDatabase("db1", m)

	type Coll1 struct {
		Key1   string `tigris:"primary_key"`
		Field1 int64
	}

	m.EXPECT().UseDatabase("db1").Return(mdb)

	c := GetCollection[Coll1](db)

	t.Run("read_limit_skip_offset", func(t *testing.T) {
		mdb.EXPECT().Read(ctx, "coll_1",
			driver.Filter(`{"$or":[{"Key1":{"$eq":"aaa"}},{"Key1":{"$eq":"ccc"}}]}`),
			driver.Projection(`{"Field1":true,"Key1":false}`),
			&driver.ReadOptions{
				Limit:  111,
				Skip:   222,
				Offset: []byte("333"),
			},
		).Return(mit, nil)

		it, err := c.ReadWithOptions(ctx, filter.Or(
			filter.Eq("Key1", "aaa"),
			filter.Eq("Key1", "ccc")),
			fields.Exclude("Key1").
				Include("Field1"),
			&ReadOptions{
				Limit:  111,
				Skip:   222,
				Offset: []byte("333"),
			},
		)
		require.NoError(t, err)

		var d Coll1
		var dd driver.Document
		var dd1 driver.Document

		d1 := &Coll1{Key1: "aaa", Field1: 123}

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, d1)).Return(true)
		mit.EXPECT().Next(&dd1).Return(false)
		mit.EXPECT().Err().Return(nil)

		for it.Next(&d) {
			require.Equal(t, *d1, d)
		}

		require.NoError(t, it.Err())

		mit.EXPECT().Close()
		it.Close()
	})

	t.Run("read_with_options_filter_all", func(t *testing.T) {
		mdb.EXPECT().Read(ctx, "coll_1",
			driver.Filter(`{}`),
			driver.Projection(`{}`),
			&driver.ReadOptions{
				Limit:  111,
				Skip:   222,
				Offset: []byte("333"),
			},
		).Return(mit, nil)

		_, err := c.ReadWithOptions(ctx, filter.All,
			fields.All,
			&ReadOptions{
				Limit:  111,
				Skip:   222,
				Offset: []byte("333"),
			},
		)
		require.NoError(t, err)
	})
}
