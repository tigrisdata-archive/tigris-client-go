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

package tigris

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/code"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/fields"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/mock"
)

func TestCollectionTx(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	m := mock.NewMockDriver(ctrl)
	mdb := mock.NewMockDatabase(ctrl)
	mtx := mock.NewMockTx(ctrl)

	type Coll1 struct {
		Key1   string `tigris:"primary_key"`
		Field1 int64
	}

	db := newDatabase("db1", m)
	m.EXPECT().UseDatabase("db1").Return(mdb)

	mdb.EXPECT().BeginTx(gomock.Any()).Return(mtx, nil)

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
			fields.Set("Field1", 345),
		)
		require.NoError(t, err)

		mit := mock.NewMockIterator(ctrl)

		mtx.EXPECT().Read(ctx, "coll_1",
			driver.Filter(`{"$or":[{"Key1":{"$eq":"aaa"}},{"Key1":{"$eq":"ccc"}}]}`),
			jm(t, `{"Field1":true,"Key1":false}`),
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

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, d1)).Return(true)
		mit.EXPECT().Next(&dd1).Return(false)
		mit.EXPECT().Err().Return(nil)

		for it.Next(&d) {
			require.Equal(t, *d1, d)
		}

		require.NoError(t, it.Err())

		mit.EXPECT().Close()
		it.Close()

		mtx.EXPECT().Read(ctx, "coll_1", driver.Filter(`{}`), driver.Projection(`{}`)).Return(mit, nil)

		_, err = c.ReadAll(ctx, fields.All)
		require.NoError(t, err)

		mtx.EXPECT().Delete(ctx, "coll_1", driver.Filter(`{}`))

		_, err = c.DeleteAll(ctx)
		require.NoError(t, err)

		mtx.EXPECT().Delete(ctx, "coll_1",
			driver.Filter(`{"$or":[{"Key1":{"$eq":"aaa"}},{"Key1":{"$eq":"ccc"}}]}`))

		_, err = c.Delete(ctx, filter.Or(
			filter.Eq("Key1", "aaa"),
			filter.Eq("Key1", "ccc")))
		require.NoError(t, err)

		mtx.EXPECT().Read(ctx, "coll_1",
			driver.Filter(`{"Key1":{"$eq":"aaa"}}`),
			driver.Projection(nil),
		).Return(mit, nil)

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, d1)).Return(true)
		mit.EXPECT().Close()

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

		mtx.EXPECT().Commit(gomock.Any())
		mtx.EXPECT().Rollback(gomock.Any())

		return nil
	})
	require.NoError(t, err)
}

func TestCollectionTxRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	m := mock.NewMockDriver(ctrl)
	mtx := mock.NewMockTx(ctrl)
	mdb := mock.NewMockDatabase(ctrl)
	db := newDatabase("db1", m)

	t.Run("retry", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(7)
		mdb.EXPECT().BeginTx(gomock.Any()).Return(mtx, nil).Times(7)

		i := 7
		err := db.Tx(ctx, func(ctx context.Context) error {
			mtx.EXPECT().Commit(gomock.Any()).DoAndReturn(
				func(ctx context.Context) error {
					i--
					if i > 0 {
						return &driver.Error{TigrisError: api.Errorf(code.Conflict, "error %v", i).
							WithRetry(time.Duration(i) * time.Millisecond)}
					}

					return nil
				})
			mtx.EXPECT().Rollback(gomock.Any())

			return nil
		}, TxOptions{AutoRetry: true})
		require.NoError(t, err)
		assert.Equal(t, 0, i)
	})

	t.Run("no_retry", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb)
		mdb.EXPECT().BeginTx(gomock.Any()).Return(mtx, nil)

		i := 7
		err := db.Tx(ctx, func(ctx context.Context) error {
			mtx.EXPECT().Commit(gomock.Any()).DoAndReturn(
				func(ctx context.Context) error {
					i--
					if i > 0 {
						return &driver.Error{TigrisError: api.Errorf(code.Conflict, "error %v", i).
							WithRetry(time.Duration(i) * time.Millisecond)}
					}

					return nil
				})
			mtx.EXPECT().Rollback(gomock.Any())

			return nil
		})
		var te Error
		require.Error(t, fmt.Errorf("error 6"), err)
		assert.True(t, errors.As(err, &te))
		assert.Equal(t, code.Conflict, te.Code)
		assert.Equal(t, 6*time.Millisecond, te.RetryDelay())
		assert.Equal(t, 6, i)
	})

	t.Run("not_retryable_tigris_error", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb)
		mdb.EXPECT().BeginTx(gomock.Any()).Return(mtx, nil)

		i := 7
		err := db.Tx(ctx, func(ctx context.Context) error {
			mtx.EXPECT().Commit(gomock.Any()).DoAndReturn(
				func(ctx context.Context) error {
					i--
					if i > 0 {
						return &driver.Error{TigrisError: api.Errorf(code.Conflict, "error %v", i)}
					}

					return nil
				})
			mtx.EXPECT().Rollback(gomock.Any())

			return nil
		})
		var te *driver.Error
		require.Error(t, fmt.Errorf("error 6"), err)
		assert.True(t, errors.As(err, &te))
		assert.Equal(t, code.Conflict, te.Code)
		assert.Equal(t, time.Duration(0), te.RetryDelay())
		assert.Equal(t, 6, i)
	})
}
