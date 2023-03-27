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

package internal

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/mock"
	"github.com/tigrisdata/tigris-client-go/tigris"
	"github.com/tigrisdata/tigris-client-go/util"
)

//go:generate go run github.com/tigrisdata/tigrisgen

var jm = driver.JM

type nColl1 struct {
	Key1   string `tigris:"primary_key"`
	Field1 int64
}

func testFilterRead(d nColl1, arg string) bool {
	return d.Key1 == "read" || d.Key1 == arg
}

func testFilterReadOne(d nColl1, arg string) bool {
	return d.Key1 == "read_one" || d.Key1 == arg
}

func testFilterReadWithOptions(d nColl1, arg string) bool {
	return d.Key1 == "read_with_options" || d.Key1 == arg
}

func testFilterUpdateOne(d nColl1, arg string) bool {
	return d.Key1 == "update_one" || d.Key1 == arg
}

func testFilterUpdate(d nColl1, arg string) bool {
	return d.Key1 == "update" || d.Key1 == arg
}

func testFilterDeleteOne(d nColl1, arg string) bool {
	return d.Key1 == "delete_one" || d.Key1 == arg
}

func testFilterDelete(d nColl1, arg string) bool {
	return d.Key1 == "delete" || d.Key1 == arg
}

func testUpdateOne(d nColl1, arg int64) {
	d.Field1 += arg
}

func testUpdate(d nColl1, arg int64) {
	d.Field1 = arg
}

func testUpdateAll(d nColl1, arg int64) {
	d.Field1 = arg
}

func TestNativeCollection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	m := mock.NewMockDriver(ctrl)
	mdb := mock.NewMockDatabase(ctrl)

	mit := mock.NewMockIterator(ctrl)

	m.EXPECT().UseDatabase("db1").Return(mdb)

	mdb.EXPECT().CreateOrUpdateCollections(gomock.Any(), driver.JAM(t,
		[]string{`{"title":"n_coll_1","properties":{
			"Key1":{"type":"string"},
			"Field1":{"type":"integer"}},
			"primary_key":["Key1"],
			"collection_type":"documents"}`}))

	db, err := tigris.TestOpenDatabase(ctx, m, "db1", &nColl1{})
	require.NoError(t, err)

	m.EXPECT().UseDatabase("db1").Return(mdb)
	m.EXPECT().UseDatabase("db1").Return(mdb)

	c := tigris.GetNativeCollection[nColl1](db)
	c1 := tigris.GetNativeProjection[nColl1, nColl1](db)

	t.Run("read_projection", func(t *testing.T) {
		mdb.EXPECT().Read(ctx, "n_coll_1",
			jm(t, `{"$or":[{"Key1":"read"},{"Key1":"read1"}]}`),
			jm(t, `{}`),
		).Return(mit, nil)

		_, err = tigris.Read(ctx, c1, testFilterRead, "read1")
		require.NoError(t, err)
	})

	t.Run("read", func(t *testing.T) {
		mdb.EXPECT().Read(ctx, "n_coll_1",
			jm(t, `{"$or":[{"Key1":"read"},{"Key1":"read1"}]}`),
			jm(t, `{}`),
		).Return(mit, nil)

		_, err = tigris.Read(ctx, c, testFilterRead, "read1")
		require.NoError(t, err)
	})

	t.Run("read_one", func(t *testing.T) {
		mdb.EXPECT().Read(ctx, "n_coll_1",
			jm(t, `{"$or":[{"Key1":"read_one"},{"Key1":"read_one1"}]}`),
			jm(t, `{}`),
			&driver.ReadOptions{Limit: 1},
		).Return(mit, nil)

		var (
			d  = nColl1{Key1: "key1", Field1: 33}
			dd driver.Document
		)

		mit.EXPECT().Next(&dd).SetArg(0, driver.ToDocument(t, d)).Return(true)
		mit.EXPECT().Close()

		res, err := tigris.ReadOne(ctx, c, testFilterReadOne, "read_one1")
		require.NoError(t, err)
		require.Equal(t, &d, res)
	})

	t.Run("read_all", func(t *testing.T) {
		mdb.EXPECT().Read(ctx, "n_coll_1",
			jm(t, `{}`),
			jm(t, `{}`),
		).Return(mit, nil)

		_, err = tigris.ReadAll(ctx, c)
		require.NoError(t, err)
	})

	t.Run("read_with_options", func(t *testing.T) {
		mdb.EXPECT().Read(ctx, "n_coll_1",
			jm(t, `{"$or":[{"Key1":"read_with_options"},{"Key1":"read_with_options1"}]}`),
			jm(t, `{}`),
			&driver.ReadOptions{
				Limit:  111,
				Skip:   222,
				Offset: []byte("333"),
			},
		).Return(mit, nil)

		_, err = tigris.ReadWithOptions(ctx, c, testFilterReadWithOptions, "read_with_options1",
			&tigris.ReadOptions{
				Limit:  111,
				Skip:   222,
				Offset: []byte("333"),
			},
		)
		require.NoError(t, err)
	})

	t.Run("update", func(t *testing.T) {
		mdb.EXPECT().Update(ctx, "n_coll_1",
			jm(t, `{"$or":[{"Key1":"update"},{"Key1":"update1"}]}`),
			jm(t, `{"$set":{"Field1":11}}`),
		)

		_, err := tigris.Update(ctx, c, testFilterUpdate, testUpdate, "update1", 11)
		require.NoError(t, err)
	})

	t.Run("update_one", func(t *testing.T) {
		mdb.EXPECT().Update(ctx, "n_coll_1",
			jm(t, `{"$or":[{"Key1":"update_one"},{"Key1":"update_one1"}]}`),
			jm(t, `{"$increment":{"Field1":10}}`),
			&driver.UpdateOptions{Limit: 1},
		)

		_, err := tigris.UpdateOne(ctx, c, testFilterUpdateOne, testUpdateOne, "update_one1", 10)
		require.NoError(t, err)
	})

	t.Run("update_all", func(t *testing.T) {
		mdb.EXPECT().Update(ctx, "n_coll_1",
			jm(t, `{}`),
			jm(t, `{"$set":{"Field1":22}}`),
		)

		_, err := tigris.UpdateAll(ctx, c, testUpdateAll, 22)
		require.NoError(t, err)
	})

	t.Run("delete", func(t *testing.T) {
		mdb.EXPECT().Delete(ctx, "n_coll_1",
			jm(t, `{"$or":[{"Key1":"delete"},{"Key1":"delete1"}]}`),
		)

		_, err := tigris.Delete(ctx, c, testFilterDelete, "delete1")
		require.NoError(t, err)
	})

	t.Run("delete_one", func(t *testing.T) {
		mdb.EXPECT().Delete(ctx, "n_coll_1",
			jm(t, `{"$or":[{"Key1":"delete_one"},{"Key1":"delete1"}]}`),
			&driver.DeleteOptions{Limit: 1},
		)

		_, err = tigris.DeleteOne(ctx, c, testFilterDeleteOne, "delete1")
		require.NoError(t, err)
	})

	t.Run("delete_all", func(t *testing.T) {
		mdb.EXPECT().Delete(ctx, "n_coll_1", jm(t, `{}`))

		_, err = tigris.DeleteAll(ctx, c)
		require.NoError(t, err)
	})

	t.Run("drop", func(t *testing.T) {
		mdb.EXPECT().DropCollection(ctx, "n_coll_1")

		err = c.Drop(ctx)
		require.NoError(t, err)
	})

	t.Run("insert", func(t *testing.T) {
		mdb.EXPECT().Insert(ctx, "n_coll_1",
			driver.JAM(t, []string{`{"Field1":88,"Key1":"key1"}`}),
		)

		_, err = c.Insert(ctx, &nColl1{
			Key1:   "key1",
			Field1: 88,
		})
		require.NoError(t, err)
	})

	t.Run("insert_or_replace", func(t *testing.T) {
		mdb.EXPECT().Replace(ctx, "n_coll_1",
			driver.JAM(t, []string{`{"Field1":99,"Key1":"key2"}`}),
		)

		_, err = c.InsertOrReplace(ctx, &nColl1{
			Key1:   "key2",
			Field1: 99,
		})
		require.NoError(t, err)
	})
}

func TestMain(m *testing.M) {
	util.Configure(util.LogConfig{Level: "info", Format: "console"})

	os.Exit(m.Run())
}
