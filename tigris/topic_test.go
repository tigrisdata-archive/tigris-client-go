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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/mock"
)

func toMessage(t *testing.T, doc interface{}) driver.Message {
	b, err := json.Marshal(doc)
	require.NoError(t, err)
	return b
}

func TestTopicBasic(t *testing.T) {
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
		Key1   string
		Field1 int64
	}

	mdb := mock.NewMockDatabase(ctrl)
	mtx := mock.NewMockTx(ctrl)

	m.EXPECT().BeginTx(gomock.Any(), "db1").Return(mtx, nil)

	mtx.EXPECT().CreateOrUpdateCollection(gomock.Any(), "coll_1", jm(t, `{"title":"coll_1","properties":{"Key1":{"type":"string"},"Field1":{"type":"integer"}},"primary_key":["Key1"],"collection_type":"documents"}`))
	mtx.EXPECT().Commit(ctx)
	mtx.EXPECT().Rollback(ctx)

	db, err := openDatabaseFromModels(ctx, m, &Config{}, "db1", &Coll1{})
	require.NoError(t, err)

	m.EXPECT().BeginTx(gomock.Any(), "db1").Return(mtx, nil)

	mtx.EXPECT().CreateOrUpdateCollection(gomock.Any(), "coll_2", jm(t, `{"title":"coll_2","properties":{"Key1":{"type":"string"},"Field1":{"type":"integer"}},"collection_type":"messages"}`))
	mtx.EXPECT().Commit(ctx)
	mtx.EXPECT().Rollback(ctx)

	err = db.CreateTopics(ctx, &Coll2{})
	require.NoError(t, err)

	m.EXPECT().UseDatabase("db1").Return(mdb)

	c := GetTopic[Coll2](db)

	d1 := &Coll2{Key1: "aaa", Field1: 123}
	d2 := &Coll2{Key1: "bbb", Field1: 123}

	mdb.EXPECT().Publish(ctx, "coll_2", []driver.Message{toMessage(t, d1), toMessage(t, d2)})

	_, err = c.Publish(ctx, d1, d2)
	require.NoError(t, err)

	mit := mock.NewMockIterator(ctrl)

	mdb.EXPECT().Subscribe(ctx, "coll_2",
		driver.Filter(`{"$or":[{"Key1":{"$eq":"aaa"}},{"Key1":{"$eq":"ccc"}}]}`),
	).Return(mit, nil)

	it, err := c.Subscribe(ctx, filter.Or(
		filter.Eq("Key1", "aaa"),
		filter.Eq("Key1", "ccc")),
	)
	require.NoError(t, err)

	var d Coll2
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
}

func TestTopicNegative(t *testing.T) {
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

	c := GetTopic[Coll1](db)

	// Iterator error
	var dd driver.Document
	mdb.EXPECT().Subscribe(ctx, "coll_1", driver.Filter(nil)).Return(mit, nil)
	mit.EXPECT().Next(&dd).Return(false)
	mit.EXPECT().Err().Return(fmt.Errorf("error0"))
	mit.EXPECT().Err().Return(fmt.Errorf("error0"))

	mit.EXPECT().Close()

	it, err := c.Subscribe(ctx, nil)
	require.NoError(t, err)

	var d Coll1
	require.False(t, it.Next(&d))
	require.Error(t, it.Err())

	mit.EXPECT().Err().Return(fmt.Errorf("error1"))
	mit.EXPECT().Err().Return(fmt.Errorf("error1"))

	require.Error(t, it.Err())

	it.err = fmt.Errorf("error2")
	require.False(t, it.Next(nil))

	it.Close()

	type Coll2 struct {
		Key1   string `tigris:"primary_key"`
		Field1 int64
	}

	err = db.CreateTopics(ctx, &Coll2{})
	require.Error(t, err)
}
