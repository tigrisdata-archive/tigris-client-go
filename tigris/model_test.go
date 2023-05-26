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
	jsoniter "github.com/json-iterator/go"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/mock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestModelMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	m := mock.NewMockDriver(ctrl)
	mdb := mock.NewMockDatabase(ctrl)

	db := newDatabase("db1", m)

	id1 := "11111111-00b6-4eb5-a64d-351be56afe36"
	id2 := "22222222-00b6-4eb5-a64d-351be56afe36"
	tm := time.Now().UTC()
	tm1 := tm.Add(1 * time.Second)
	tm2 := tm.Add(2 * time.Second)

	t.Run("embedded_model", func(t *testing.T) {
		type Coll1 struct {
			Model
		}

		m.EXPECT().UseDatabase("db1").Return(mdb)

		c := GetCollection[Coll1](db)

		d1 := &Coll1{}
		d2 := &Coll1{}

		mdb.EXPECT().Insert(ctx, "coll_1",
			[]driver.Document{toDocument(t, d1), toDocument(t, d2)}).
			Return(&driver.InsertResponse{
				Metadata: &api.ResponseMetadata{
					CreatedAt: timestamppb.New(tm),
					UpdatedAt: timestamppb.New(tm1),
					DeletedAt: timestamppb.New(tm2),
				},
				Keys: [][]byte{
					[]byte(`{"id":"` + id1 + `"}`),
					[]byte(`{"Id":"` + id2 + `"}`),
				},
			}, nil)

		_, err := c.Insert(ctx, d1, d2)
		require.NoError(t, err)

		require.Equal(t, &Coll1{Model{ID: uuid.MustParse(id1), Metadata: Metadata{createdAt: tm, updatedAt: tm1, deletedAt: tm2}}}, d1)
		require.Equal(t, &Coll1{Model{ID: uuid.MustParse(id2), Metadata: Metadata{createdAt: tm, updatedAt: tm1, deletedAt: tm2}}}, d2)

		d1 = &Coll1{}
		d2 = &Coll1{}

		mdb.EXPECT().Replace(ctx, "coll_1",
			[]driver.Document{toDocument(t, d1), toDocument(t, d2)}).
			Return(&driver.ReplaceResponse{
				Metadata: &api.ResponseMetadata{
					CreatedAt: timestamppb.New(tm),
					UpdatedAt: timestamppb.New(tm1),
					DeletedAt: timestamppb.New(tm2),
				},
				Keys: [][]byte{
					[]byte(`{"id":"` + id1 + `"}`),
					[]byte(`{"ID":"` + id2 + `"}`),
				},
			}, nil)

		_, err = c.InsertOrReplace(ctx, d1, d2)
		require.NoError(t, err)

		expD1 := &Coll1{Model{ID: uuid.MustParse(id1), Metadata: Metadata{createdAt: tm, updatedAt: tm1, deletedAt: tm2}}}
		require.Equal(t, expD1, d1)
		require.Equal(t, &Coll1{Model{ID: uuid.MustParse(id2), Metadata: Metadata{createdAt: tm, updatedAt: tm1, deletedAt: tm2}}}, d2)

		require.Equal(t, tm, expD1.GetCreatedAt())
		require.Equal(t, tm1, expD1.GetUpdatedAt())
		require.Equal(t, tm2, expD1.GetDeletedAt())
	})

	t.Run("composite_key", func(t *testing.T) {
		type Coll2 struct {
			Key1 string `tigris:"primary_key:1"`
			Key2 string `tigris:"primary_key:2"`
		}

		d3 := &Coll2{}

		m.EXPECT().UseDatabase("db1").Return(mdb)

		c2 := GetCollection[Coll2](db)

		mdb.EXPECT().Replace(ctx, "coll_2",
			[]driver.Document{toDocument(t, d3)}).
			Return(&driver.ReplaceResponse{
				Metadata: &api.ResponseMetadata{
					CreatedAt: timestamppb.New(tm),
					UpdatedAt: timestamppb.New(tm.Add(1 * time.Second)),
					DeletedAt: timestamppb.New(tm.Add(2 * time.Second)),
				},
				Keys: [][]byte{
					[]byte(`{"Key1":"` + id1 + `", "Key2":"` + id2 + `"}`),
				},
			}, nil)

		_, err := c2.InsertOrReplace(ctx, d3)
		require.NoError(t, err)

		require.Equal(t, d3, &Coll2{Key1: id1, Key2: id2})
	})

	t.Run("all_types", func(t *testing.T) {
		type Coll2 struct {
			Int     int       `json:"int"`
			Int64   int64     `json:"int64"`
			Time    time.Time `json:"time"`
			UUID    uuid.UUID `json:"uuid"`
			String  string    `json:"string"`
			Float64 float64   `json:"float64"`
		}

		d3 := &Coll2{}

		m.EXPECT().UseDatabase("db1").Return(mdb)

		c2 := GetCollection[Coll2](db)

		exp := &Coll2{Int: 123, Int64: 456, Time: tm, UUID: uuid.MustParse(id1), String: "str1", Float64: 123.123}

		b, err := jsoniter.Marshal(exp)
		require.NoError(t, err)

		mdb.EXPECT().Replace(ctx, "coll_2",
			[]driver.Document{toDocument(t, d3)}).
			Return(&driver.ReplaceResponse{
				Keys: [][]byte{b},
			}, nil)

		_, err = c2.InsertOrReplace(ctx, d3)
		require.NoError(t, err)

		require.Equal(t, d3, exp)
	})

	t.Run("wrong_docs_count", func(t *testing.T) {
		type Coll2 struct {
			Key1 string `tigris:"primary_key:1"`
			Key2 string `tigris:"primary_key:2"`
		}

		d3 := &Coll2{}

		m.EXPECT().UseDatabase("db1").Return(mdb)

		c2 := GetCollection[Coll2](db)

		mdb.EXPECT().Insert(ctx, "coll_2",
			[]driver.Document{toDocument(t, d3)}).
			Return(&driver.InsertResponse{
				Keys: [][]byte{
					[]byte(`{"Key1":"` + id1 + `", "Key2":"` + id2 + `"}`),
					[]byte(`{"Key1":"` + id1 + `", "Key2":"` + id2 + `"}`),
				},
			}, nil)

		_, err := c2.Insert(ctx, d3)
		require.Error(t, err)

		mdb.EXPECT().Replace(ctx, "coll_2",
			[]driver.Document{toDocument(t, d3)}).
			Return(&driver.ReplaceResponse{
				Keys: [][]byte{
					[]byte(`{"Key1":"` + id1 + `", "Key2":"` + id2 + `"}`),
					[]byte(`{"Key1":"` + id1 + `", "Key2":"` + id2 + `"}`),
				},
			}, nil)

		_, err = c2.InsertOrReplace(ctx, d3)
		require.Error(t, err)
	})

	t.Run("empty_keys", func(t *testing.T) {
		type Coll2 struct {
			Key1 string `tigris:"primary_key:1"`
			Key2 string `tigris:"primary_key:2"`
		}

		d3 := &Coll2{}

		m.EXPECT().UseDatabase("db1").Return(mdb)

		c2 := GetCollection[Coll2](db)

		mdb.EXPECT().Insert(ctx, "coll_2",
			[]driver.Document{toDocument(t, d3)}).
			Return(&driver.InsertResponse{}, nil)

		_, err := c2.Insert(ctx, d3)
		require.NoError(t, err)

		require.Equal(t, &Coll2{}, d3)

		mdb.EXPECT().Replace(ctx, "coll_2",
			[]driver.Document{toDocument(t, d3)}).
			Return(&driver.ReplaceResponse{}, nil)

		_, err = c2.InsertOrReplace(ctx, d3)
		require.NoError(t, err)

		require.Equal(t, &Coll2{}, d3)

		err = populateModelMetadata(&d3, &api.ResponseMetadata{}, nil)
		require.NoError(t, err)

		require.Equal(t, &Coll2{}, d3)
	})
}
