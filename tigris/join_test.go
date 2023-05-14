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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/fields"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/mock"
	"github.com/tigrisdata/tigris-client-go/sort"
)

func expectReads(t *testing.T, ctx context.Context, mdb *mock.MockDatabase, mit *mock.MockIterator, joinCond string) {
	t.Helper()

	mdb.EXPECT().Read(ctx, "users",
		driver.Filter(`{}`),
		driver.Projection(nil),
		&driver.ReadOptions{},
	).Return(mit, nil)

	mdb.EXPECT().Read(ctx, "orders",
		jm(t, joinCond),
		driver.Projection(`{}`),
	).Return(mit, nil)
}

func TestJoin(t *testing.T) {
	type Nested struct {
		ID    uuid.UUID
		IDInt int
	}

	type User struct {
		ID        uuid.UUID
		IDInt     int64
		IDFloat   float64
		IDBool    bool
		IDArr     []int
		IDNull    *int
		NonUnique int

		Nested Nested
	}

	type Order struct {
		ID uuid.UUID

		UserID      uuid.UUID
		UserIDInt   int64
		UserIDFloat float64
		UserIDBool  bool
		UserIDArr   []int
		UserIDNull  *int
		UserIDInt1  int

		Nested Nested
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	m := mock.NewMockDriver(ctrl)

	mdb := mock.NewMockDatabase(ctrl)

	m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)
	mdb.EXPECT().CreateOrUpdateCollections(gomock.Any(), driver.JAM(t, []string{}))

	db, err := TestOpenDatabase(ctx, m, "db1", &User{})
	require.NoError(t, err)

	mit := mock.NewMockIterator(ctrl)

	usr := &User{
		ID:      uuid.MustParse("b1f05d83-ea5e-44bc-b56a-d60d2941fcea"),
		IDInt:   24,
		IDFloat: 25.25,
		IDBool:  true,
		IDArr:   []int{5, 7},
		IDNull:  nil,
		Nested: Nested{
			ID: uuid.MustParse("b1f05d83-ea5e-44bc-b56a-d60d2941fcea"),
		},
		NonUnique: 25,
	}

	usr1 := &User{
		ID:    uuid.MustParse("991c8a61-08d2-4ae5-b3dc-254e4ff710bb"),
		IDArr: []int{10, 20},
		Nested: Nested{
			ID: uuid.MustParse("991c8a61-08d2-4ae5-b3dc-254e4ff710bb"),
		},
		NonUnique: 25,
	}

	ord := &Order{
		ID:          uuid.MustParse("9f249aae-9966-4f8e-a1b3-1052b2fe25cb"),
		UserID:      uuid.MustParse("b1f05d83-ea5e-44bc-b56a-d60d2941fcea"),
		UserIDInt:   24,
		UserIDFloat: 24.24,
		UserIDBool:  true,
		Nested: Nested{
			ID:    uuid.MustParse("b1f05d83-ea5e-44bc-b56a-d60d2941fcea"),
			IDInt: 10,
		},
	}

	ord1 := &Order{
		ID:          uuid.MustParse("9f249aae-9966-4f8e-a1b3-1052b2fe25cb"),
		UserID:      uuid.MustParse("c59c491a-679a-41c1-ae50-b180cbd7ea44"),
		UserIDInt:   25,
		UserIDFloat: 25.25,
		Nested: Nested{
			ID:    uuid.MustParse("b1f05d83-ea5e-44bc-b56a-d60d2941fcea"),
			IDInt: 5,
		},
	}

	ord2 := &Order{
		ID:          uuid.MustParse("9f249aae-9966-4f8e-a1b3-1052b2fe25cb"),
		UserID:      uuid.MustParse("b1f05d83-ea5e-44bc-b56a-d60d2941fcea"),
		UserIDInt:   25,
		UserIDFloat: 27.27,
		UserIDArr:   []int{5, 7},
		Nested: Nested{
			ID:    uuid.MustParse("b1f05d83-ea5e-44bc-b56a-d60d2941fcea"),
			IDInt: 7,
		},
	}

	t.Run("join", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "ID", "UserID")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserID":{"$eq":"b1f05d83-ea5e-44bc-b56a-d60d2941fcea"}}]}`)

		var dd, dd1 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).Return(false)

		var ddo, ddo1, ddo2 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord)).Return(true)
		ddo1 = toDocument(t, ord)
		mit.EXPECT().Next(&ddo1).SetArg(0, toDocument(t, ord2)).Return(true)
		ddo2 = toDocument(t, ord2)
		mit.EXPECT().Next(&ddo2).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		for it.Next(&u, &o) {
			require.Equal(t, usr, &u)
			require.Equal(t, 2, len(o))
			require.Equal(t, ord, o[0])
			require.Equal(t, ord2, o[1])
		}
	})

	t.Run("join_nested", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "Nested.ID", "Nested.ID")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"Nested.ID":{"$eq":"b1f05d83-ea5e-44bc-b56a-d60d2941fcea"}}]}`)

		var dd, dd1 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).Return(false)

		var ddo, ddo1 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord1)).Return(true)
		ddo1 = toDocument(t, ord1)
		mit.EXPECT().Next(&ddo1).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		for it.Next(&u, &o) {
			require.Equal(t, usr, &u)
			require.Equal(t, 1, len(o))
			require.Equal(t, ord1, o[0])
		}
	})

	t.Run("join_int", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "IDInt", "UserIDInt")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserIDInt":{"$eq":24}}]}`)

		var dd, dd1 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).Return(false)

		var ddo, ddo1 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord)).Return(true)
		ddo1 = toDocument(t, ord)
		mit.EXPECT().Next(&ddo1).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		for it.Next(&u, &o) {
			require.Equal(t, usr, &u)
			require.Equal(t, 1, len(o))
			require.Equal(t, ord, o[0])
		}
	})

	t.Run("join_float", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "IDFloat", "UserIDFloat")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserIDFloat":{"$eq":25.25}}]}`)

		var dd, dd1 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).Return(false)
		//	mit.EXPECT().Next(&dd).Return(false)

		var ddo, ddo1 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord1)).Return(true)
		ddo1 = toDocument(t, ord1)
		mit.EXPECT().Next(&ddo1).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		for it.Next(&u, &o) {
			require.Equal(t, usr, &u)
			require.Equal(t, 1, len(o))
			require.Equal(t, ord1, o[0])
		}
	})

	t.Run("join_bool", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "IDBool", "UserIDBool")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserIDBool":{"$eq":true}}]}`)

		var dd, dd1 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).Return(false)

		var ddo, ddo1 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord)).Return(true)
		ddo1 = toDocument(t, ord)
		mit.EXPECT().Next(&ddo1).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		for it.Next(&u, &o) {
			require.Equal(t, usr, &u)
			require.Equal(t, 1, len(o))
			require.Equal(t, ord, o[0])
		}
	})

	t.Run("join_bool_false", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "IDBool", "UserIDBool")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserIDBool":{"$eq":false}}]}`)

		var dd, dd1 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr1)).Return(true)
		dd1 = toDocument(t, usr1)
		mit.EXPECT().Next(&dd1).Return(false)

		var ddo, ddo1, ddo2 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord1)).Return(true)
		ddo1 = toDocument(t, ord1)
		mit.EXPECT().Next(&ddo1).SetArg(0, toDocument(t, ord2)).Return(true)
		ddo2 = toDocument(t, ord2)
		mit.EXPECT().Next(&ddo2).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		for it.Next(&u, &o) {
			require.Equal(t, usr1, &u)
			require.Equal(t, 2, len(o))
			require.Equal(t, ord1, o[0])
			require.Equal(t, ord2, o[1])
		}
	})

	t.Run("join_arr", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "IDArr", "UserIDArr")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserIDArr":{"$eq":[5,7]}}]}`)

		var dd, dd1 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).Return(false)

		var ddo, ddo1 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord2)).Return(true)
		ddo1 = toDocument(t, ord2)
		mit.EXPECT().Next(&ddo1).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		for it.Next(&u, &o) {
			require.Equal(t, usr, &u)
			require.Equal(t, 1, len(o))
			require.Equal(t, ord2, o[0])
		}
	})

	t.Run("join_null", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "IDNull", "UserIDNull")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserIDNull":{"$eq":null}}]}`)

		var dd, dd1 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).Return(false)

		var ddo, ddo1 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord2)).Return(true)
		ddo1 = toDocument(t, ord2)
		mit.EXPECT().Next(&ddo1).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		for it.Next(&u, &o) {
			require.Equal(t, usr, &u)
			require.Equal(t, 1, len(o))
			require.Equal(t, ord2, o[0])
		}
	})

	t.Run("join_outer", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "ID", "UserID")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserID":{"$eq":"b1f05d83-ea5e-44bc-b56a-d60d2941fcea"}},{"UserID":{"$eq":"991c8a61-08d2-4ae5-b3dc-254e4ff710bb"}}]}`)

		var dd, dd1, dd2 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).SetArg(0, toDocument(t, usr1)).Return(true)
		dd2 = toDocument(t, usr1)
		mit.EXPECT().Next(&dd2).Return(false)

		var ddo, ddo1, ddo2 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord)).Return(true)
		ddo1 = toDocument(t, ord)
		mit.EXPECT().Next(&ddo1).SetArg(0, toDocument(t, ord2)).Return(true)
		ddo2 = toDocument(t, ord2)
		mit.EXPECT().Next(&ddo2).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		require.True(t, it.Next(&u, &o))

		require.Equal(t, usr, &u)
		require.Equal(t, 2, len(o))
		require.Equal(t, ord, o[0])
		require.Equal(t, ord2, o[1])

		require.True(t, it.Next(&u, &o))

		require.Equal(t, usr1, &u)
		require.Equal(t, 0, len(o))

		require.False(t, it.Next(&u, &o))
	})

	t.Run("join_inner", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "ID", "UserID", &JoinOptions{Type: InnerJoin})

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserID":{"$eq":"b1f05d83-ea5e-44bc-b56a-d60d2941fcea"}},{"UserID":{"$eq":"991c8a61-08d2-4ae5-b3dc-254e4ff710bb"}}]}`)

		var dd, dd1, dd2 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).SetArg(0, toDocument(t, usr1)).Return(true)
		dd2 = toDocument(t, usr1)
		mit.EXPECT().Next(&dd2).Return(false)

		var ddo, ddo1, ddo2 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord)).Return(true)
		ddo1 = toDocument(t, ord)
		mit.EXPECT().Next(&ddo1).SetArg(0, toDocument(t, ord2)).Return(true)
		ddo2 = toDocument(t, ord2)
		mit.EXPECT().Next(&ddo2).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		require.True(t, it.Next(&u, &o))

		require.Equal(t, usr, &u)
		require.Equal(t, 2, len(o))
		require.Equal(t, ord, o[0])
		require.Equal(t, ord2, o[1])

		require.False(t, it.Next(&u, &o))
	})

	t.Run("join_iterator_array", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "ID", "UserID")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserID":{"$eq":"b1f05d83-ea5e-44bc-b56a-d60d2941fcea"}}]}`)

		var dd, dd2 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd2 = toDocument(t, usr)
		mit.EXPECT().Next(&dd2).Return(false)

		var ddo, ddo1, ddo2 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord)).Return(true)
		ddo1 = toDocument(t, ord)
		mit.EXPECT().Next(&ddo1).SetArg(0, toDocument(t, ord2)).Return(true)
		ddo2 = toDocument(t, ord2)
		mit.EXPECT().Next(&ddo2).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		res, err := it.Array()
		require.NoError(t, err)

		require.Equal(t, 1, len(res))

		require.Equal(t, usr, res[0].Parent)
		require.Equal(t, 2, len(res[0].Child))
		require.Equal(t, ord, res[0].Child[0])
		require.Equal(t, ord2, res[0].Child[1])
	})

	t.Run("join_iterator_closure", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "ID", "UserID")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserID":{"$eq":"b1f05d83-ea5e-44bc-b56a-d60d2941fcea"}}]}`)

		var dd, dd2 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd2 = toDocument(t, usr)
		mit.EXPECT().Next(&dd2).Return(false)

		var ddo, ddo1, ddo2 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord)).Return(true)
		ddo1 = toDocument(t, ord)
		mit.EXPECT().Next(&ddo1).SetArg(0, toDocument(t, ord2)).Return(true)
		ddo2 = toDocument(t, ord2)
		mit.EXPECT().Next(&ddo2).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		err = it.Iterate(
			func(u *User, o []*Order) error {
				require.Equal(t, usr, u)
				require.Equal(t, 2, len(o))
				require.Equal(t, ord, o[0])
				require.Equal(t, ord2, o[1])

				return nil
			})
		require.NoError(t, err)
	})

	t.Run("join_read_one", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "ID", "UserID")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserID":{"$eq":"b1f05d83-ea5e-44bc-b56a-d60d2941fcea"}},{"UserID":{"$eq":"991c8a61-08d2-4ae5-b3dc-254e4ff710bb"}}]}`)

		var dd, dd1, dd2 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).SetArg(0, toDocument(t, usr1)).Return(true)
		dd2 = toDocument(t, usr1)
		mit.EXPECT().Next(&dd2).Return(false)

		var ddo, ddo1, ddo2 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord)).Return(true)
		ddo1 = toDocument(t, ord)
		mit.EXPECT().Next(&ddo1).SetArg(0, toDocument(t, ord2)).Return(true)
		ddo2 = toDocument(t, ord2)
		mit.EXPECT().Next(&ddo2).Return(false)

		res, err := j.ReadOne(ctx, filter.All)
		require.NoError(t, err)

		require.Equal(t, usr, res.Parent)
		require.Equal(t, 2, len(res.Child))
		require.Equal(t, ord, res.Child[0])
		require.Equal(t, ord2, res.Child[1])

		mdb.EXPECT().Read(ctx, "users",
			driver.Filter(`{}`),
			driver.Projection(nil),
			&driver.ReadOptions{},
		).Return(mit, nil)

		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord)).Return(false)
		_, err = j.ReadOne(ctx, filter.All)
		require.Error(t, err)
	})

	t.Run("join_read_all", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "ID", "UserID")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserID":{"$eq":"b1f05d83-ea5e-44bc-b56a-d60d2941fcea"}},{"UserID":{"$eq":"991c8a61-08d2-4ae5-b3dc-254e4ff710bb"}}]}`)

		var dd, dd1, dd2 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).SetArg(0, toDocument(t, usr1)).Return(true)
		dd2 = toDocument(t, usr1)
		mit.EXPECT().Next(&dd2).Return(false)

		var ddo, ddo1, ddo2 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord)).Return(true)
		ddo1 = toDocument(t, ord)
		mit.EXPECT().Next(&ddo1).SetArg(0, toDocument(t, ord2)).Return(true)
		ddo2 = toDocument(t, ord2)
		mit.EXPECT().Next(&ddo2).Return(false)

		it, err := j.ReadAll(ctx)
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		require.True(t, it.Next(&u, &o))

		require.Equal(t, usr, &u)
		require.Equal(t, 2, len(o))
		require.Equal(t, ord, o[0])
		require.Equal(t, ord2, o[1])

		require.True(t, it.Next(&u, &o))

		require.Equal(t, usr1, &u)
		require.Equal(t, 0, len(o))

		require.False(t, it.Next(&u, &o))
	})

	t.Run("join_read_with_options", func(t *testing.T) {
		mdb = mock.NewMockDatabase(ctrl)
		mit = mock.NewMockIterator(ctrl)

		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "ID", "UserID")

		mdb.EXPECT().Read(ctx, "users",
			driver.Filter(`{}`),
			driver.Projection(`{}`),
			&driver.ReadOptions{
				Limit:  10,
				Offset: []byte("aaa"),
				Skip:   30,
				Sort:   []byte(`[{"some_field":"$desc"}]`),
			},
		).Return(mit, nil)

		mdb.EXPECT().Read(ctx, "orders",
			driver.Filter(`{"$or":[{"UserID":{"$eq":"b1f05d83-ea5e-44bc-b56a-d60d2941fcea"}},{"UserID":{"$eq":"991c8a61-08d2-4ae5-b3dc-254e4ff710bb"}}]}`),
			driver.Projection(`{}`),
		).Return(mit, nil)

		var dd, dd1, dd2 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).SetArg(0, toDocument(t, usr1)).Return(true)
		dd2 = toDocument(t, usr1)
		mit.EXPECT().Next(&dd2).Return(false)

		var ddo, ddo1, ddo2 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord)).Return(true)
		ddo1 = toDocument(t, ord)
		mit.EXPECT().Next(&ddo1).SetArg(0, toDocument(t, ord2)).Return(true)
		ddo2 = toDocument(t, ord2)
		mit.EXPECT().Next(&ddo2).Return(false)

		it, err := j.ReadWithOptions(ctx, filter.All, fields.All, &ReadOptions{
			Limit:  10,
			Skip:   30,
			Offset: []byte("aaa"),
			Sort:   sort.Descending("some_field"),
		})
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		require.True(t, it.Next(&u, &o))

		require.Equal(t, usr, &u)
		require.Equal(t, 2, len(o))
		require.Equal(t, ord, o[0])
		require.Equal(t, ord2, o[1])

		require.True(t, it.Next(&u, &o))

		require.Equal(t, usr1, &u)
		require.Equal(t, 0, len(o))

		require.False(t, it.Next(&u, &o))

		_, err = j.ReadWithOptions(ctx, filter.All, fields.All, nil)
		require.Error(t, err)
	})

	t.Run("join_unexpected_child_row", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "ID", "UserID")

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserID":{"$eq":"991c8a61-08d2-4ae5-b3dc-254e4ff710bb"}}]}`)

		var dd1, dd2 driver.Document

		mit.EXPECT().Next(&dd1).SetArg(0, toDocument(t, usr1)).Return(true)
		dd2 = toDocument(t, usr1)
		mit.EXPECT().Next(&dd2).Return(false)

		var ddo driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord)).Return(true)

		_, err := j.Read(ctx, filter.All)
		require.Error(t, err)
	})

	t.Run("join_array_unroll", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "IDArr", "Nested.IDInt", &JoinOptions{ArrayUnroll: true})

		expectReads(t, ctx, mdb, mit, `{"$or":[{"Nested.IDInt":{"$eq":5}},{"Nested.IDInt":{"$eq":7}},{"Nested.IDInt":{"$eq":10}},{"Nested.IDInt":{"$eq":20}}]}`)

		var dd, dd1, dd2 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).SetArg(0, toDocument(t, usr1)).Return(true)
		dd2 = toDocument(t, usr1)
		mit.EXPECT().Next(&dd2).Return(false)

		var ddo0, ddo, ddo1, ddo2 driver.Document
		mit.EXPECT().Next(&ddo0).SetArg(0, toDocument(t, ord)).Return(true)
		ddo = toDocument(t, ord)
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord1)).Return(true)
		ddo1 = toDocument(t, ord1)
		mit.EXPECT().Next(&ddo1).SetArg(0, toDocument(t, ord2)).Return(true)
		ddo2 = toDocument(t, ord2)
		mit.EXPECT().Next(&ddo2).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		require.True(t, it.Next(&u, &o))

		require.Equal(t, usr, &u)
		require.Equal(t, 2, len(o))
		require.Equal(t, ord1, o[0])
		require.Equal(t, ord2, o[1])

		require.True(t, it.Next(&u, &o))

		require.Equal(t, usr1, &u)
		require.Equal(t, 1, len(o))
		require.Equal(t, ord, o[0])

		require.False(t, it.Next(&u, &o))
	})

	t.Run("join_non_unique_left_field", func(t *testing.T) {
		m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)

		j := GetJoin[User, Order](db, "NonUnique", "UserIDInt", &JoinOptions{ArrayUnroll: true})

		expectReads(t, ctx, mdb, mit, `{"$or":[{"UserIDInt":{"$eq":25}}]}`)

		var dd, dd1, dd2 driver.Document

		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
		dd1 = toDocument(t, usr)
		mit.EXPECT().Next(&dd1).SetArg(0, toDocument(t, usr1)).Return(true)
		dd2 = toDocument(t, usr1)
		mit.EXPECT().Next(&dd2).Return(false)

		var ddo, ddo1, ddo2 driver.Document
		mit.EXPECT().Next(&ddo).SetArg(0, toDocument(t, ord1)).Return(true)
		ddo1 = toDocument(t, ord1)
		mit.EXPECT().Next(&ddo1).SetArg(0, toDocument(t, ord2)).Return(true)
		ddo2 = toDocument(t, ord2)
		mit.EXPECT().Next(&ddo2).Return(false)

		it, err := j.Read(ctx, filter.All)
		require.NoError(t, err)

		var (
			u User
			o []*Order
		)

		require.True(t, it.Next(&u, &o))

		require.Equal(t, usr, &u)
		require.Equal(t, 2, len(o))
		require.Equal(t, ord1, o[0])
		require.Equal(t, ord2, o[1])

		require.True(t, it.Next(&u, &o))

		require.Equal(t, usr1, &u)
		require.Equal(t, 2, len(o))
		require.Equal(t, ord1, o[0])
		require.Equal(t, ord2, o[1])

		require.False(t, it.Next(&u, &o))
	})
}
