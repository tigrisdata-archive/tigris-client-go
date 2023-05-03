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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/mock"
)

func TestProjectionBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	m := mock.NewMockDriver(ctrl)

	type Address struct {
		Street  string
		City    string
		State   string
		Zipcode string
	}

	type Account struct {
		unexported string //nolint:unused
		JSONSkip   string `json:"-"`
		TigrisSkip string `tigris:"-"`

		Name        string
		Balance     string
		BankAddress *Address
	}

	type Assets struct {
		Accounts []*Account
	}

	type User struct {
		Name           string
		Address        *Address
		MailingAddress Address `json:"mailing_address"`
		Assets         Assets
		CreatedAt      time.Time
		UUID           uuid.UUID
	}

	type UserName struct {
		Name string
	}

	type DiagonalProjection struct {
		Assets struct {
			Accounts []struct {
				Balance string
			}
		}
		Address struct {
			State string
		}
	}

	type NestedProjection struct {
		Assets struct {
			Accounts []struct {
				Name        string
				Balance     string
				BankAddress struct {
					Street string
				}
			}
		}
		Address struct {
			State string
		}
	}

	type FieldNotInDoc struct {
		NoSuchField string
	}

	type FieldTypeMismatch struct {
		Name float64
	}

	mdb := mock.NewMockDatabase(ctrl)

	m.EXPECT().UseDatabase("db1").Return(mdb).Times(2)
	mdb.EXPECT().CreateOrUpdateCollections(gomock.Any(), driver.JAM(t, []string{
		`
{
	"title":"users",
	"properties":
	{ 
		"Address": {
			"type":"object",
			"properties":{
				"City":{"type":"string"},
				"State":{"type":"string"},
				"Street":{"type":"string"},
				"Zipcode":{"type":"string"}
			}
		},
		"Assets": {
			"type":"object",
			"properties":{
				"Accounts":{
					"type":"array",
					"items":{
						"type":"object",
						"properties":{
							"BankAddress": {
								"type":"object",
								"properties":{
									"City":{"type":"string"},
									"State":{"type":"string"},
									"Street":{"type":"string"},
									"Zipcode":{"type":"string"}
								}
							},
							"Balance":{"type":"string"},
							"Name":{"type":"string"}
						}
					}
				}
			}
		},
		"CreatedAt":{"type":"string","format":"date-time"},
		"ID":{"type":"string","format":"uuid","autoGenerate":true},
		"Name":{"type":"string"},
		"UUID":{"type":"string","format":"uuid"},
		"mailing_address":{
			"type":"object",
			"properties":{
				"City":{"type":"string"},
				"State":{"type":"string"},
				"Street":{"type":"string"},
				"Zipcode":{"type":"string"}
			}
		}
	},
	"primary_key":["ID"],
	"collection_type":"documents"
}`,
	}))

	db, err := TestOpenDatabase(ctx, m, "db1", &User{})
	require.NoError(t, err)

	pt := GetProjection[User, Address](db, "mailing_address")
	require.NoError(t, pt.err)
	require.Equal(t, `{"mailing_address":true}`, string(pt.projection))

	m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)
	pt1 := GetProjection[User, UserName](db)
	require.NoError(t, pt1.err)
	require.Equal(t, `{"Name":true}`, string(pt1.projection))

	m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)
	pt2 := GetProjection[User, DiagonalProjection](db)
	require.NoError(t, pt2.err)
	require.JSONEq(t, `{"Address.State":true,"Assets.Accounts.Balance":true}`, string(pt2.projection))

	m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)
	pt3 := GetProjection[User, DiagonalProjection](db, "invalid_path")
	require.Equal(t, fmt.Errorf("no subobject found for key path [invalid_path]"), pt3.err)

	m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)
	pt3 = GetProjection[User, DiagonalProjection](db, "invalid_path", "nested_invalid")
	require.Error(t, fmt.Errorf("no subobject found for key path [invalid_path]"), pt3.err)

	m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)
	pt33 := GetProjection[User, FieldNotInDoc](db)
	require.Equal(t, fmt.Errorf("field present in projection and doesn't exist in the document: NoSuchField"), pt33.err)

	expErr := fmt.Errorf("projection type mismatch. doc=string, projection=float64")
	m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)
	pt34 := GetProjection[User, FieldTypeMismatch](db)
	require.Equal(t, expErr, pt34.err)

	// test that read API returns error delayed from GetProjection
	_, err = pt34.Read(ctx, filter.Eq("f1", true))
	require.Equal(t, expErr, err)
	_, err = pt34.ReadAll(ctx)
	require.Equal(t, expErr, err)
	_, err = pt34.ReadOne(ctx, filter.Eq("f1", true))
	require.Equal(t, expErr, err)
	_, err = pt34.ReadWithOptions(ctx, filter.Eq("f1", true), &ReadOptions{Skip: 10})
	require.Equal(t, expErr, err)

	m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)
	pt4 := GetProjection[User, []Account](db, "Assets", "Accounts")
	require.NoError(t, pt4.err)
	require.Equal(t, `{"Assets.Accounts":true}`, string(pt4.projection))

	m.EXPECT().UseDatabase("db1").Return(mdb).Times(1)
	pt5 := GetProjection[User, NestedProjection](db)
	require.NoError(t, pt5.err)
	require.JSONEq(t, `{"Address.State":true, "Assets.Accounts.Balance":true, "Assets.Accounts.BankAddress.Street":true, "Assets.Accounts.Name":true}`, string(pt5.projection))

	mit := mock.NewMockIterator(ctrl)

	mdb.EXPECT().Read(ctx, "users",
		driver.Filter(`{"$or":[{"Key1":{"$eq":"aaa"}},{"Key1":{"$eq":"ccc"}}]}`),
		driver.Projection(`{"Assets.Accounts":true}`),
	).Return(mit, nil)

	it, err := pt4.Read(ctx, filter.Or(
		filter.Eq("Key1", "aaa"),
		filter.Eq("Key1", "ccc")),
	)
	require.NoError(t, err)

	var (
		d  []Account
		dd driver.Document
	)

	usr := &User{
		Assets: Assets{
			Accounts: []*Account{
				{
					Name:        "name1",
					Balance:     "balance1",
					BankAddress: &Address{},
				},
			},
		},
	}

	mit.EXPECT().Close()
	mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
	mit.EXPECT().Next(&dd).Return(false)
	mit.EXPECT().Err().Return(nil)

	for it.Next(&d) {
		require.Equal(t, []Account{
			{
				Name:        "name1",
				Balance:     "balance1",
				BankAddress: &Address{},
			},
		}, d)
	}

	require.NoError(t, it.Err())

	it.Close()

	mdb.EXPECT().Read(ctx, "users",
		driver.Filter(`{"$or":[{"Key1":{"$eq":"aaa"}},{"Key1":{"$eq":"ccc"}}]}`),
		driver.Projection(`{"Name":true}`),
	).Return(mit, nil)

	it1, err := pt1.Read(ctx, filter.Or(
		filter.Eq("Key1", "aaa"),
		filter.Eq("Key1", "ccc")),
	)
	require.NoError(t, err)

	usr = &User{
		Name: "user_name1",
		Assets: Assets{
			Accounts: []*Account{
				{
					Name:        "name1",
					Balance:     "balance1",
					BankAddress: &Address{},
				},
			},
		},
	}

	mit.EXPECT().Close()
	mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
	mit.EXPECT().Next(&dd).Return(false)
	mit.EXPECT().Err().Return(nil)

	var u UserName

	for it1.Next(&u) {
		require.Equal(t, usr.Name, u.Name)
	}

	require.NoError(t, it.Err())

	it.Close()

	mdb.EXPECT().Read(ctx, "users",
		driver.Filter(`{"$or":[{"Key1":{"$eq":"aaa"}},{"Key1":{"$eq":"ccc"}}]}`),
		driver.Projection(`{"mailing_address":true}`),
	).Return(mit, nil)

	it2, err := pt.Read(ctx, filter.Or(
		filter.Eq("Key1", "aaa"),
		filter.Eq("Key1", "ccc")),
	)
	require.NoError(t, err)

	usr = &User{
		Name: "user_name1",
		Assets: Assets{
			Accounts: []*Account{
				{
					Name:        "name1",
					Balance:     "balance1",
					BankAddress: &Address{},
				},
			},
		},
		MailingAddress: Address{
			City:   "Palo Alto",
			Street: "Louis Rd",
		},
	}

	mit.EXPECT().Close()
	mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
	mit.EXPECT().Next(&dd).Return(false)
	mit.EXPECT().Err().Return(nil)

	var ma Address

	for it2.Next(&ma) {
		require.Equal(t, usr.MailingAddress, ma)
	}

	require.NoError(t, it.Err())

	it.Close()

	mdb.EXPECT().Read(ctx, "users",
		driver.Filter(`{}`),
		driver.Projection(`{"mailing_address":true}`),
	).Return(mit, nil)

	it2, err = pt.ReadAll(ctx)
	require.NoError(t, err)

	mit.EXPECT().Close()
	mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
	mit.EXPECT().Next(&dd).Return(false)
	mit.EXPECT().Err().Return(nil)

	for it2.Next(&ma) {
		require.Equal(t, usr.MailingAddress, ma)
	}

	require.NoError(t, it.Err())

	it.Close()

	mdb.EXPECT().Read(ctx, "users",
		driver.Filter(`{}`),
		driver.Projection(`{"mailing_address":true}`),
		&driver.ReadOptions{},
	).Return(mit, nil)

	it2, err = pt.ReadWithOptions(ctx, filter.All, &ReadOptions{})
	require.NoError(t, err)

	mit.EXPECT().Close()
	mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)
	mit.EXPECT().Next(&dd).Return(false)
	mit.EXPECT().Err().Return(nil)

	for it2.Next(&ma) {
		require.Equal(t, usr.MailingAddress, ma)
	}

	require.NoError(t, it.Err())

	it.Close()

	t.Run("read_one", func(t *testing.T) {
		mdb.EXPECT().Read(ctx, "users",
			driver.Filter(`{}`),
			driver.Projection(`{"mailing_address":true}`),
		).Return(mit, nil)

		mit.EXPECT().Close()
		mit.EXPECT().Next(&dd).SetArg(0, toDocument(t, usr)).Return(true)

		ma, err := pt.ReadOne(ctx, filter.All)
		require.NoError(t, err)
		require.Equal(t, &usr.MailingAddress, ma)
	})
}
