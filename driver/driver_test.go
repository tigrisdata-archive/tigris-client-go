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

package driver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/golang/mock/gomock"
	//nolint:staticcheck
	gproto "github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	mock "github.com/tigrisdata/tigris-client-go/mock/api"
	"github.com/tigrisdata/tigris-client-go/test"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	metadata2 "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func setupGRPCTests(t *testing.T, config *config.Driver) (Driver, *grpcDriver, *test.MockServers, func()) {
	mockServers, cancel := test.SetupTests(t, 0)
	config.TLS = test.SetupTLS(t)
	config.URL = test.GRPCURL(0)
	client, err := newGRPCClient(context.Background(), config)
	require.NoError(t, err)

	return &driver{driverWithOptions: client, cfg: config}, client, mockServers, func() { cancel(); _ = client.Close() }
}

func setupHTTPTests(t *testing.T, config *config.Driver) (Driver, *httpDriver, *test.MockServers, func()) {
	mockServers, cancel := test.SetupTests(t, 2)
	url := test.HTTPURL(2)
	config.URL = url
	config.TLS = test.SetupTLS(t)
	client, err := newHTTPClient(context.Background(), config)
	require.NoError(t, err)

	// FIXME: implement proper wait for HTTP server to start
	time.Sleep(10 * time.Millisecond)

	return &driver{driverWithOptions: client, cfg: config}, client, mockServers, func() { cancel(); _ = client.Close() }
}

func SetupO11yGRPCTests(t *testing.T, config *config.Driver) (Driver, Observability, *test.MockServers, func()) {
	return setupGRPCTests(t, config)
}

func SetupO11yHTTPTests(t *testing.T, config *config.Driver) (Driver, Observability, *test.MockServers, func()) {
	return setupHTTPTests(t, config)
}

func SetupMgmtGRPCTests(t *testing.T, config *config.Driver) (Driver, Management, *test.MockServers, func()) {
	return setupGRPCTests(t, config)
}

func SetupMgmtHTTPTests(t *testing.T, config *config.Driver) (Driver, Management, *test.MockServers, func()) {
	return setupHTTPTests(t, config)
}

func SetupGRPCTests(t *testing.T, config *config.Driver) (Driver, *mock.MockTigrisServer, func()) {
	d, _, s, f := setupGRPCTests(t, config)
	return d, s.API, f
}

func SetupHTTPTests(t *testing.T, config *config.Driver) (Driver, *mock.MockTigrisServer, func()) {
	d, _, s, f := setupHTTPTests(t, config)
	return d, s.API, f
}

func testError(t *testing.T, d Driver, mc *mock.MockTigrisServer, in error, exp error, rd time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var r *api.DeleteResponse
	if in == nil {
		r = &api.DeleteResponse{}
	}

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{},
		})).Return(r, in)

	_, err := d.UseDatabase("db1").Delete(ctx, "c1", Filter(`{"filter":"value"}`))

	require.Equal(t, exp, err)

	var de *Error
	if errors.As(err, &de) {
		require.Equal(t, rd, de.RetryDelay())
	}
}

func testReadStreamError(t *testing.T, d Driver, mc *mock.MockTigrisServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mc.EXPECT().Read(
		pm(&api.ReadRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":"value"}`),
			Options:    &api.ReadRequestOptions{},
		}), gomock.Any()).DoAndReturn(func(r *api.ReadRequest, srv api.Tigris_ReadServer) error {
		err := srv.Send(&api.ReadResponse{Data: Document(`{"aaa":"bbbb"}`)})
		require.NoError(t, err)

		return &api.TigrisError{Code: api.Code_DATA_LOSS, Message: "error_stream"}
	})

	it, err := d.UseDatabase("db1").Read(ctx, "c1", Filter(`{"filter":"value"}`),
		Projection(`{"fields":"value"}`))
	require.NoError(t, err)

	var doc Document

	require.True(t, it.Next(&doc))
	require.Equal(t, Document(`{"aaa":"bbbb"}`), doc)
	require.False(t, it.Next(&doc))
	require.Equal(t, &Error{&api.TigrisError{Code: api.Code_DATA_LOSS, Message: "error_stream"}}, it.Err())
}

func testSearchStreamError(t *testing.T, d Driver, mc *mock.MockTigrisServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	expectedMeta := &api.SearchMetadata{
		Found:      125,
		TotalPages: 5,
		Page: &api.Page{
			Current: 2,
			Size:    25,
		},
	}

	mc.EXPECT().Search(
		pm(&api.SearchRequest{
			Project:    "db1",
			Collection: "c1",
			Q:          "search text",
		}), gomock.Any()).DoAndReturn(func(r *api.SearchRequest, srv api.Tigris_SearchServer) error {
		err := srv.Send(&api.SearchResponse{Meta: expectedMeta})
		require.NoError(t, err)

		return &api.TigrisError{Code: api.Code_ABORTED, Message: "error_stream"}
	})

	it, err := d.UseDatabase("db1").Search(ctx, "c1", &SearchRequest{Q: "search text"})
	require.NoError(t, err)

	var doc SearchResponse

	require.True(t, it.Next(&doc))
	require.Equal(t, expectedMeta.Page.Current, doc.Meta.Page.Current)
	require.Equal(t, expectedMeta.Page.Size, doc.Meta.Page.Size)
	require.Equal(t, expectedMeta.Found, doc.Meta.Found)
	require.Equal(t, expectedMeta.TotalPages, doc.Meta.TotalPages)
	require.False(t, it.Next(&doc))
	require.Equal(t, &Error{&api.TigrisError{Code: api.Code_ABORTED, Message: "error_stream"}}, it.Err())
}

func testBranchCrudErrors(t *testing.T, d Driver, mc *mock.MockTigrisServer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mc.EXPECT().CreateBranch(gomock.Any(), pm(&api.CreateBranchRequest{
		Project: "db1",
		Branch:  "staging",
	})).DoAndReturn(func(ctx context.Context, req *api.CreateBranchRequest) (*api.CreateBranchResponse, error) {
		return nil, &api.TigrisError{Code: api.Code_ALREADY_EXISTS, Message: "branch already exists"}
	})

	createResp, err := d.UseDatabase("db1").CreateBranch(ctx, "staging")
	require.Nil(t, createResp)
	require.Equal(t, &Error{&api.TigrisError{Code: api.Code_ALREADY_EXISTS, Message: "branch already exists"}}, err)

	mc.EXPECT().DeleteBranch(gomock.Any(), pm(&api.DeleteBranchRequest{
		Project: "db1",
		Branch:  "feature_1",
	})).DoAndReturn(func(ctx context.Context, req *api.DeleteBranchRequest) (*api.DeleteBranchResponse, error) {
		return nil, &api.TigrisError{Code: api.Code_NOT_FOUND, Message: "project does not exist"}
	})

	delResp, err := d.UseDatabase("db1").DeleteBranch(ctx, "feature_1")
	require.Nil(t, delResp)
	require.Equal(t, &Error{&api.TigrisError{Code: api.Code_NOT_FOUND, Message: "project does not exist"}}, err)
}

func testErrors(t *testing.T, d Driver, mc *mock.MockTigrisServer) {
	t.Helper()

	cases := []struct {
		name       string
		in         error
		exp        error
		retryDelay time.Duration
	}{
		{
			"tigris_error", api.Errorf(api.Code_UNAUTHENTICATED, "some error"),
			&Error{&api.TigrisError{Code: api.Code_UNAUTHENTICATED, Message: "some error"}}, 0,
		},
		{
			"invalid_argument_error", api.Errorf(api.Code_INVALID_ARGUMENT, "invalid argument error"),
			&Error{&api.TigrisError{Code: api.Code_INVALID_ARGUMENT, Message: "invalid argument error"}}, 0,
		},
		{
			"error", fmt.Errorf("some error 1"),
			&Error{&api.TigrisError{Code: api.Code_UNKNOWN, Message: "some error 1"}}, 0,
		},
		{
			"grpc_error", status.Error(codes.PermissionDenied, "some error 1"),
			&Error{&api.TigrisError{Code: api.Code_PERMISSION_DENIED, Message: "some error 1"}}, 0,
		},
		{"no_error", nil, nil, 0},
		{
			"extended_tigris_error", api.Errorf(api.Code_CONFLICT, "extended error"),
			&Error{&api.TigrisError{Code: api.Code_CONFLICT, Message: "extended error"}}, 0,
		},
		{
			"retry_error", api.Errorf(api.Code_CONFLICT, "retry error").WithRetry(5 * time.Second),
			&Error{&api.TigrisError{
				Code: api.Code_CONFLICT, Message: "retry error",
				Details: []gproto.Message{&errdetails.RetryInfo{RetryDelay: durationpb.New(5 * time.Second)}},
			}},
			5 * time.Second,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			testError(t, d, mc, c.in, c.exp, c.retryDelay)
		})
	}
}

func TestGRPCError(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{})
	defer cancel()
	testErrors(t, client, mockServer)
	t.Run("read_stream_error", func(t *testing.T) {
		testReadStreamError(t, client, mockServer)
	})
	t.Run("search_stream_error", func(t *testing.T) {
		testSearchStreamError(t, client, mockServer)
	})
	t.Run("branch_crud_errors", func(t *testing.T) {
		testBranchCrudErrors(t, client, mockServer)
	})
}

func TestHTTPError(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{})
	defer cancel()
	testErrors(t, client, mockServer)
	t.Run("read_stream_error", func(t *testing.T) {
		testReadStreamError(t, client, mockServer)
	})
	t.Run("search_stream_error", func(t *testing.T) {
		testSearchStreamError(t, client, mockServer)
	})
	t.Run("branch_crud_errors", func(t *testing.T) {
		testBranchCrudErrors(t, client, mockServer)
	})
}

func pm(m proto.Message) gomock.Matcher {
	return &test.ProtoMatcher{Message: m}
}

func testTxCRUDBasic(t *testing.T, c Tx, mc *mock.MockTigrisServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doc1 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`)}

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	setGRPCTxCtx(ctx, txCtx, metadata2.MD{})

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Project:    "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc1)),
			Options:    &api.InsertRequestOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.InsertRequest) (*api.InsertResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return &api.InsertResponse{}, nil
		})

	_, err := c.Insert(ctx, "c1", doc1)
	require.NoError(t, err)

	doc123 := []Document{
		Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`), Document(`{"K1":"vK1","K2":2,"D1":"vD2"}`),
		Document(`{"K1":"vK2","K2":1,"D1":"vD3"}`),
	}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Project:    "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.InsertRequestOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.InsertRequest) (*api.InsertResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return &api.InsertResponse{}, nil
		})

	_, err = c.Insert(ctx, "c1", doc123)
	require.NoError(t, err)

	mc.EXPECT().Replace(gomock.Any(),
		pm(&api.ReplaceRequest{
			Project:    "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.ReplaceRequestOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.ReplaceRequest) (*api.ReplaceResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return &api.ReplaceResponse{}, nil
		})

	_, err = c.Replace(ctx, "c1", doc123)
	require.NoError(t, err)

	mc.EXPECT().Update(gomock.Any(),
		pm(&api.UpdateRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":1}`),
			Options:    &api.UpdateRequestOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.UpdateRequest) (*api.UpdateResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return &api.UpdateResponse{}, nil
		})

	_, err = c.Update(ctx, "c1", Filter(`{"filter":"value"}`), Update(`{"fields":1}`))
	require.NoError(t, err)

	mc.EXPECT().Read(
		pm(&api.ReadRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":"value"}`),
			Options:    &api.ReadRequestOptions{},
		}), gomock.Any()).Return(nil)

	it, err := c.Read(ctx, "c1", Filter(`{"filter":"value"}`), Projection(`{"fields":"value"}`))
	require.NoError(t, err)

	require.False(t, it.Next(nil))

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.DeleteRequest) (*api.DeleteResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return &api.DeleteResponse{}, nil
		})

	_, err = c.Delete(ctx, "c1", Filter(`{"filter":"value"}`))
	require.NoError(t, err)

	mc.EXPECT().ListCollections(gomock.Any(),
		pm(&api.ListCollectionsRequest{
			Project: "db1",
		})).DoAndReturn(
		func(ctx context.Context, r *api.ListCollectionsRequest) (*api.ListCollectionsResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return &api.ListCollectionsResponse{Collections: []*api.CollectionInfo{
				{Collection: "lc1"},
				{Collection: "lc2"},
			}}, nil
		})

	colls, err := c.ListCollections(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"lc1", "lc2"}, colls)

	sch := `{"schema":"field"}`

	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Project:    "db1",
			Collection: "c1",
			Schema:     []byte(sch),
			Options:    &api.CollectionOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) (
			*api.CreateOrUpdateCollectionResponse, error,
		) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return &api.CreateOrUpdateCollectionResponse{}, nil
		})

	err = c.CreateOrUpdateCollection(ctx, "c1", Schema(sch))
	require.NoError(t, err)

	mc.EXPECT().DropCollection(gomock.Any(),
		pm(&api.DropCollectionRequest{
			Project:    "db1",
			Collection: "c1",
			Options:    &api.CollectionOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.DropCollectionRequest) (*api.DropCollectionResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return &api.DropCollectionResponse{}, nil
		})

	err = c.DropCollection(ctx, "c1")
	require.NoError(t, err)
}

func testCRUDBasic(t *testing.T, c Driver, mc *mock.MockTigrisServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := c.UseDatabase("db1")

	doc1 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`)}

	options := &api.WriteOptions{}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Project:    "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc1)),
			Options:    &api.InsertRequestOptions{WriteOptions: options},
		})).Return(&api.InsertResponse{Status: "inserted"}, nil)

	insResp, err := db.Insert(ctx, "c1", doc1, &InsertOptions{WriteOptions: options})
	require.NoError(t, err)
	require.Equal(t, "inserted", insResp.Status)

	mc.EXPECT().Replace(gomock.Any(),
		pm(&api.ReplaceRequest{
			Project:    "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc1)),
			Options:    &api.ReplaceRequestOptions{WriteOptions: options},
		})).Return(&api.ReplaceResponse{Status: "replaced"}, nil)

	repResp, err := db.Replace(ctx, "c1", doc1, &ReplaceOptions{WriteOptions: options})
	require.NoError(t, err)
	require.Equal(t, "replaced", repResp.Status)

	doc123 := []Document{
		Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`), Document(`{"K1":"vK1","K2":2,"D1":"vD2"}`),
		Document(`{"K1":"vK2","K2":1,"D1":"vD3"}`),
	}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Project:    "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.InsertRequestOptions{},
		})).Return(&api.InsertResponse{}, nil)

	_, err = db.Insert(ctx, "c1", doc123)
	require.NoError(t, err)

	mc.EXPECT().Update(gomock.Any(),
		pm(&api.UpdateRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":1}`),
			Options:    &api.UpdateRequestOptions{},
		})).Return(&api.UpdateResponse{Status: "updated"}, nil)

	updResp, err := db.Update(ctx, "c1", Filter(`{"filter":"value"}`), Update(`{"fields":1}`))
	require.NoError(t, err)
	require.Equal(t, "updated", updResp.Status)

	roptions := &api.ReadRequestOptions{}

	mc.EXPECT().Read(
		pm(&api.ReadRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":"value"}`),
			Options:    roptions,
		}), gomock.Any()).Return(nil)

	it, err := db.Read(ctx, "c1", Filter(`{"filter":"value"}`), Projection(`{"fields":"value"}`))
	require.NoError(t, err)

	require.False(t, it.Next(nil))

	var sit SearchResultIterator

	mc.EXPECT().Search(
		pm(&api.SearchRequest{
			Project:       "db1",
			Collection:    "c1",
			Q:             "search text",
			SearchFields:  []string{"field_1"},
			Facet:         []byte(`{"field_1":{"size":10},"field_2":{"size":10}}`),
			IncludeFields: nil,
			ExcludeFields: nil,
			Sort:          []byte(`[{"field_1":"$desc"},{"field_2":"$asc"},{"field_3":"$desc"}]`),
			Filter:        nil,
			PageSize:      12,
			Page:          3,
		}), gomock.Any()).Return(nil)

	sit, err = db.Search(ctx, "c1", &SearchRequest{
		Q:            "search text",
		SearchFields: []string{"field_1"},
		Facet:        Facet(`{"field_1":{"size":10},"field_2":{"size":10}}`),
		Sort:         SortOrder(`[{"field_1":"$desc"},{"field_2":"$asc"},{"field_3":"$desc"}]`),
		PageSize:     12,
		Page:         3,
	})

	require.NoError(t, err)
	require.False(t, sit.Next(nil))

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{},
		})).Return(&api.DeleteResponse{Status: "deleted"}, nil)

	delResp, err := db.Delete(ctx, "c1", Filter(`{"filter":"value"}`))
	require.NoError(t, err)
	require.Equal(t, "deleted", delResp.Status)

	mc.EXPECT().CreateBranch(gomock.Any(), pm(&api.CreateBranchRequest{
		Project: "db1",
		Branch:  "staging",
	})).Return(&api.CreateBranchResponse{Status: "creationOk"}, nil)

	branchCreateResp, err := db.CreateBranch(ctx, "staging")
	require.NoError(t, err)
	require.Equal(t, "creationOk", branchCreateResp.Status)

	mc.EXPECT().DeleteBranch(gomock.Any(), pm(&api.DeleteBranchRequest{
		Project: "db1",
		Branch:  "staging",
	})).Return(&api.DeleteBranchResponse{Status: "deletionOk"}, nil)

	branchDelResp, err := db.DeleteBranch(ctx, "staging")
	require.NoError(t, err)
	require.Equal(t, "deletionOk", branchDelResp.Status)
}

func testDriverBasic(t *testing.T, c Driver, mc *mock.MockTigrisServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := c.UseDatabase("db1")

	// Test empty list response
	mc.EXPECT().ListCollections(gomock.Any(),
		pm(&api.ListCollectionsRequest{
			Project: "db1",
		})).Return(&api.ListCollectionsResponse{Collections: nil}, nil)

	colls, err := db.ListCollections(ctx, &CollectionOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{}, colls)

	mc.EXPECT().ListCollections(gomock.Any(),
		pm(&api.ListCollectionsRequest{
			Project: "db1",
		})).Return(&api.ListCollectionsResponse{Collections: []*api.CollectionInfo{
		{Collection: "lc1"},
		{Collection: "lc2"},
	}}, nil)

	colls, err = db.ListCollections(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"lc1", "lc2"}, colls)

	descExp := api.DescribeCollectionResponse{
		Collection: "coll1",
		Schema:     []byte(`{"a":"b"}`),
		Size:       123456,
	}

	mc.EXPECT().DescribeCollection(gomock.Any(),
		pm(&api.DescribeCollectionRequest{
			Project:      "db1",
			Collection:   "coll1",
			SchemaFormat: "fmt1",
		})).Return(&descExp, nil)

	desc, err := db.DescribeCollection(ctx, "coll1", &DescribeCollectionOptions{SchemaFormat: "fmt1"})
	require.NoError(t, err)
	require.Equal(t, descExp.Collection, desc.Collection)
	require.Equal(t, descExp.Schema, desc.Schema)
	require.Equal(t, descExp.Size, desc.Size)

	descDBExp := api.DescribeDatabaseResponse{
		Size: 314159,
		Collections: []*api.CollectionDescription{
			{
				Collection: "coll1",
				Schema:     []byte(`{"a":"b"}`),
				Size:       111111,
			},
			{
				Collection: "coll2",
				Schema:     []byte(`{"c":"d"}`),
				Size:       222222,
			},
		},
		Branches: []string{"main", "bug-fix", "feature_2"},
	}

	mc.EXPECT().DescribeDatabase(gomock.Any(),
		pm(&api.DescribeDatabaseRequest{
			Project:      "db1",
			SchemaFormat: "fmt2",
		})).Return(&descDBExp, nil)

	descDB, err := c.DescribeDatabase(ctx, "db1", &DescribeProjectOptions{SchemaFormat: "fmt2"})
	require.NoError(t, err)
	require.Equal(t, int64(314159), descDB.Size)
	require.Equal(t, descDBExp.Collections[0].Collection, descDB.Collections[0].Collection)
	require.Equal(t, descDBExp.Collections[0].Schema, descDB.Collections[0].Schema)
	require.Equal(t, descDBExp.Collections[0].Size, descDB.Collections[0].Size)
	require.Equal(t, descDBExp.Collections[1].Collection, descDB.Collections[1].Collection)
	require.Equal(t, descDBExp.Collections[1].Schema, descDB.Collections[1].Schema)
	require.Equal(t, descDBExp.Collections[1].Size, descDB.Collections[1].Size)
	require.Equal(t, descDBExp.Branches, descDB.Branches)

	sch := `{"schema":"field"}`

	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Project:    "db1",
			Collection: "c1",
			Schema:     []byte(sch),
			OnlyCreate: true,
			Options:    &api.CollectionOptions{},
		})).Return(&api.CreateOrUpdateCollectionResponse{}, nil)

	err = db.CreateOrUpdateCollection(ctx, "c1", Schema(sch), &CreateCollectionOptions{OnlyCreate: true})
	require.NoError(t, err)

	mc.EXPECT().DropCollection(gomock.Any(),
		pm(&api.DropCollectionRequest{
			Project:    "db1",
			Collection: "c1",
			Options:    &api.CollectionOptions{},
		})).Return(&api.DropCollectionResponse{}, nil)

	err = db.DropCollection(ctx, "c1", &CollectionOptions{})
	require.NoError(t, err)

	testCRUDBasic(t, c, mc)
}

func testTxBasic(t *testing.T, c Driver, mc *mock.MockTigrisServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Project: "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	tx, err := c.UseDatabase("db1").BeginTx(ctx, &TxOptions{})
	require.NoError(t, err)

	testTxCRUDBasic(t, tx, mc)

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Project: "db1",
		})).Return(&api.CommitTransactionResponse{}, nil)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	err = tx.Rollback(ctx)
	require.NoError(t, err)
}

func testResponseMetadata(t *testing.T, c Driver, mc *mock.MockTigrisServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := c.UseDatabase("db1")

	doc1 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`)}

	options := &api.WriteOptions{}

	tm := time.Now()
	md := &api.ResponseMetadata{
		CreatedAt: timestamppb.New(tm),
		UpdatedAt: timestamppb.New(tm),
		DeletedAt: timestamppb.New(tm),
	}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Project:    "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc1)),
			Options:    &api.InsertRequestOptions{},
		})).Return(&api.InsertResponse{Status: "inserted", Metadata: md}, nil)

	insResp, err := db.Insert(ctx, "c1", doc1)
	require.NoError(t, err)
	require.Equal(t, "inserted", insResp.Status)
	require.Equal(t, md.CreatedAt.AsTime(), insResp.Metadata.CreatedAt.AsTime())

	mc.EXPECT().Replace(gomock.Any(),
		pm(&api.ReplaceRequest{
			Project:    "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc1)),
			Options:    &api.ReplaceRequestOptions{WriteOptions: options},
		})).Return(&api.ReplaceResponse{Status: "replaced", Metadata: md}, nil)

	repResp, err := db.Replace(ctx, "c1", doc1, &ReplaceOptions{WriteOptions: options})
	require.NoError(t, err)
	require.Equal(t, "replaced", repResp.Status)
	require.Equal(t, md.CreatedAt.AsTime(), repResp.Metadata.CreatedAt.AsTime())

	mc.EXPECT().Update(gomock.Any(),
		pm(&api.UpdateRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":1}`),
			Options:    &api.UpdateRequestOptions{WriteOptions: options},
		})).Return(&api.UpdateResponse{Status: "updated", Metadata: md}, nil)

	updResp, err := db.Update(ctx, "c1", Filter(`{"filter":"value"}`), Update(`{"fields":1}`),
		&UpdateOptions{WriteOptions: options})
	require.NoError(t, err)
	require.Equal(t, "updated", updResp.Status)
	require.Equal(t, updResp.Metadata.UpdatedAt.AsTime(), repResp.Metadata.UpdatedAt.AsTime())

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{WriteOptions: options},
		})).Return(&api.DeleteResponse{Status: "deleted", Metadata: md}, nil)

	delResp, err := db.Delete(ctx, "c1", Filter(`{"filter":"value"}`), &DeleteOptions{WriteOptions: options})
	require.NoError(t, err)
	require.Equal(t, "deleted", delResp.Status)
	require.Equal(t, md.DeletedAt.AsTime(), delResp.Metadata.DeletedAt.AsTime())
}

func TestGRPCDriver(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{})
	defer cancel()
	testDriverBasic(t, client, mockServer)
	testResponseMetadata(t, client, mockServer)
}

func TestHTTPDriver(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{})
	defer cancel()
	testDriverBasic(t, client, mockServer)
	testResponseMetadata(t, client, mockServer)
}

func TestTxGRPCDriver(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{})
	defer cancel()
	testTxBasic(t, client, mockServer)
}

func TestTxHTTPDriver(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{})
	defer cancel()
	testTxBasic(t, client, mockServer)
}

func testTxCRUDBasicNegative(t *testing.T, c Tx, mc *mock.MockTigrisServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	doc1 := []Document{Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`)}

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}
	setGRPCTxCtx(ctx, txCtx, metadata2.MD{})

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Project:    "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc1)),
			Options:    &api.InsertRequestOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.InsertRequest) (*api.InsertResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return nil, fmt.Errorf("error")
		})

	_, err := c.Insert(ctx, "c1", doc1, &InsertOptions{})
	require.Error(t, err)

	doc123 := []Document{
		Document(`{"K1":"vK1","K2":1,"D1":"vD1"}`), Document(`{"K1":"vK1","K2":2,"D1":"vD2"}`),
		Document(`{"K1":"vK2","K2":1,"D1":"vD3"}`),
	}

	mc.EXPECT().Insert(gomock.Any(),
		pm(&api.InsertRequest{
			Project:    "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.InsertRequestOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.InsertRequest) (*api.InsertResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return nil, fmt.Errorf("error")
		})

	_, err = c.Insert(ctx, "c1", doc123, &InsertOptions{})
	require.Error(t, err)

	mc.EXPECT().Replace(gomock.Any(),
		pm(&api.ReplaceRequest{
			Project:    "db1",
			Collection: "c1",
			Documents:  *(*[][]byte)(unsafe.Pointer(&doc123)),
			Options:    &api.ReplaceRequestOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.ReplaceRequest) (*api.ReplaceResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return nil, fmt.Errorf("error")
		})

	_, err = c.Replace(ctx, "c1", doc123, &ReplaceOptions{})
	require.Error(t, err)

	mc.EXPECT().Update(gomock.Any(),
		pm(&api.UpdateRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":1}`),
			Options:    &api.UpdateRequestOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.UpdateRequest) (*api.UpdateResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return nil, fmt.Errorf("error")
		})

	_, err = c.Update(ctx, "c1", Filter(`{"filter":"value"}`), Update(`{"fields":1}`), &UpdateOptions{})
	require.Error(t, err)

	mc.EXPECT().Read(
		pm(&api.ReadRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":"value"}`),
			Options:    &api.ReadRequestOptions{},
		}), gomock.Any()).Return(&api.TigrisError{Code: api.Code_DATA_LOSS, Message: "errrror"})

	it, err := c.Read(ctx, "c1", Filter(`{"filter":"value"}`), Projection(`{"fields":"value"}`))
	require.NoError(t, err)

	var d Document

	require.False(t, it.Next(&d))
	require.Equal(t, &Error{&api.TigrisError{Code: api.Code_DATA_LOSS, Message: "errrror"}}, it.Err())

	mc.EXPECT().Search(
		pm(&api.SearchRequest{
			Project:    "db1",
			Collection: "c1",
			Q:          "search query",
		}), gomock.Any()).Return(&api.TigrisError{Code: api.Code_DATA_LOSS, Message: "search error"})

	sit, err := c.Search(ctx, "c1", &SearchRequest{Q: "search query"})
	require.NoError(t, err)

	var resp SearchResponse

	require.False(t, sit.Next(&resp))
	require.Equal(t, &Error{&api.TigrisError{Code: api.Code_DATA_LOSS, Message: "search error"}}, sit.Err())

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.DeleteRequest) (*api.DeleteResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return nil, fmt.Errorf("error")
		})

	_, err = c.Delete(ctx, "c1", Filter(`{"filter":"value"}`))
	require.Error(t, err)

	mc.EXPECT().Read(
		pm(&api.ReadRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":"value"}`),
			Options:    &api.ReadRequestOptions{},
		}), gomock.Any()).Return(nil)

	_, err = c.Read(ctx, "c1", Filter(`{"filter":"value"}`), Projection(`{"fields":"value"}`), nil)
	require.NoError(t, err)

	mc.EXPECT().Read(
		pm(&api.ReadRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Fields:     []byte(`{"fields":"value"}`),
			Options:    &api.ReadRequestOptions{},
		}), gomock.Any()).Return(nil)

	var ro *ReadOptions
	_, err = c.Read(ctx, "c1", Filter(`{"filter":"value"}`), Projection(`{"fields":"value"}`), ro)
	require.NoError(t, err)

	mc.EXPECT().ListCollections(gomock.Any(),
		pm(&api.ListCollectionsRequest{
			Project: "db1",
		})).DoAndReturn(
		func(ctx context.Context, r *api.ListCollectionsRequest) (*api.ListCollectionsResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))
			return nil, fmt.Errorf("error")
		})

	_, err = c.ListCollections(ctx)
	require.Error(t, err)

	sch := `{"schema":"field"}`

	mc.EXPECT().CreateOrUpdateCollection(gomock.Any(),
		pm(&api.CreateOrUpdateCollectionRequest{
			Project:    "db1",
			Collection: "c1",
			Schema:     []byte(sch),
			Options:    &api.CollectionOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.CreateOrUpdateCollectionRequest) (
			*api.CreateOrUpdateCollectionResponse, error,
		) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return nil, fmt.Errorf("error")
		})

	err = c.CreateOrUpdateCollection(ctx, "c1", Schema(sch))
	require.Error(t, err)

	mc.EXPECT().DropCollection(gomock.Any(),
		pm(&api.DropCollectionRequest{
			Project:    "db1",
			Collection: "c1",
			Options:    &api.CollectionOptions{},
		})).DoAndReturn(
		func(ctx context.Context, r *api.DropCollectionRequest) (*api.DropCollectionResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return nil, fmt.Errorf("error")
		})

	err = c.DropCollection(ctx, "c1")
	require.Error(t, err)
}

func testTxBasicNegative(t *testing.T, c Driver, mc *mock.MockTigrisServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Project: "db1",
			Options: &api.TransactionOptions{},
		})).Return(nil, fmt.Errorf("error"))

	_, err := c.UseDatabase("db1").BeginTx(ctx, &TxOptions{})
	require.Error(t, err)

	// Empty tx context
	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Project: "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{}, nil)

	_, err = c.UseDatabase("db1").BeginTx(ctx, &TxOptions{})
	require.Error(t, err)

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Project: "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	tx, err := c.UseDatabase("db1").BeginTx(ctx, &TxOptions{})
	require.NoError(t, err)

	testTxCRUDBasicNegative(t, tx, mc)

	mc.EXPECT().CommitTransaction(gomock.Any(),
		pm(&api.CommitTransactionRequest{
			Project: "db1",
		})).DoAndReturn(
		func(ctx context.Context, r *api.CommitTransactionRequest) (*api.CommitTransactionResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return nil, fmt.Errorf("error")
		})

	err = tx.Commit(ctx)
	require.Error(t, err)

	mc.EXPECT().RollbackTransaction(gomock.Any(),
		pm(&api.RollbackTransactionRequest{
			Project: "db1",
		})).DoAndReturn(
		func(ctx context.Context, r *api.RollbackTransactionRequest) (*api.RollbackTransactionResponse, error) {
			require.True(t, proto.Equal(txCtx, api.GetTransaction(ctx)))

			return nil, fmt.Errorf("error")
		})

	err = tx.Rollback(ctx)
	require.Error(t, err)

	mc.EXPECT().DescribeDatabase(gomock.Any(),
		pm(&api.DescribeDatabaseRequest{
			Project: "db1",
		})).Return(&api.DescribeDatabaseResponse{}, nil)

	_, err = c.DescribeDatabase(ctx, "db1", nil)
	require.NoError(t, err)
}

func TestTxGRPCDriverNegative(t *testing.T) {
	client, mockServer, cancel := SetupGRPCTests(t, &config.Driver{})
	defer cancel()
	testTxBasicNegative(t, client, mockServer)
}

func TestTxHTTPDriverNegative(t *testing.T) {
	client, mockServer, cancel := SetupHTTPTests(t, &config.Driver{})
	defer cancel()
	testTxBasicNegative(t, client, mockServer)
}

func TestNewDriver(t *testing.T) {
	_, cancel := test.SetupTests(t, 4)
	defer cancel()

	DefaultProtocol = HTTP
	cfg := config.Driver{URL: test.HTTPURL(4)}
	client, err := NewDriver(context.Background(), &cfg)
	require.NoError(t, err)

	_ = client.Close()

	DefaultProtocol = GRPC

	certPool := x509.NewCertPool()
	require.True(t, certPool.AppendCertsFromPEM([]byte(test.CaCert)))

	cfg = config.Driver{URL: test.GRPCURL(4), TLS: &tls.Config{
		RootCAs: certPool, ServerName: "localhost",
		MinVersion: tls.VersionTLS12,
	}}
	client, err = NewDriver(context.Background(), &cfg)
	require.NoError(t, err)

	_ = client.Close()

	DefaultProtocol = "SOMETHING"
	_, err = NewDriver(context.Background(), nil)
	require.Error(t, err)

	DefaultProtocol = GRPC
	cfg1 := &config.Driver{URL: test.GRPCURL(4), ClientSecret: "aaaa"}
	cfg1, err = initConfig(cfg1)
	require.NoError(t, err)
	require.NotNil(t, cfg1.TLS)
}

func TestInvalidDriverAPIOptions(t *testing.T) {
	c, mc, cancel := SetupGRPCTests(t, &config.Driver{})
	defer cancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db := c.UseDatabase("db1")

	_, err := db.ListCollections(ctx, &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
	_, err = db.Insert(ctx, "coll1", nil, &InsertOptions{}, &InsertOptions{})
	require.Error(t, err)
	_, err = db.Replace(ctx, "coll1", nil, &ReplaceOptions{}, &ReplaceOptions{})
	require.Error(t, err)
	_, err = db.Update(ctx, "coll1", nil, nil, &UpdateOptions{}, &UpdateOptions{})
	require.Error(t, err)
	_, err = db.Delete(ctx, "coll1", nil, nil, &DeleteOptions{}, &DeleteOptions{})
	require.Error(t, err)
	_, err = c.UseDatabase("db1").BeginTx(ctx, &TxOptions{}, &TxOptions{})
	require.Error(t, err)
	err = db.CreateOrUpdateCollection(ctx, "coll1", nil, &CreateCollectionOptions{}, &CreateCollectionOptions{})
	require.Error(t, err)
	err = db.DropCollection(ctx, "coll1", &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
	_, err = db.Read(ctx, "coll1", nil, nil, &ReadOptions{}, &ReadOptions{})
	require.Error(t, err)
	_, err = db.Search(ctx, "coll1", nil)
	require.Error(t, err)

	txCtx := &api.TransactionCtx{Id: "tx_id1", Origin: "origin_id1"}

	mc.EXPECT().BeginTransaction(gomock.Any(),
		pm(&api.BeginTransactionRequest{
			Project: "db1",
			Options: &api.TransactionOptions{},
		})).Return(&api.BeginTransactionResponse{TxCtx: txCtx}, nil)

	tx, err := c.UseDatabase("db1").BeginTx(ctx)
	require.NoError(t, err)
	_, err = tx.ListCollections(ctx, &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
	_, err = tx.Insert(ctx, "coll1", nil, &InsertOptions{}, &InsertOptions{})
	require.Error(t, err)
	_, err = tx.Replace(ctx, "coll1", nil, &ReplaceOptions{}, &ReplaceOptions{})
	require.Error(t, err)
	_, err = tx.Update(ctx, "coll1", nil, nil, &UpdateOptions{}, &UpdateOptions{})
	require.Error(t, err)
	_, err = tx.Delete(ctx, "coll1", nil, nil, &DeleteOptions{}, &DeleteOptions{})
	require.Error(t, err)
	_, err = tx.Read(ctx, "coll1", nil, nil, &ReadOptions{}, &ReadOptions{})
	require.Error(t, err)
	err = tx.CreateOrUpdateCollection(ctx, "coll1", nil, &CreateCollectionOptions{}, &CreateCollectionOptions{})
	require.Error(t, err)
	err = tx.DropCollection(ctx, "coll1", &CollectionOptions{}, &CollectionOptions{})
	require.Error(t, err)
}
