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

package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/code"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/mock"
	"github.com/tigrisdata/tigris-client-go/sort"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	jm         = driver.JM
	toDocument = driver.ToDocument
)

type CollSearchTest struct {
	Metadata

	Field1 string
}

func TestSearchIndex_DocumentCRUDIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	mdrv := mock.NewMockDriver(ctrl)
	msearch := mock.NewMockSearchClient(ctrl)

	mdrv.EXPECT().UseSearch("proj1").Return(msearch)
	msearch.EXPECT().CreateOrUpdateIndex(gomock.Any(), "coll_search_tests", jm(t,
		`{
                   "title":"coll_search_tests",
				   "properties":{ "Field1":{"type":"string"} },
                   "source": { "type": "external"}
				 }`))

	s, err := openSearch(ctx, mdrv, "proj1", &CollSearchTest{})
	require.NoError(t, err)

	d1 := &CollSearchTest{Field1: "value1"}
	d2 := &CollSearchTest{Field1: "value2"}
	tm := time.Now().UTC()

	idx := GetIndex[CollSearchTest](s)

	t.Run("get", func(t *testing.T) {
		msearch.EXPECT().Get(gomock.Any(), "coll_search_tests", []string{"id1"}).Return(
			[]*api.SearchHit{{Data: toDocument(t, d1), Metadata: &api.SearchHitMeta{
				CreatedAt: timestamppb.New(tm),
				UpdatedAt: timestamppb.New(tm),
			}}}, nil)

		d, err := idx.Get(ctx, []string{"id1"})
		require.NoError(t, err)
		require.Equal(t, []CollSearchTest{{
			Field1:   "value1",
			Metadata: newMetadata(tm, tm),
		}}, d)
		require.Equal(t, tm, d[0].GetCreatedAt())
		require.Equal(t, tm, d[0].GetUpdatedAt())
	})

	t.Run("create", func(t *testing.T) {
		msearch.EXPECT().Create(gomock.Any(), "coll_search_tests", []driver.Document{toDocument(t, d1), toDocument(t, d2)}).
			Return(
				[]*api.DocStatus{
					{Id: "id1", Error: nil},
					{Id: "id2", Error: &api.Error{Code: api.Code_INVALID_ARGUMENT, Message: "error2"}},
				}, nil)

		resp, err := idx.Create(ctx, d1, d2)
		require.NoError(t, err)

		require.Equal(t, &Response{
			Statuses: []DocStatus{{ID: "id1"}, {ID: "id2", Error: NewError(code.InvalidArgument, "error2")}},
		}, resp)
	})

	t.Run("create_or_replace", func(t *testing.T) {
		msearch.EXPECT().CreateOrReplace(gomock.Any(), "coll_search_tests", []driver.Document{toDocument(t, d1), toDocument(t, d2)}).
			Return(
				[]*api.DocStatus{
					{Id: "id1", Error: nil},
					{Id: "id2", Error: &api.Error{Code: api.Code_INVALID_ARGUMENT, Message: "error2"}},
				}, nil)

		resp, err := idx.CreateOrReplace(ctx, d1, d2)
		require.NoError(t, err)

		require.Equal(t, &Response{
			Statuses: []DocStatus{{ID: "id1"}, {ID: "id2", Error: NewError(code.InvalidArgument, "error2")}},
		}, resp)
	})

	t.Run("update", func(t *testing.T) {
		msearch.EXPECT().Update(gomock.Any(), "coll_search_tests", []driver.Document{toDocument(t, d1), toDocument(t, d2)}).
			Return(
				[]*api.DocStatus{
					{Id: "id1", Error: nil},
					{Id: "id2", Error: &api.Error{Code: api.Code_INVALID_ARGUMENT, Message: "error2"}},
				}, nil)

		resp, err := idx.Update(ctx, d1, d2)
		require.NoError(t, err)

		require.Equal(t, &Response{
			Statuses: []DocStatus{{ID: "id1"}, {ID: "id2", Error: NewError(code.InvalidArgument, "error2")}},
		}, resp)
	})

	t.Run("delete", func(t *testing.T) {
		msearch.EXPECT().Delete(gomock.Any(), "coll_search_tests", []string{"id1", "id2"}).
			Return(
				[]*api.DocStatus{
					{Id: "id1", Error: nil},
					{Id: "id2", Error: &api.Error{Code: api.Code_INVALID_ARGUMENT, Message: "error2"}},
				}, nil)

		resp, err := idx.Delete(ctx, []string{"id1", "id2"})
		require.NoError(t, err)

		require.Equal(t, &Response{
			Statuses: []DocStatus{{ID: "id1"}, {ID: "id2", Error: NewError(code.InvalidArgument, "error2")}},
		}, resp)
	})

	t.Run("delete_by_query", func(t *testing.T) {
		msearch.EXPECT().DeleteByQuery(gomock.Any(), "coll_search_tests", driver.Filter(`{"all":{"$eq":"b"}}`)).
			Return(int32(2), nil)

		resp, err := idx.DeleteByQuery(ctx, filter.Eq("all", "b"))
		require.NoError(t, err)

		require.Equal(t, 2, resp)
	})
}

func requireError(t *testing.T, code code.Code, msg string, err error) {
	var ep Error
	require.True(t, errors.As(err, &ep))
	require.Equal(t, NewError(code, msg), &ep)
}

func TestSearchIndex_DocumentCRUDIndexNegative(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	mdrv := mock.NewMockDriver(ctrl)
	msearch := mock.NewMockSearchClient(ctrl)

	mdrv.EXPECT().UseSearch("proj1").Return(msearch)
	msearch.EXPECT().CreateOrUpdateIndex(gomock.Any(), "coll_search_tests", jm(t,
		`{
                   "title":"coll_search_tests",
				   "properties":{ "Field1":{"type":"string"} },
                   "source": { "type": "external"}
				 }`))

	s, err := openSearch(ctx, mdrv, "proj1", &CollSearchTest{})
	require.NoError(t, err)

	idx := GetIndex[CollSearchTest](s)

	t.Run("create", func(t *testing.T) {
		msearch.EXPECT().Create(gomock.Any(), "coll_search_tests", []driver.Document{}).
			Return(nil, driver.NewError(api.Code_NOT_FOUND, "error2"))

		_, err = idx.Create(ctx)
		requireError(t, code.NotFound, "error2", err)
	})

	t.Run("get", func(t *testing.T) {
		msearch.EXPECT().Get(gomock.Any(), "coll_search_tests", []string{"id1"}).Return(nil,
			driver.NewError(api.Code_INVALID_ARGUMENT, "error1"))

		_, err = idx.Get(ctx, []string{"id1"})
		requireError(t, code.InvalidArgument, "error1", err)
	})

	t.Run("delete", func(t *testing.T) {
		msearch.EXPECT().Delete(gomock.Any(), "coll_search_tests", []string{"id1", "id2"}).
			Return(nil, driver.NewError(api.Code_INVALID_ARGUMENT, "error1"))

		_, err = idx.Delete(ctx, []string{"id1", "id2"})
		requireError(t, code.InvalidArgument, "error1", err)
	})

	t.Run("delete_by_query", func(t *testing.T) {
		msearch.EXPECT().DeleteByQuery(gomock.Any(), "coll_search_tests", driver.Filter(`{"all":{"$eq":"b"}}`)).
			Return(int32(0), driver.NewError(api.Code_INVALID_ARGUMENT, "error1"))

		_, err = idx.DeleteByQuery(ctx, filter.Eq("all", "b"))
		requireError(t, code.InvalidArgument, "error1", err)
	})
}

func TestSearchIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	mdrv := mock.NewMockDriver(ctrl)
	msearch := mock.NewMockSearchClient(ctrl)

	mdrv.EXPECT().UseSearch("proj1").Return(msearch)
	msearch.EXPECT().CreateOrUpdateIndex(gomock.Any(), "coll_search_tests", jm(t,
		`{
                   "title":"coll_search_tests",
				   "properties":{ "Field1":{"type":"string"} },
                   "source": { "type": "external"}
				 }`))

	s, err := openSearch(ctx, mdrv, "proj1", &CollSearchTest{})
	require.NoError(t, err)

	msearch.EXPECT().DeleteIndex(gomock.Any(), "coll_search_tests")

	err = s.DeleteIndex(ctx, &CollSearchTest{})
	require.NoError(t, err)

	msearch.EXPECT().DeleteIndex(gomock.Any(), "coll_search_tests")
	msearch.EXPECT().DeleteIndex(gomock.Any(), "coll_1")

	err = s.DeleteIndex(ctx, &CollSearchTest{}, Coll1{})
	require.NoError(t, err)

	msearch.EXPECT().DeleteIndex(gomock.Any(), "coll_search_tests").Return(fmt.Errorf("error1"))

	err = s.DeleteIndex(ctx, &CollSearchTest{})
	require.Equal(t, fmt.Errorf("error1"), err)

	msearch.EXPECT().DeleteIndex(gomock.Any(), "coll_search_tests")
	msearch.EXPECT().DeleteIndex(gomock.Any(), "coll_1").Return(fmt.Errorf("error2"))

	err = s.DeleteIndex(ctx, &CollSearchTest{}, Coll1{})
	require.Equal(t, fmt.Errorf("error2"), err)
}

func createSearchResponse(t *testing.T, doc interface{}) driver.SearchIndexResponse {
	t.Helper()

	d, err := json.Marshal(doc)
	require.NoError(t, err)
	tm := time.Now()
	return &api.SearchIndexResponse{
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

func TestSearchIndex_Search(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ctrl := gomock.NewController(t)
	mdrv := mock.NewMockDriver(ctrl)
	msearch := mock.NewMockSearchClient(ctrl)

	mdrv.EXPECT().UseSearch("proj1").Return(msearch)
	msearch.EXPECT().CreateOrUpdateIndex(gomock.Any(), "coll_search_tests", jm(t,
		`{
                   "title":"coll_search_tests",
				   "properties":{ "Field1":{"type":"string"} },
                   "source": { "type": "external"}
				 }`))

	s, err := openSearch(ctx, mdrv, "proj1", &CollSearchTest{})
	require.NoError(t, err)

	idx := GetIndex[CollSearchTest](s)

	// search with all params parses completely
	t.Run("with all request params", func(t *testing.T) {
		rit := mock.NewMockSearchIndexResultIterator(ctrl)
		sr := NewRequestBuilder().
			WithQuery("search query").
			WithSearchFields("field_1").
			WithFilter(filter.Eq("field_2", "some value")).
			WithSorting(sort.Ascending("field_1"), sort.Descending("field_2")).
			WithFacet(NewFacetQueryBuilder().WithFields("field_3").Build()).
			WithIncludeFields("field_4").
			WithOptions(&DefaultSearchOptions).
			Build()
		msearch.EXPECT().Search(ctx, "coll_search_tests", &driver.SearchRequest{
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
		searchIter, err := idx.Search(ctx, sr)
		require.NoError(t, err)
		require.NotNil(t, searchIter)

		// mock search response and validate iterator conversion
		var r driver.SearchIndexResponse
		d1 := &CollSearchTest{Field1: "value1"}
		rit.EXPECT().Next(&r).SetArg(0, createSearchResponse(t, d1)).Return(true)
		rit.EXPECT().Next(&r).Return(false)
		rit.EXPECT().Err().Return(nil)

		var rs Result[CollSearchTest]
		for searchIter.Next(&rs) {
			require.Equal(t, d1, rs.Hits[0].Document)
		}
		require.Nil(t, rit.Err())
	})

	t.Run("with partial request params", func(t *testing.T) {
		rit := mock.NewMockSearchIndexResultIterator(ctrl)
		sr := NewRequestBuilder().Build()
		msearch.EXPECT().Search(ctx, "coll_search_tests", &driver.SearchRequest{
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
		searchIter, err := idx.Search(ctx, sr)
		require.NoError(t, err)
		require.NotNil(t, searchIter)
	})

	// search with nil req
	t.Run("with nil req", func(t *testing.T) {
		searchIter, err := idx.Search(ctx, nil)
		require.NotNil(t, err)
		require.ErrorContains(t, err, "cannot be null")
		require.Nil(t, searchIter)
	})

	// with marshalling failure
	t.Run("when response unmarshalling fails", func(t *testing.T) {
		rit := mock.NewMockSearchIndexResultIterator(ctrl)
		sr := NewRequestBuilder().Build()
		msearch.EXPECT().Search(ctx, "coll_search_tests", gomock.Any()).Return(rit, nil)
		searchIter, err := idx.Search(ctx, sr)
		require.NoError(t, err)

		var r driver.SearchIndexResponse
		// conversion will fail as Field1 is supposed to be int
		d1 := `{Key1: "aaa", Field1: "123"}`
		rit.EXPECT().Next(&r).SetArg(0, createSearchResponse(t, d1)).Return(true)
		rit.EXPECT().Close()

		var rs Result[CollSearchTest]
		require.Nil(t, searchIter.err)
		require.False(t, searchIter.Next(&rs))
		require.NotNil(t, searchIter.err)
		require.ErrorContains(t, searchIter.err, "cannot unmarshal string")
	})
}
