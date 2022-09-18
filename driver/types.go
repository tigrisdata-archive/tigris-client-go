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

package driver

import (
	"encoding/json"
	"time"

	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
)

const (
	GRPC  = "GRPC"
	HTTP  = "HTTP"
	HTTPS = "HTTPS"

	ApplicationID     = "TIGRIS_APPLICATION_ID"
	ApplicationSecret = "TIGRIS_APPLICATION_SECRET"
	Token             = "TIGRIS_TOKEN"
	Protocol          = "TIGRIS_PROTOCOL"

	Version   = "v1.0.0"
	UserAgent = "tigris-client-go/" + Version

	tokenRequestTimeout = 15 * time.Second
)

var (
	DefaultProtocol = GRPC

	// TokenURLOverride Only used in tests to point auth to proper HTTP port in GRPC tests
	TokenURLOverride string
)

type Document json.RawMessage
type Message json.RawMessage
type Filter json.RawMessage
type Projection json.RawMessage
type Update json.RawMessage
type Schema json.RawMessage
type Facet json.RawMessage
type SortOrder json.RawMessage

type WriteOptions api.WriteOptions

type InsertOptions api.InsertRequestOptions
type ReplaceOptions api.ReplaceRequestOptions
type UpdateOptions api.UpdateRequestOptions
type DeleteOptions api.DeleteRequestOptions
type ReadOptions api.ReadRequestOptions
type PublishOptions api.PublishRequestOptions
type SubscribeOptions api.SubscribeRequestOptions

type CollectionOptions api.CollectionOptions
type DatabaseOptions api.DatabaseOptions
type TxOptions api.TransactionOptions

type InsertResponse api.InsertResponse
type ReplaceResponse api.ReplaceResponse
type UpdateResponse api.UpdateResponse
type DeleteResponse api.DeleteResponse
type PublishResponse api.PublishResponse
type SubscribeResponse api.SubscribeResponse

type DescribeDatabaseResponse api.DescribeDatabaseResponse
type DescribeCollectionResponse api.DescribeCollectionResponse

type InfoResponse api.GetInfoResponse

type SearchRequest struct {
	Q             string
	SearchFields  []string
	Filter        Filter
	Facet         Facet
	Sort          SortOrder
	IncludeFields []string
	ExcludeFields []string
	Page          int32
	PageSize      int32
}
type SearchResponse *api.SearchResponse

type Error struct {
	*api.TigrisError
}

// As converts driver.Error the error which implements AsTigrisError interface
func (e *Error) As(i any) bool {
	if x, ok := i.(interface{ AsTigrisError(*Error) bool }); ok && x.AsTigrisError(e) {
		return true
	}
	return false
}

type Application api.Application
type TokenResponse api.GetAccessTokenResponse
