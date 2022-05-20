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

	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
)

const (
	GRPC = iota
	HTTP = iota

	TokenEnv = "TIGRIS_TOKEN"

	Version   = "v1.0.0"
	UserAgent = "tigris-client-go/" + Version
)

var (
	DefaultProtocol = GRPC
	TokenRefreshURL = "https://tigrisdata-dev.us.auth0.com/oauth/token"
)

type Document json.RawMessage
type Filter json.RawMessage
type Projection json.RawMessage
type Update json.RawMessage
type Schema json.RawMessage
type Event *api.StreamEvent

type WriteOptions api.WriteOptions

type InsertOptions api.InsertRequestOptions
type ReplaceOptions api.ReplaceRequestOptions
type UpdateOptions api.UpdateRequestOptions
type DeleteOptions api.DeleteRequestOptions
type ReadOptions api.ReadRequestOptions
type StreamOptions api.StreamRequestOptions

type CollectionOptions api.CollectionOptions
type DatabaseOptions api.DatabaseOptions
type TxOptions api.TransactionOptions

type InsertResponse api.InsertResponse
type ReplaceResponse api.ReplaceResponse
type UpdateResponse api.UpdateResponse
type DeleteResponse api.DeleteResponse

type DescribeDatabaseResponse api.DescribeDatabaseResponse
type DescribeCollectionResponse api.DescribeCollectionResponse

type InfoResponse api.GetInfoResponse
