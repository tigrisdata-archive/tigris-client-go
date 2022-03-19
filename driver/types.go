package driver

import (
	"crypto/tls"
	"encoding/json"

	api "github.com/tigrisdata/tigrisdb-api/server/v1"
)

const (
	GRPC = iota
	HTTP = iota

	TOKEN_ENV = "TIGRISDB_TOKEN"
)

var (
	DefaultProtocol   = GRPC
	TOKEN_REFRESH_URL = "https://tigrisdata-dev.us.auth0.com/oauth/token"
)

type Document json.RawMessage
type Filter json.RawMessage
type Fields json.RawMessage
type Schema json.RawMessage

type WriteOptions api.WriteOptions

type InsertOptions api.InsertRequestOptions
type UpdateOptions api.UpdateRequestOptions
type DeleteOptions api.DeleteRequestOptions
type ReadOptions api.ReadRequestOptions

type CollectionOptions api.CollectionOptions
type DatabaseOptions api.DatabaseOptions
type TxOptions api.TransactionOptions

type InsertResponse *api.InsertResponse
type UpdateResponse *api.UpdateResponse
type DeleteResponse *api.DeleteResponse

type Config struct {
	TLS   *tls.Config `json:"tls,omitempty"`
	Token string      `json:",omitempty"`
}
