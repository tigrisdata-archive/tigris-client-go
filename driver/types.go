package driver

import (
	"crypto/tls"
	"encoding/json"

	api "github.com/tigrisdata/tigrisdb-client-go/api/server/v1"
)

const (
	GRPC = iota
	HTTP = iota

	TokenEnv = "TIGRISDB_TOKEN"

	Version   = "v1.0.0"
	UserAgent = "tigrisdb-client-go/" + Version
)

var (
	DefaultProtocol  = GRPC
	ToekenRefreshURL = "https://tigrisdata-dev.us.auth0.com/oauth/token"
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
