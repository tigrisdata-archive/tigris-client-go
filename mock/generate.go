package mock

//go:generate mockgen -package mock -destination user_grpc.go github.com/tigrisdata/tigrisdb-api/server/v1 TigrisDBServer
