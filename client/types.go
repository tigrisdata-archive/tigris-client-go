package client

import "github.com/tigrisdata/tigris-client-go/config"

// DatabaseConfig contains database and connection config
type DatabaseConfig struct {
	config.Config
	// MustExist if set skips implicit database creation
	MustExist bool
}

type InsertResponse struct{}
type InsertOrReplaceResponse struct{}
type DeleteResponse struct{}
type UpdateResponse struct{}
