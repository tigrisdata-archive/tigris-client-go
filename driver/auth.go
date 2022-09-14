package driver

import (
	"context"
	"fmt"

	"github.com/tigrisdata/tigris-client-go/config"
)

// Auth declares Tigris Auth APIs
type Auth interface {
	CreateApplication(ctx context.Context, name string, description string) (*Application, error)
	DeleteApplication(ctx context.Context, id string) error
	UpdateApplication(ctx context.Context, id string, name string, description string) (*Application, error)
	ListApplications(ctx context.Context) ([]*Application, error)
	RotateApplicationSecret(ctx context.Context, id string) (*Application, error)
	GetAccessToken(ctx context.Context, applicationID string, applicationSecret string, refreshToken string) (*TokenResponse, error)

	Close() error
}

// NewAuth instantiates authentication API client
func NewAuth(ctx context.Context, cfg *config.Driver) (Auth, error) {
	cfg = initConfig(cfg)

	var auth Auth
	var err error
	if DefaultProtocol == GRPC {
		auth, err = newGRPCClient(ctx, cfg.URL, cfg)
	} else if DefaultProtocol == HTTP {
		auth, err = newHTTPClient(ctx, cfg.URL, cfg)
	} else {
		err = fmt.Errorf("unsupported protocol")
	}
	if err != nil {
		return nil, err
	}

	return auth, nil
}
