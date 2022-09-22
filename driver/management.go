package driver

import (
	"context"
	"fmt"

	"github.com/tigrisdata/tigris-client-go/config"
)

// Management declares Tigris Management APIs
type Management interface {
	CreateApplication(ctx context.Context, name string, description string) (*Application, error)
	DeleteApplication(ctx context.Context, id string) error
	UpdateApplication(ctx context.Context, id string, name string, description string) (*Application, error)
	ListApplications(ctx context.Context) ([]*Application, error)
	RotateClientSecret(ctx context.Context, id string) (*Application, error)
	GetAccessToken(ctx context.Context, clientId string, clientSecret string, refreshToken string) (*TokenResponse, error)

	Close() error
}

// NewManagement instantiates authentication API client
func NewManagement(ctx context.Context, cfg *config.Driver) (Management, error) {
	cfg = initConfig(cfg)

	var auth Management
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
