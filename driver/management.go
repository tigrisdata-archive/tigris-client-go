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
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/tigrisdata/tigris-client-go/config"
)

// Management declares Tigris Management APIs.
type Management interface {
	CreateApplication(ctx context.Context, name string, description string) (*Application, error)
	DeleteApplication(ctx context.Context, id string) error
	UpdateApplication(ctx context.Context, id string, name string, description string) (*Application, error)
	ListApplications(ctx context.Context) ([]*Application, error)
	RotateClientSecret(ctx context.Context, id string) (*Application, error)
	GetAccessToken(ctx context.Context, clientID string, clientSecret string, refreshToken string) (*TokenResponse, error)

	Close() error
}

// NewManagement instantiates authentication API client.
func NewManagement(ctx context.Context, cfg *config.Driver) (Management, error) {
	cfg = initConfig(cfg)

	protocol := DefaultProtocol
	if os.Getenv(EnvProtocol) != "" {
		protocol = strings.ToUpper(os.Getenv(EnvProtocol))
	}

	var (
		mgmt Management
		err  error
	)

	switch protocol {
	case GRPC:
		mgmt, err = newGRPCClient(ctx, cfg.URL, cfg)
	case HTTP:
		mgmt, err = newHTTPClient(ctx, cfg.URL, cfg)
	default:
		err = fmt.Errorf("unsupported protocol")
	}

	if err != nil {
		return nil, err
	}

	return mgmt, nil
}
