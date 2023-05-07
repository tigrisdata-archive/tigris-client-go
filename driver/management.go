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

package driver

import (
	"context"
	"fmt"

	"github.com/tigrisdata/tigris-client-go/config"
)

// Management declares Tigris Management APIs.
type Management interface {
	GetAccessToken(ctx context.Context, clientID string, clientSecret string, refreshToken string) (*TokenResponse, error)

	CreateNamespace(ctx context.Context, name string) error
	ListNamespaces(ctx context.Context) ([]*Namespace, error)

	CreateInvitations(ctx context.Context, invitations []*InvitationInfo) error
	DeleteInvitations(ctx context.Context, email string, status string) error
	ListInvitations(ctx context.Context, status string) ([]*Invitation, error)
	VerifyInvitation(ctx context.Context, email string, code string) error
	ListUsers(ctx context.Context) ([]*User, error)

	Close() error
}

// NewManagement instantiates authentication API client.
func NewManagement(ctx context.Context, cfg *config.Driver) (Management, error) {
	var err error

	cfg, err = initConfig(cfg)
	if err != nil {
		return nil, err
	}

	proto := DefaultProtocol
	if cfg.Protocol != "" {
		proto = cfg.Protocol
	}

	initDrv, ok := drivers[proto]
	if !ok {
		return nil, fmt.Errorf("unsupported protocol")
	}

	_, mgmt, _, err := initDrv(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return mgmt, err
}
