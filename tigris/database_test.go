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

package tigris

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris-client-go/test"
)

func TestDatabase(t *testing.T) {
	ms, cancel := test.SetupTests(t, 8)
	defer cancel()

	mc := ms.API

	ctx, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	testCfg := &Config{URL: test.GRPCURL(8), Project: "db1", Branch: "staging"}
	testCfg.TLS = test.SetupTLS(t)

	client, err := NewClient(ctx, testCfg)
	require.NoError(t, err)

	t.Run("creates a branch", func(t *testing.T) {
		mc.EXPECT().CreateBranch(gomock.Any(), pm(&api.CreateBranchRequest{
			Project: "db1",
			Branch:  "my-feature-branch",
		})).Return(&api.CreateBranchResponse{Status: "created"}, nil)

		resp, err1 := client.GetDatabase().CreateBranch(ctx, "my-feature-branch")
		require.NoError(t, err1)
		require.Equal(t, "created", resp.Status)
	})

	t.Run("throws error when create branch fails", func(t *testing.T) {
		mc.EXPECT().CreateBranch(gomock.Any(), pm(&api.CreateBranchRequest{
			Project: "db1",
			Branch:  "my_branch",
		})).DoAndReturn(func(ctx context.Context, req *api.CreateBranchRequest) (*api.CreateBranchResponse, error) {
			return nil, &api.TigrisError{Code: api.Code_ALREADY_EXISTS, Message: "branch already exists"}
		})

		resp, err1 := client.GetDatabase().CreateBranch(ctx, "my_branch")
		require.Nil(t, resp)
		require.Equal(t, &driver.Error{TigrisError: &api.TigrisError{Code: api.Code_ALREADY_EXISTS, Message: "branch already exists"}}, err1)
	})

	t.Run("deletes a branch", func(t *testing.T) {
		mc.EXPECT().DeleteBranch(gomock.Any(), pm(&api.DeleteBranchRequest{
			Project: "db1",
			Branch:  "my-feature-branch",
		})).Return(&api.DeleteBranchResponse{Status: "deleted"}, nil)

		resp, err1 := client.GetDatabase().DeleteBranch(ctx, "my-feature-branch")
		require.NoError(t, err1)
		require.Equal(t, "deleted", resp.Status)
	})

	t.Run("throws error when delete branch fails", func(t *testing.T) {
		mc.EXPECT().DeleteBranch(gomock.Any(), pm(&api.DeleteBranchRequest{
			Project: "db1",
			Branch:  "my_branch",
		})).DoAndReturn(func(ctx context.Context, req *api.DeleteBranchRequest) (*api.DeleteBranchResponse, error) {
			return nil, &api.TigrisError{Code: api.Code_NOT_FOUND, Message: "branch does not exist"}
		})

		resp, err1 := client.GetDatabase().DeleteBranch(ctx, "my_branch")
		require.Nil(t, resp)
		require.Equal(t, &driver.Error{TigrisError: &api.TigrisError{Code: api.Code_NOT_FOUND, Message: "branch does not exist"}}, err1)
	})
}
