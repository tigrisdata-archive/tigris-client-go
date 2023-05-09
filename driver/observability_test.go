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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	mock "github.com/tigrisdata/tigris-client-go/mock/api"
)

func testDriverObservabilityNegative(t *testing.T, c Observability, mc *mock.MockObservabilityServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mc.EXPECT().QuotaLimits(gomock.Any(),
		pm(&api.QuotaLimitsRequest{})).Return(
		nil, fmt.Errorf("error1"))

	_, err := c.QuotaLimits(ctx)
	require.Error(t, err)

	mc.EXPECT().QuotaUsage(gomock.Any(),
		pm(&api.QuotaUsageRequest{})).Return(
		nil, fmt.Errorf("error2"))

	_, err = c.QuotaUsage(ctx)
	require.Error(t, err)
}

func testDriverObservability(t *testing.T, c Observability, mc *mock.MockObservabilityServer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mc.EXPECT().QuotaLimits(gomock.Any(),
		pm(&api.QuotaLimitsRequest{})).Return(
		&api.QuotaLimitsResponse{ReadUnits: 1000, WriteUnits: 500}, nil)

	lim, err := c.QuotaLimits(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(500), lim.WriteUnits)
	assert.Equal(t, int64(1000), lim.ReadUnits)

	mc.EXPECT().QuotaUsage(gomock.Any(),
		pm(&api.QuotaUsageRequest{})).Return(
		&api.QuotaUsageResponse{ReadUnits: 1000, WriteUnits: 500}, nil)

	u, err := c.QuotaUsage(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(500), u.WriteUnits)
	assert.Equal(t, int64(1000), u.ReadUnits)
}
