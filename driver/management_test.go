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
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	mock "github.com/tigrisdata/tigris-client-go/mock/api"
)

var testTokenURLOverride = "https://localhost:33333/v1/auth/token" //nolint:golint,gosec

func appEqual(t *testing.T, exp *api.AppKey, act *AppKey) {
	t.Helper()

	require.Equal(t, exp.Id, act.Id)
	require.Equal(t, exp.Name, act.Name)
	require.Equal(t, exp.CreatedAt, act.CreatedAt)
	require.Equal(t, exp.CreatedBy, act.CreatedBy)
	require.Equal(t, exp.Description, act.Description)
}

func globalAppKeyEqual(t *testing.T, exp *api.GlobalAppKey, act *GlobalAppKey) {
	t.Helper()

	require.Equal(t, exp.Id, act.Id)
	require.Equal(t, exp.Name, act.Name)
	require.Equal(t, exp.CreatedAt, act.CreatedAt)
	require.Equal(t, exp.CreatedBy, act.CreatedBy)
	require.Equal(t, exp.Description, act.Description)
}

func testDriverAuthNegative(t *testing.T, drv Driver, m Management, mt *mock.MockTigrisServer, mc *mock.MockManagementServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	project1 := "project1"
	mt.EXPECT().CreateAppKey(gomock.Any(),
		pm(&api.CreateAppKeyRequest{Name: "app1", Description: "description1", Project: project1})).Return(
		&api.CreateAppKeyResponse{CreatedAppKey: nil}, nil)

	_, err := drv.CreateAppKey(ctx, project1, "app1", "description1")
	require.Error(t, err)

	mt.EXPECT().UpdateAppKey(gomock.Any(),
		pm(&api.UpdateAppKeyRequest{Id: "upd_id1", Project: project1, Name: "upd_app1", Description: "upd_description1"})).Return(
		&api.UpdateAppKeyResponse{UpdatedAppKey: nil}, nil)

	_, err = drv.UpdateAppKey(ctx, project1, "upd_id1", "upd_app1", "upd_description1")
	require.Error(t, err)

	mt.EXPECT().UpdateAppKey(gomock.Any(),
		pm(&api.UpdateAppKeyRequest{Id: "upd_id1", Project: project1, Name: "upd_app1", Description: "upd_description1"})).Return(
		&api.UpdateAppKeyResponse{UpdatedAppKey: &api.AppKey{Id: "id1"}}, fmt.Errorf("some error"))

	_, err = drv.UpdateAppKey(ctx, project1, "upd_id1", "upd_app1", "upd_description1")
	require.Error(t, err)

	mt.EXPECT().ListAppKeys(gomock.Any(),
		pm(&api.ListAppKeysRequest{Project: project1})).Return(
		nil, fmt.Errorf("some error"))

	_, err = drv.ListAppKeys(ctx, project1)
	require.Error(t, err)

	mt.EXPECT().RotateAppKeySecret(gomock.Any(),
		pm(&api.RotateAppKeyRequest{Id: "ras_id1", Project: project1})).Return(
		&api.RotateAppKeyResponse{AppKey: nil}, nil)

	_, err = drv.RotateAppKeySecret(ctx, project1, "ras_id1")
	require.Error(t, err)

	mt.EXPECT().RotateAppKeySecret(gomock.Any(),
		pm(&api.RotateAppKeyRequest{Id: "ras_id1", Project: project1})).Return(
		&api.RotateAppKeyResponse{AppKey: &api.AppKey{Id: "id1"}}, fmt.Errorf("some error"))

	_, err = drv.RotateAppKeySecret(ctx, project1, "ras_id1")
	require.Error(t, err)

	mc.EXPECT().CreateNamespace(gomock.Any(),
		pm(&api.CreateNamespaceRequest{Name: "app1"})).Return(
		nil, fmt.Errorf("some error"))

	err = m.CreateNamespace(ctx, "app1")
	require.Error(t, err)

	mc.EXPECT().ListNamespaces(gomock.Any(),
		pm(&api.ListNamespacesRequest{})).Return(
		nil, fmt.Errorf("some error"))

	_, err = m.ListNamespaces(ctx)
	require.Error(t, err)
}

func testDriverAuth(t *testing.T, drv Driver, m Management, mt *mock.MockTigrisServer, ma *mock.MockAuthServer,
	mc *mock.MockManagementServer,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	capp := &api.AppKey{Id: "id1", Name: "app1", Description: "desc1", CreatedAt: 111, CreatedBy: "cby", Secret: "secret"}

	project1 := "project1"
	mt.EXPECT().CreateAppKey(gomock.Any(),
		pm(&api.CreateAppKeyRequest{Name: "app1", Description: "description1", Project: project1})).Return(
		&api.CreateAppKeyResponse{CreatedAppKey: capp}, nil)

	app, err := drv.CreateAppKey(ctx, project1, "app1", "description1")
	require.NoError(t, err)

	appEqual(t, capp, app)

	mt.EXPECT().DeleteAppKey(gomock.Any(),
		pm(&api.DeleteAppKeyRequest{Id: "del_id1", Project: project1})).Return(
		&api.DeleteAppKeyResponse{Deleted: true}, nil)

	err = drv.DeleteAppKey(ctx, project1, "del_id1")
	require.NoError(t, err)

	uapp := &api.AppKey{Id: "upd_id1", Name: "upd_app1", Description: "upd_desc1", UpdatedAt: 222, UpdatedBy: "uby"}

	mt.EXPECT().UpdateAppKey(gomock.Any(),
		pm(&api.UpdateAppKeyRequest{Id: "upd_id1", Project: project1, Name: "upd_app1", Description: "upd_description1"})).Return(
		&api.UpdateAppKeyResponse{UpdatedAppKey: uapp}, nil)

	app, err = drv.UpdateAppKey(ctx, project1, "upd_id1", "upd_app1", "upd_description1")
	require.NoError(t, err)

	appEqual(t, uapp, app)

	lapp1 := &api.AppKey{
		Id: "list_id1", Name: "list_app1", Description: "list_desc1", UpdatedAt: 222,
		UpdatedBy: "uby", CreatedAt: 111, CreatedBy: "cby",
	}
	lapp2 := &api.AppKey{
		Id: "list_id2", Name: "list_app2", Description: "list_desc2", UpdatedAt: 222222,
		UpdatedBy: "uby2", CreatedAt: 111222, CreatedBy: "cby2",
	}
	lapp3 := &api.AppKey{
		Id: "list_id3", Name: "list_app3", Description: "list_desc3", UpdatedAt: 222333,
		UpdatedBy: "uby3", CreatedAt: 111333, CreatedBy: "cby3",
	}

	mt.EXPECT().ListAppKeys(gomock.Any(),
		pm(&api.ListAppKeysRequest{Project: project1})).Return(
		&api.ListAppKeysResponse{AppKeys: []*api.AppKey{lapp1, lapp2, lapp3}}, nil)

	list, err := drv.ListAppKeys(ctx, project1)
	require.NoError(t, err)

	require.Equal(t, 3, len(list))
	appEqual(t, lapp1, list[0])
	appEqual(t, lapp2, list[1])
	appEqual(t, lapp3, list[2])

	mt.EXPECT().ListAppKeys(gomock.Any(),
		pm(&api.ListAppKeysRequest{Project: project1})).Return(
		&api.ListAppKeysResponse{}, nil)

	list, err = drv.ListAppKeys(ctx, project1)
	require.NoError(t, err)
	require.Equal(t, 0, len(list))

	mt.EXPECT().RotateAppKeySecret(gomock.Any(),
		pm(&api.RotateAppKeyRequest{Id: "ras_id1", Project: project1})).Return(
		&api.RotateAppKeyResponse{AppKey: lapp2}, nil)

	app, err = drv.RotateAppKeySecret(ctx, project1, "ras_id1")
	require.NoError(t, err)

	appEqual(t, lapp2, app)

	ma.EXPECT().GetAccessToken(gomock.Any(),
		pm(&api.GetAccessTokenRequest{
			ClientId: "client_id1", ClientSecret: "secret1",
			GrantType: api.GrantType_CLIENT_CREDENTIALS,
		})).Return(
		&api.GetAccessTokenResponse{AccessToken: "access_token1", RefreshToken: "refresh_token1"}, nil)

	token, err := m.GetAccessToken(ctx, "client_id1", "secret1", "")
	require.NoError(t, err)
	require.Equal(t, "access_token1", token.AccessToken)
	require.Equal(t, "refresh_token1", token.RefreshToken)

	ma.EXPECT().GetAccessToken(gomock.Any(),
		pm(&api.GetAccessTokenRequest{
			ClientId: "", ClientSecret: "", RefreshToken: "refresh_token_req",
			GrantType: api.GrantType_REFRESH_TOKEN,
		})).Return(
		&api.GetAccessTokenResponse{AccessToken: "access_token2", RefreshToken: "refresh_token2"}, nil)

	token, err = m.GetAccessToken(ctx, "", "", "refresh_token_req")
	require.NoError(t, err)
	require.Equal(t, "access_token2", token.AccessToken)
	require.Equal(t, "refresh_token2", token.RefreshToken)

	nsapp1 := &api.NamespaceInfo{Name: "ns_list_app1"}
	nsapp2 := &api.NamespaceInfo{Name: "ns_list_app2"}
	nsapp3 := &api.NamespaceInfo{Name: "ns_list_app3"}

	mc.EXPECT().ListNamespaces(gomock.Any(),
		pm(&api.ListNamespacesRequest{})).Return(
		&api.ListNamespacesResponse{Namespaces: []*api.NamespaceInfo{nsapp1, nsapp2, nsapp3}}, nil)

	nsList, err := m.ListNamespaces(ctx)
	require.NoError(t, err)

	require.Equal(t, 3, len(nsList))
	assert.Equal(t, nsapp1.Id, nsList[0].Id)
	assert.Equal(t, nsapp1.Name, nsList[0].Name)
	assert.Equal(t, nsapp2.Id, nsList[1].Id)
	assert.Equal(t, nsapp2.Name, nsList[1].Name)
	assert.Equal(t, nsapp3.Id, nsList[2].Id)
	assert.Equal(t, nsapp3.Name, nsList[2].Name)

	mc.EXPECT().CreateNamespace(gomock.Any(),
		pm(&api.CreateNamespaceRequest{Name: "nsapp1"})).Return(
		&api.CreateNamespaceResponse{}, nil)

	err = m.CreateNamespace(ctx, "nsapp1")
	require.NoError(t, err)
}

func testGlobalAppKeys(t *testing.T, drv Driver, mt *mock.MockTigrisServer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	capp := &api.GlobalAppKey{Id: "id1", Name: "app1", Description: "desc1", CreatedAt: 111, CreatedBy: "cby", Secret: "secret"}

	mt.EXPECT().CreateGlobalAppKey(gomock.Any(),
		pm(&api.CreateGlobalAppKeyRequest{Name: "app1", Description: "description1"})).Return(
		&api.CreateGlobalAppKeyResponse{CreatedAppKey: capp}, nil)

	app, err := drv.CreateGlobalAppKey(ctx, "app1", "description1")
	require.NoError(t, err)

	globalAppKeyEqual(t, capp, app)

	mt.EXPECT().DeleteGlobalAppKey(gomock.Any(),
		pm(&api.DeleteGlobalAppKeyRequest{Id: "del_id1"})).Return(
		&api.DeleteGlobalAppKeyResponse{Deleted: true}, nil)

	err = drv.DeleteGlobalAppKey(ctx, "del_id1")
	require.NoError(t, err)

	uapp := &api.GlobalAppKey{Id: "upd_id1", Name: "upd_app1", Description: "upd_desc1", UpdatedAt: 222, UpdatedBy: "uby"}

	mt.EXPECT().UpdateGlobalAppKey(gomock.Any(),
		pm(&api.UpdateGlobalAppKeyRequest{Id: "upd_id1", Name: "upd_app1", Description: "upd_description1"})).Return(
		&api.UpdateGlobalAppKeyResponse{UpdatedAppKey: uapp}, nil)

	app, err = drv.UpdateGlobalAppKey(ctx, "upd_id1", "upd_app1", "upd_description1")
	require.NoError(t, err)

	globalAppKeyEqual(t, uapp, app)

	lapp1 := &api.GlobalAppKey{
		Id: "list_id1", Name: "list_app1", Description: "list_desc1", UpdatedAt: 222,
		UpdatedBy: "uby", CreatedAt: 111, CreatedBy: "cby",
	}
	lapp2 := &api.GlobalAppKey{
		Id: "list_id2", Name: "list_app2", Description: "list_desc2", UpdatedAt: 222222,
		UpdatedBy: "uby2", CreatedAt: 111222, CreatedBy: "cby2",
	}
	lapp3 := &api.GlobalAppKey{
		Id: "list_id3", Name: "list_app3", Description: "list_desc3", UpdatedAt: 222333,
		UpdatedBy: "uby3", CreatedAt: 111333, CreatedBy: "cby3",
	}

	mt.EXPECT().ListGlobalAppKeys(gomock.Any(),
		pm(&api.ListGlobalAppKeysRequest{})).Return(
		&api.ListGlobalAppKeysResponse{AppKeys: []*api.GlobalAppKey{lapp1, lapp2, lapp3}}, nil)

	list, err := drv.ListGlobalAppKeys(ctx)
	require.NoError(t, err)

	require.Equal(t, 3, len(list))
	globalAppKeyEqual(t, lapp1, list[0])
	globalAppKeyEqual(t, lapp2, list[1])
	globalAppKeyEqual(t, lapp3, list[2])

	mt.EXPECT().ListGlobalAppKeys(gomock.Any(),
		pm(&api.ListGlobalAppKeysRequest{})).Return(
		&api.ListGlobalAppKeysResponse{}, nil)

	list, err = drv.ListGlobalAppKeys(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(list))

	mt.EXPECT().RotateGlobalAppKeySecret(gomock.Any(),
		pm(&api.RotateGlobalAppKeySecretRequest{Id: "ras_id1"})).Return(
		&api.RotateGlobalAppKeySecretResponse{AppKey: lapp2}, nil)

	app, err = drv.RotateGlobalAppKeySecret(ctx, "ras_id1")
	require.NoError(t, err)

	globalAppKeyEqual(t, lapp2, app)
}

func testGlobalGlobalAppKeysNegative(t *testing.T, drv Driver, mt *mock.MockTigrisServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mt.EXPECT().CreateGlobalAppKey(gomock.Any(),
		pm(&api.CreateGlobalAppKeyRequest{Name: "app1", Description: "description1"})).Return(
		&api.CreateGlobalAppKeyResponse{CreatedAppKey: nil}, nil)

	_, err := drv.CreateGlobalAppKey(ctx, "app1", "description1")
	require.Error(t, err)

	mt.EXPECT().UpdateGlobalAppKey(gomock.Any(),
		pm(&api.UpdateGlobalAppKeyRequest{Id: "upd_id1", Name: "upd_app1", Description: "upd_description1"})).Return(
		&api.UpdateGlobalAppKeyResponse{UpdatedAppKey: nil}, nil)

	_, err = drv.UpdateGlobalAppKey(ctx, "upd_id1", "upd_app1", "upd_description1")
	require.Error(t, err)

	mt.EXPECT().UpdateGlobalAppKey(gomock.Any(),
		pm(&api.UpdateGlobalAppKeyRequest{Id: "upd_id1", Name: "upd_app1", Description: "upd_description1"})).Return(
		&api.UpdateGlobalAppKeyResponse{UpdatedAppKey: &api.GlobalAppKey{Id: "id1"}}, fmt.Errorf("some error"))

	_, err = drv.UpdateGlobalAppKey(ctx, "upd_id1", "upd_app1", "upd_description1")
	require.Error(t, err)

	mt.EXPECT().ListGlobalAppKeys(gomock.Any(),
		pm(&api.ListGlobalAppKeysRequest{})).Return(
		nil, fmt.Errorf("some error"))

	_, err = drv.ListGlobalAppKeys(ctx)
	require.Error(t, err)

	mt.EXPECT().RotateGlobalAppKeySecret(gomock.Any(),
		pm(&api.RotateGlobalAppKeySecretRequest{Id: "ras_id1"})).Return(
		&api.RotateGlobalAppKeySecretResponse{AppKey: nil}, nil)

	_, err = drv.RotateGlobalAppKeySecret(ctx, "ras_id1")
	require.Error(t, err)

	mt.EXPECT().RotateGlobalAppKeySecret(gomock.Any(),
		pm(&api.RotateGlobalAppKeySecretRequest{Id: "ras_id1"})).Return(
		&api.RotateGlobalAppKeySecretResponse{AppKey: &api.GlobalAppKey{Id: "id1"}}, fmt.Errorf("some error"))

	_, err = drv.RotateGlobalAppKeySecret(ctx, "ras_id1")
	require.Error(t, err)
}

func testDriverToken(t *testing.T, d Driver, mc *mock.MockTigrisServer, mca *mock.MockAuthServer, token string, getToken bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if getToken {
		mca.EXPECT().GetAccessToken(gomock.Any(),
			pm(&api.GetAccessTokenRequest{
				ClientId: "client_id_test", ClientSecret: "client_secret_test",
				GrantType: api.GrantType_CLIENT_CREDENTIALS,
			})).Return(
			&api.GetAccessTokenResponse{AccessToken: token, RefreshToken: "refresh_token1", ExpiresIn: 11}, nil)
	}

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{},
		})).Do(func(ctx context.Context, r *api.DeleteRequest) {
		assert.Equal(t, "Bearer "+token, metautils.ExtractIncoming(ctx).Get("authorization"))
		ua := "grpcgateway-user-agent"
		if metautils.ExtractIncoming(ctx).Get(ua) == "" {
			ua = "user-agent"
		}
		assert.True(t, strings.Contains(metautils.ExtractIncoming(ctx).Get(ua), UserAgent))
	}).Return(&api.DeleteResponse{}, nil)

	db := d.UseDatabase("db1")

	_, err := db.Delete(ctx, "c1", Filter(`{"filter":"value"}`))
	require.NoError(t, err)

	// Should reuse the token for subsequent API calls
	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Project:    "db1",
			Collection: "c1",
			Filter:     []byte(`{"filter":"value"}`),
			Options:    &api.DeleteRequestOptions{},
		})).Do(func(ctx context.Context, r *api.DeleteRequest) {
		assert.Equal(t, "Bearer "+token, metautils.ExtractIncoming(ctx).Get("authorization"))
		ua := "grpcgateway-user-agent"
		if metautils.ExtractIncoming(ctx).Get(ua) == "" {
			ua = "user-agent"
		}
		assert.True(t, strings.Contains(metautils.ExtractIncoming(ctx).Get(ua), UserAgent))
	}).Return(&api.DeleteResponse{}, nil)

	_, err = db.Delete(ctx, "c1", Filter(`{"filter":"value"}`))
	require.NoError(t, err)
}

func testGetInfo(t *testing.T, c Driver, mc *mock.MockObservabilityServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mc.EXPECT().GetInfo(gomock.Any(),
		pm(&api.GetInfoRequest{})).Return(&api.GetInfoResponse{ServerVersion: "some version"}, nil)

	info, err := c.Info(ctx)
	require.NoError(t, err)
	require.Equal(t, "some version", info.ServerVersion)

	mc.EXPECT().GetInfo(gomock.Any(),
		pm(&api.GetInfoRequest{})).Return(nil, fmt.Errorf("some error"))

	_, err = c.Info(ctx)
	require.Error(t, err)
}

func testInvitations(t *testing.T, m Management, ma *mock.MockAuthServer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("create_invitations", func(t *testing.T) {
		ma.EXPECT().CreateInvitations(gomock.Any(),
			pm(&api.CreateInvitationsRequest{
				Invitations: []*api.InvitationInfo{
					{
						Email:                "email1",
						Role:                 "role1",
						InvitationSentByName: "inv_by_name1",
					},
					{
						Email:                "email2",
						Role:                 "role2",
						InvitationSentByName: "inv_by_name2",
					},
				},
			})).Return(&api.CreateInvitationsResponse{
			Status:  "created",
			Message: "msg1",
			Count:   2,
		}, nil)

		err := m.CreateInvitations(ctx, []*InvitationInfo{
			{
				Email:                "email1",
				Role:                 "role1",
				InvitationSentByName: "inv_by_name1",
			},
			{
				Email:                "email2",
				Role:                 "role2",
				InvitationSentByName: "inv_by_name2",
			},
		})
		require.NoError(t, err)
	})

	t.Run("delete_invitations", func(t *testing.T) {
		status := "status1"

		ma.EXPECT().DeleteInvitations(gomock.Any(),
			pm(&api.DeleteInvitationsRequest{
				Email:  "email1",
				Status: &status,
			}),
		).Return(&api.DeleteInvitationsResponse{Status: "deleted", Message: "msg1"}, nil)

		err := m.DeleteInvitations(ctx, "email1", "status1")
		require.NoError(t, err)
	})

	t.Run("verify_invitations", func(t *testing.T) {
		ma.EXPECT().VerifyInvitation(gomock.Any(),
			pm(&api.VerifyInvitationRequest{
				Email: "email1",
				Code:  "code1",
			}),
		).Return(&api.VerifyInvitationResponse{
			TigrisNamespace:     "ns1",
			TigrisNamespaceName: "ns_name1",
			Role:                "role1",
		}, nil)

		err := m.VerifyInvitation(ctx, "email1", "code1")
		require.NoError(t, err)
	})

	t.Run("list_invitations", func(t *testing.T) {
		status := "status1"
		ma.EXPECT().ListInvitations(gomock.Any(),
			pm(&api.ListInvitationsRequest{
				Status: &status,
			}),
		).Return(&api.ListInvitationsResponse{
			Invitations: []*api.Invitation{
				{
					Email:               "email1",
					Role:                "role1",
					Status:              "status1",
					TigrisNamespace:     "ns1",
					TigrisNamespaceName: "ns_name1",
					CreatedBy:           "created_by1",
					CreatedByName:       "created_by_name1",
					ExpirationTime:      12345,
				},
				{
					Email:               "email2",
					Role:                "role2",
					Status:              "status2",
					TigrisNamespace:     "ns2",
					TigrisNamespaceName: "ns_name2",
					CreatedBy:           "created_by2",
					CreatedByName:       "created_by_name2",
					ExpirationTime:      22345,
				},
			},
		}, nil)

		invs, err := m.ListInvitations(ctx, "status1")
		require.NoError(t, err)

		require.Equal(t, []*Invitation{
			{
				Email:               "email1",
				Role:                "role1",
				Status:              "status1",
				TigrisNamespace:     "ns1",
				TigrisNamespaceName: "ns_name1",
				CreatedBy:           "created_by1",
				CreatedByName:       "created_by_name1",
				ExpirationTime:      12345,
			},
			{
				Email:               "email2",
				Role:                "role2",
				Status:              "status2",
				TigrisNamespace:     "ns2",
				TigrisNamespaceName: "ns_name2",
				CreatedBy:           "created_by2",
				CreatedByName:       "created_by_name2",
				ExpirationTime:      22345,
			},
		}, invs)
	})

	t.Run("list_users", func(t *testing.T) {
		ma.EXPECT().ListUsers(gomock.Any(),
			pm(&api.ListUsersRequest{}),
		).Return(&api.ListUsersResponse{
			Users: []*api.User{
				{
					Email:     "email1",
					Name:      "name1",
					CreatedAt: 12345,
					Picture:   "pic1",
					Role:      "role1",
				},
				{
					Email:     "email2",
					Name:      "name2",
					CreatedAt: 22345,
					Picture:   "pic2",
					Role:      "role2",
				},
			},
		}, nil)
		users, err := m.ListUsers(ctx)
		require.NoError(t, err)
		require.Equal(t, []*User{
			{
				Email:     "email1",
				Name:      "name1",
				CreatedAt: 12345,
				Picture:   "pic1",
				Role:      "role1",
			},
			{
				Email:     "email2",
				Name:      "name2",
				CreatedAt: 22345,
				Picture:   "pic2",
				Role:      "role2",
			},
		}, users)
	})
}

func testInvitationsNegative(t *testing.T, m Management, ma *mock.MockAuthServer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("create_invitations", func(t *testing.T) {
		ma.EXPECT().CreateInvitations(gomock.Any(),
			pm(&api.CreateInvitationsRequest{
				Invitations: []*api.InvitationInfo{
					{
						Email:                "email1",
						Role:                 "role1",
						InvitationSentByName: "inv_by_name1",
					},
				},
			})).Return(nil, fmt.Errorf("some error"))

		err := m.CreateInvitations(ctx, []*InvitationInfo{
			{
				Email:                "email1",
				Role:                 "role1",
				InvitationSentByName: "inv_by_name1",
			},
		})
		require.Error(t, err)
	})

	t.Run("delete_invitations", func(t *testing.T) {
		status := "status1"

		ma.EXPECT().DeleteInvitations(gomock.Any(),
			pm(&api.DeleteInvitationsRequest{
				Email:  "email1",
				Status: &status,
			}),
		).Return(nil, fmt.Errorf("some error"))

		err := m.DeleteInvitations(ctx, "email1", "status1")
		require.Error(t, err)
	})

	t.Run("verify_invitations", func(t *testing.T) {
		ma.EXPECT().VerifyInvitation(gomock.Any(),
			pm(&api.VerifyInvitationRequest{
				Email: "email1",
				Code:  "code1",
			}),
		).Return(nil, fmt.Errorf("some error"))

		err := m.VerifyInvitation(ctx, "email1", "code1")
		require.Error(t, err)
	})

	t.Run("list_invitations", func(t *testing.T) {
		status := "status1"
		ma.EXPECT().ListInvitations(gomock.Any(),
			pm(&api.ListInvitationsRequest{
				Status: &status,
			}),
		).Return(nil, fmt.Errorf("some error"))

		_, err := m.ListInvitations(ctx, "status1")
		require.Error(t, err)
	})

	t.Run("list_users", func(t *testing.T) {
		ma.EXPECT().ListUsers(gomock.Any(),
			pm(&api.ListUsersRequest{}),
		).Return(nil, nil)
		_, err := m.ListUsers(ctx)
		require.Error(t, err)
	})
}
