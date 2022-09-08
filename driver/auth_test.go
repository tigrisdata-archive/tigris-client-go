package driver

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	"github.com/tigrisdata/tigris-client-go/config"
	mock "github.com/tigrisdata/tigris-client-go/mock/api"
	"github.com/tigrisdata/tigris-client-go/test"
)

func appEqual(t *testing.T, exp *api.Application, act *Application) {
	require.Equal(t, exp.Id, act.Id)
	require.Equal(t, exp.Name, act.Name)
	require.Equal(t, exp.CreatedAt, act.CreatedAt)
	require.Equal(t, exp.CreatedBy, act.CreatedBy)
	require.Equal(t, exp.Description, act.Description)
}

func testDriverAuthNegative(t *testing.T, c Auth, mc *mock.MockAuthServer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mc.EXPECT().CreateApplication(gomock.Any(),
		pm(&api.CreateApplicationRequest{Name: "app1", Description: "description1"})).Return(
		&api.CreateApplicationResponse{CreatedApplication: nil}, nil)

	_, err := c.CreateApplication(ctx, "app1", "description1")
	require.Error(t, err)

	mc.EXPECT().UpdateApplication(gomock.Any(),
		pm(&api.UpdateApplicationRequest{Id: "upd_id1", Name: "upd_app1", Description: "upd_description1"})).Return(
		&api.UpdateApplicationResponse{UpdatedApplication: nil}, nil)

	_, err = c.UpdateApplication(ctx, "upd_id1", "upd_app1", "upd_description1")
	require.Error(t, err)

	mc.EXPECT().RotateApplicationSecret(gomock.Any(),
		pm(&api.RotateApplicationSecretRequest{Id: "ras_id1"})).Return(
		&api.RotateApplicationSecretResponse{Application: nil}, nil)

	_, err = c.RotateApplicationSecret(ctx, "ras_id1")
	require.Error(t, err)
}

func testDriverAuth(t *testing.T, c Auth, mc *mock.MockAuthServer) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	capp := &api.Application{Id: "id1", Name: "app1", Description: "desc1", CreatedAt: 111, CreatedBy: "cby", Secret: "secret"}

	mc.EXPECT().CreateApplication(gomock.Any(),
		pm(&api.CreateApplicationRequest{Name: "app1", Description: "description1"})).Return(
		&api.CreateApplicationResponse{CreatedApplication: capp}, nil)

	app, err := c.CreateApplication(ctx, "app1", "description1")
	require.NoError(t, err)

	appEqual(t, capp, app)

	mc.EXPECT().DeleteApplication(gomock.Any(),
		pm(&api.DeleteApplicationsRequest{Id: "del_id1"})).Return(
		&api.DeleteApplicationResponse{Deleted: true}, nil)

	err = c.DeleteApplication(ctx, "del_id1")
	require.NoError(t, err)

	uapp := &api.Application{Id: "upd_id1", Name: "upd_app1", Description: "upd_desc1", UpdatedAt: 222, UpdatedBy: "uby"}

	mc.EXPECT().UpdateApplication(gomock.Any(),
		pm(&api.UpdateApplicationRequest{Id: "upd_id1", Name: "upd_app1", Description: "upd_description1"})).Return(
		&api.UpdateApplicationResponse{UpdatedApplication: uapp}, nil)

	app, err = c.UpdateApplication(ctx, "upd_id1", "upd_app1", "upd_description1")
	require.NoError(t, err)

	appEqual(t, uapp, app)

	lapp1 := &api.Application{Id: "list_id1", Name: "list_app1", Description: "list_desc1", UpdatedAt: 222, UpdatedBy: "uby", CreatedAt: 111, CreatedBy: "cby"}
	lapp2 := &api.Application{Id: "list_id2", Name: "list_app2", Description: "list_desc2", UpdatedAt: 222222, UpdatedBy: "uby2", CreatedAt: 111222, CreatedBy: "cby2"}
	lapp3 := &api.Application{Id: "list_id3", Name: "list_app3", Description: "list_desc3", UpdatedAt: 222333, UpdatedBy: "uby3", CreatedAt: 111333, CreatedBy: "cby3"}

	mc.EXPECT().ListApplications(gomock.Any(),
		pm(&api.ListApplicationsRequest{})).Return(
		&api.ListApplicationsResponse{Applications: []*api.Application{lapp1, lapp2, lapp3}}, nil)

	list, err := c.ListApplications(ctx)
	require.NoError(t, err)

	require.Equal(t, 3, len(list))
	appEqual(t, lapp1, list[0])
	appEqual(t, lapp2, list[1])
	appEqual(t, lapp3, list[2])

	mc.EXPECT().ListApplications(gomock.Any(),
		pm(&api.ListApplicationsRequest{})).Return(
		&api.ListApplicationsResponse{}, nil)

	list, err = c.ListApplications(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, len(list))

	mc.EXPECT().RotateApplicationSecret(gomock.Any(),
		pm(&api.RotateApplicationSecretRequest{Id: "ras_id1"})).Return(
		&api.RotateApplicationSecretResponse{Application: lapp2}, nil)

	app, err = c.RotateApplicationSecret(ctx, "ras_id1")
	require.NoError(t, err)

	appEqual(t, lapp2, app)

	mc.EXPECT().GetAccessToken(gomock.Any(),
		pm(&api.GetAccessTokenRequest{ClientId: "client_id1", ClientSecret: "secret1", GrantType: api.GrantType_CLIENT_CREDENTIALS})).Return(
		&api.GetAccessTokenResponse{AccessToken: "access_token1", RefreshToken: "refresh_token1"}, nil)

	token, err := c.GetAccessToken(ctx, "client_id1", "secret1", "")
	require.NoError(t, err)
	require.Equal(t, "access_token1", token.AccessToken)
	require.Equal(t, "refresh_token1", token.RefreshToken)

	mc.EXPECT().GetAccessToken(gomock.Any(),
		pm(&api.GetAccessTokenRequest{ClientId: "", ClientSecret: "", RefreshToken: "refresh_token_req", GrantType: api.GrantType_REFRESH_TOKEN})).Return(
		&api.GetAccessTokenResponse{AccessToken: "access_token2", RefreshToken: "refresh_token2"}, nil)

	token, err = c.GetAccessToken(ctx, "", "", "refresh_token_req")
	require.NoError(t, err)
	require.Equal(t, "access_token2", token.AccessToken)
	require.Equal(t, "refresh_token2", token.RefreshToken)
}

func TestGRPCAuthDriver(t *testing.T) {
	_, client, _, mockAuthServer, cancel := SetupAuthGRPCTests(t, &config.Driver{})
	defer cancel()
	testDriverAuth(t, client, mockAuthServer)
	testDriverAuthNegative(t, client, mockAuthServer)
}

func TestHTTPAuthDriver(t *testing.T) {
	_, client, _, mockAuthServer, cancel := SetupAuthHTTPTests(t, &config.Driver{})
	defer cancel()
	testDriverAuth(t, client, mockAuthServer)
}

func testDriverToken(t *testing.T, d Driver, mc *mock.MockTigrisServer, mca *mock.MockAuthServer, token string, getToken bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if getToken {
		mca.EXPECT().GetAccessToken(gomock.Any(),
			pm(&api.GetAccessTokenRequest{ClientId: "client_id_test", ClientSecret: "client_secret_test", GrantType: api.GrantType_CLIENT_CREDENTIALS})).Return(
			&api.GetAccessTokenResponse{AccessToken: token, RefreshToken: "refresh_token1"}, nil)
	}

	mc.EXPECT().Delete(gomock.Any(),
		pm(&api.DeleteRequest{
			Db:         "db1",
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
			Db:         "db1",
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

func TestGRPCDriverCredentials(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		TokenURLOverride = "https://localhost:33333/auth/token"
		client, _, mockServer, mockAuthServer, cancel := SetupAuthGRPCTests(t, &config.Driver{
			URL:               test.GRPCURL(0),
			ApplicationId:     "client_id_test",
			ApplicationSecret: "client_secret_test",
		})
		defer cancel()

		testDriverToken(t, client, mockServer, mockAuthServer, "token_config_123", true)
	})

	t.Run("env", func(t *testing.T) {
		TokenURLOverride = "https://localhost:33333/auth/token"

		err := os.Setenv(ApplicationID, "client_id_test")
		require.NoError(t, err)
		err = os.Setenv(ApplicationSecret, "client_secret_test")
		require.NoError(t, err)

		defer func() {
			err = os.Unsetenv(ApplicationSecret)
			require.NoError(t, err)
			err = os.Unsetenv(ApplicationID)
			require.NoError(t, err)
		}()

		client, _, mockServer, mockAuthServer, cancel := SetupAuthGRPCTests(t, &config.Driver{
			URL: test.GRPCURL(0),
		})
		defer cancel()

		testDriverToken(t, client, mockServer, mockAuthServer, "token_config_123", true)
	})
}

func TestHTTPDriverCredentials(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		TokenURLOverride = ""
		client, _, mockServer, mockAuthServer, cancel := SetupAuthHTTPTests(t, &config.Driver{
			URL:               test.HTTPURL(2),
			ApplicationId:     "client_id_test",
			ApplicationSecret: "client_secret_test",
		})
		defer cancel()

		testDriverToken(t, client, mockServer, mockAuthServer, "token_config_123", true)
	})

	t.Run("env", func(t *testing.T) {
		TokenURLOverride = ""
		err := os.Setenv(ApplicationID, "client_id_test")
		require.NoError(t, err)
		err = os.Setenv(ApplicationSecret, "client_secret_test")
		require.NoError(t, err)
		defer func() {
			err = os.Unsetenv(ApplicationSecret)
			require.NoError(t, err)
			err = os.Unsetenv(ApplicationID)
			require.NoError(t, err)
		}()

		client, _, mockServer, mockAuthServer, cancel := SetupAuthHTTPTests(t, &config.Driver{
			URL: test.HTTPURL(2),
		})
		defer cancel()

		testDriverToken(t, client, mockServer, mockAuthServer, "token_config_123", true)
	})
}

func TestGRPCDriverToken(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		// this is needed because in the tests GRPC and HTTP servers listen on different ports
		// HTTP - 33333, GRPC 33334
		TokenURLOverride = "https://localhost:33333/auth/token"
		client, _, mockServer, mockAuthServer, cancel := SetupAuthGRPCTests(t, &config.Driver{
			URL:   test.GRPCURL(0),
			Token: "token_grpc_config_1",
		})
		defer cancel()

		testDriverToken(t, client, mockServer, mockAuthServer, "token_grpc_config_1", false)
	})

	t.Run("env", func(t *testing.T) {
		TokenURLOverride = "https://localhost:33333/auth/token"

		err := os.Setenv(Token, "token_grpc_env_1")
		require.NoError(t, err)
		defer func() {
			err = os.Unsetenv(Token)
			require.NoError(t, err)
		}()

		client, _, mockServer, mockAuthServer, cancel := SetupAuthGRPCTests(t, &config.Driver{
			URL: test.GRPCURL(0),
		})
		defer cancel()

		testDriverToken(t, client, mockServer, mockAuthServer, "token_grpc_env_1", false)
	})
}

func TestHTTPDriverToken(t *testing.T) {
	t.Run("config", func(t *testing.T) {
		TokenURLOverride = ""
		client, _, mockServer, mockAuthServer, cancel := SetupAuthHTTPTests(t, &config.Driver{
			URL:   test.HTTPURL(2),
			Token: "token_http_config_1",
		})
		defer cancel()

		testDriverToken(t, client, mockServer, mockAuthServer, "token_http_config_1", false)
	})

	t.Run("env", func(t *testing.T) {
		TokenURLOverride = ""
		err := os.Setenv(Token, "token_http_env_1")
		require.NoError(t, err)
		defer func() {
			err = os.Unsetenv(Token)
			require.NoError(t, err)
		}()

		client, _, mockServer, mockAuthServer, cancel := SetupAuthHTTPTests(t, &config.Driver{
			URL: test.HTTPURL(2),
		})
		defer cancel()

		testDriverToken(t, client, mockServer, mockAuthServer, "token_http_env_1", false)
	})
}
