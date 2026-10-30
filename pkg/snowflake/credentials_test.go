package snowflake

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func credentialRowTypes() []map[string]interface{} {
	return []map[string]interface{}{
		{"name": "USER_NAME", "type": "text"},
		{"name": "TYPE", "type": "text"},
		{"name": "NAME", "type": "text"},
		{"name": "PUBLIC_KEY_FP", "type": "text"},
		{"name": "LAST_USED_ON", "type": "timestamp_ltz"},
	}
}

type credentialsMock struct {
	posts      atomic.Int32
	status     int
	code       string
	async      bool
	partitions [][][]*string
}

func str(s string) *string { return &s }

func (m *credentialsMock) serve(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			m.posts.Add(1)
			if m.status != 0 {
				w.WriteHeader(m.status)
				_ = json.NewEncoder(w).Encode(map[string]string{"code": m.code, "message": "statement failed"})
				return
			}
			if m.async {
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(map[string]string{"statementHandle": "handle"})
				return
			}
		}
		partition := 0
		if p := r.URL.Query().Get("partition"); p != "" {
			partition = int(p[0] - '0')
		}
		body := map[string]interface{}{"statementHandle": "handle", "data": m.partitions[partition]}
		if partition == 0 {
			numRows := 0
			info := make([]map[string]int, 0, len(m.partitions))
			for _, rows := range m.partitions {
				numRows += len(rows)
				info = append(info, map[string]int{"rowCount": len(rows)})
			}
			body["resultSetMetadata"] = map[string]interface{}{
				"numRows":       numRows,
				"rowType":       credentialRowTypes(),
				"partitionInfo": info,
			}
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
}

func newCredentialsClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()
	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)
	return client
}

func TestUserCredentials_ParsesRowsAndNulls(t *testing.T) {
	mock := &credentialsMock{partitions: [][][]*string{{
		{str("ALICE"), str("KEYPAIR"), str("RSA_PUBLIC_KEY"), str("SHA256:abc="), str("1790726400.000000000")},
		{str("ALICE"), str("PAT"), str("TOKEN_1"), nil, nil},
		{str("BOB"), str("KEYPAIR"), str("RSA_PUBLIC_KEY"), str("def="), str("1790726400.000000000")},
	}}}
	server := mock.serve(t)
	defer server.Close()

	credentials, available, err := newCredentialsClient(t, server).UserCredentials(context.Background(), nil, "ALICE")
	require.NoError(t, err)
	require.True(t, available)
	require.Len(t, credentials, 2)

	assert.Equal(t, CredentialTypeKeyPair, credentials[0].Type)
	assert.Equal(t, "SHA256:abc=", credentials[0].PublicKeyFp)
	require.NotNil(t, credentials[0].LastUsedOn)
	assert.True(t, time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC).Equal(*credentials[0].LastUsedOn))

	assert.Equal(t, CredentialTypePAT, credentials[1].Type)
	assert.Equal(t, "TOKEN_1", credentials[1].Name)
	assert.Empty(t, credentials[1].PublicKeyFp)
	assert.Nil(t, credentials[1].LastUsedOn)
}

func TestUserCredentials_FollowUpFetch(t *testing.T) {
	mock := &credentialsMock{async: true, partitions: [][][]*string{{
		{str("ALICE"), str("PAT"), str("TOKEN_1"), nil, str("1790726400")},
	}}}
	server := mock.serve(t)
	defer server.Close()

	credentials, available, err := newCredentialsClient(t, server).UserCredentials(context.Background(), nil, "ALICE")
	require.NoError(t, err)
	require.True(t, available)
	require.Len(t, credentials, 1)
	assert.NotNil(t, credentials[0].LastUsedOn)
}

func TestUserCredentials_GroupsUserAcrossPartitions(t *testing.T) {
	mock := &credentialsMock{partitions: [][][]*string{
		{
			{str("ALICE"), str("PAT"), str("TOKEN_1"), nil, nil},
			{str("BOB"), str("PAT"), str("TOKEN_1"), nil, nil},
		},
		{
			{str("BOB"), str("PAT"), str("TOKEN_2"), nil, nil},
			{str("CAROL"), str("PAT"), str("TOKEN_1"), nil, nil},
		},
	}}
	server := mock.serve(t)
	defer server.Close()

	client := newCredentialsClient(t, server)
	ss := newFakeSessionStore()
	ctx := context.Background()

	bob, available, err := client.UserCredentials(ctx, ss, "BOB")
	require.NoError(t, err)
	require.True(t, available)
	require.Len(t, bob, 2)
	assert.Equal(t, "TOKEN_1", bob[0].Name)
	assert.Equal(t, "TOKEN_2", bob[1].Name)

	carol, _, err := client.UserCredentials(ctx, ss, "CAROL")
	require.NoError(t, err)
	assert.Len(t, carol, 1)
}

func TestUserCredentials_LoadsOncePerSession(t *testing.T) {
	mock := &credentialsMock{partitions: [][][]*string{{
		{str("ALICE"), str("PAT"), str("TOKEN_1"), nil, nil},
	}}}
	server := mock.serve(t)
	defer server.Close()

	client := newCredentialsClient(t, server)
	ss := newFakeSessionStore()
	for _, user := range []string{"ALICE", "BOB", "CAROL", "ALICE"} {
		_, available, err := client.UserCredentials(context.Background(), ss, user)
		require.NoError(t, err)
		require.True(t, available)
	}
	assert.Equal(t, int32(1), mock.posts.Load())

	none, _, err := client.UserCredentials(context.Background(), ss, "BOB")
	require.NoError(t, err)
	assert.Empty(t, none)
}

func TestUserCredentials_UnavailableIsCached(t *testing.T) {
	tests := []struct {
		code      string
		wantError error
	}{
		{code: "003001", wantError: ErrInsufficientPrivileges},
		{code: "002003", wantError: ErrInsufficientPrivileges},
		{code: "000606", wantError: errNoWarehouse},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			mock := &credentialsMock{status: http.StatusUnprocessableEntity, code: tt.code}
			server := mock.serve(t)
			defer server.Close()

			client := newCredentialsClient(t, server)
			err := client.listCredentials(context.Background(), func(map[string][]Credential) error { return nil })
			require.ErrorIs(t, err, tt.wantError)

			var warnings atomic.Int32
			core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(io.Discard), zap.WarnLevel)
			core = zapcore.RegisterHooks(core, func(e zapcore.Entry) error {
				if strings.Contains(e.Message, "last-used dates unavailable") {
					warnings.Add(1)
				}
				return nil
			})
			ctx := ctxzap.ToContext(context.Background(), zap.New(core))
			for range 2 {
				ss := newFakeSessionStore()
				for range 3 {
					credentials, available, err := client.UserCredentials(ctx, ss, "ALICE")
					require.NoError(t, err)
					assert.False(t, available)
					assert.Nil(t, credentials)
				}
			}
			assert.Equal(t, int32(3), mock.posts.Load())
			assert.Equal(t, int32(2), warnings.Load(), "one warning per sync session")
		})
	}
}

func TestUserCredentials_OtherErrorsAreReturnedAndNotCached(t *testing.T) {
	tests := []struct {
		name   string
		status int
		code   string
	}{
		{name: "server error", status: http.StatusInternalServerError},
		{name: "compilation error", status: http.StatusUnprocessableEntity, code: "001003"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &credentialsMock{status: tt.status, code: tt.code}
			server := mock.serve(t)
			defer server.Close()

			client := newCredentialsClient(t, server)
			ss := newFakeSessionStore()
			for range 2 {
				_, _, err := client.UserCredentials(context.Background(), ss, "ALICE")
				require.Error(t, err)
				assert.False(t, IsInsufficientPrivileges(err))
			}
			assert.Equal(t, int32(2), mock.posts.Load())
		})
	}
}

func TestGetUserRsa_ParsesFingerprints(t *testing.T) {
	response := &RsaGetUserRawResponse{}
	response.ResultSetMetadata.RowTypes = []RowType{
		{Name: "property", Type: "text"}, {Name: "value", Type: "text"},
		{Name: "default", Type: "text"}, {Name: "description", Type: "text"},
	}
	response.Data = [][]string{
		{"NAME", "ALICE", "null", ""},
		{"RSA_PUBLIC_KEY_FP", "SHA256:abc=", "null", ""},
		{"RSA_PUBLIC_KEY_2_FP", "null", "null", ""},
		{"RSA_PUBLIC_KEY_LAST_SET_TIME", "2025-04-09 10:00:00.000", "null", ""},
	}

	rsa, err := response.GetUserRsa(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "SHA256:abc=", rsa.RsaPublicKeyFp)
	assert.Empty(t, rsa.RsaPublicKeyFp2)
	require.NotNil(t, rsa.RsaPublicKeyLastSetTime)
	assert.Nil(t, rsa.RsaPublicKeyLastSetTime2)
}
