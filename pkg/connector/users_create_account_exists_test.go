package connector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// existingUser is the Snowflake user the mock already holds.
type existingUser struct {
	name, login, email string
}

// mockSnowflake serves POST /api/v2/users with createStatus (409 = the user exists) and answers
// DESCRIBE USER with u, or with describeStatus when that is not 200. It counts DESCRIBE calls.
func mockSnowflake(t *testing.T, createStatus, describeStatus int, u existingUser) (*snowflake.Client, *atomic.Int32) {
	t.Helper()
	describes := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && r.URL.Path == "/api/v2/users" {
			w.WriteHeader(createStatus)
			_, _ = w.Write([]byte(`{"code":"002002","message":"User already exists."}`))
			return
		}
		describes.Add(1)
		if describeStatus != http.StatusOK {
			w.WriteHeader(describeStatus)
			_, _ = w.Write([]byte(`{"code":"003001","message":"not authorized"}`))
			return
		}
		_, _ = fmt.Fprintf(w, `{"code":"090001","statementHandle":"h1",
"resultSetMetaData":{"numRows":12,"rowType":[{"name":"property","type":"text"},{"name":"value","type":"text"}]},
"data":[["NAME",%q],["LOGIN_NAME",%q],["DISPLAY_NAME",%q],["FIRST_NAME",""],["LAST_NAME",""],
["EMAIL",%q],["DISABLED","false"],["SNOWFLAKE_LOCK","false"],["DEFAULT_ROLE",""],["TYPE","PERSON"],
["HAS_MFA","false"],["COMMENT",""]]}`, u.name, u.login, u.name, u.email)
	}))
	t.Cleanup(srv.Close)

	client, err := snowflake.New(srv.URL, snowflake.JWTConfig{}, srv.Client())
	require.NoError(t, err)
	return client, describes
}

func noPasswordOpts() *v2.LocalCredentialOptions {
	return &v2.LocalCredentialOptions{
		Options: &v2.LocalCredentialOptions_NoPassword_{NoPassword: &v2.LocalCredentialOptions_NoPassword{}},
	}
}

func randomPasswordOpts() *v2.LocalCredentialOptions {
	return &v2.LocalCredentialOptions{
		Options: &v2.LocalCredentialOptions_RandomPassword_{RandomPassword: &v2.LocalCredentialOptions_RandomPassword{Length: 16}},
	}
}

func createAccountFor(t *testing.T, client *snowflake.Client, profile map[string]any, opts *v2.LocalCredentialOptions) (
	any, int, error,
) {
	t.Helper()
	p, err := structpb.NewStruct(profile)
	require.NoError(t, err)
	b := newUserBuilder(client, secretOptions{})
	result, plaintext, _, err := b.CreateAccount(context.Background(), &v2.AccountInfo{Profile: p}, opts)
	return result, len(plaintext), err
}

func TestCreateAccountUserAlreadyExists(t *testing.T) {
	alice := existingUser{name: "alice", login: "alice", email: "alice@example.com"}

	tests := []struct {
		name     string
		profile  map[string]any
		opts     *v2.LocalCredentialOptions
		existing existingUser
	}{
		{
			name:     "name only",
			profile:  map[string]any{"name": "alice"},
			opts:     noPasswordOpts(),
			existing: alice,
		},
		{
			name:     "random password requested but none set on the existing account",
			profile:  map[string]any{"name": "alice"},
			opts:     randomPasswordOpts(),
			existing: alice,
		},
		{
			name:     "matching email, case-insensitive",
			profile:  map[string]any{"name": "alice", "email": "Alice@Example.COM"},
			opts:     noPasswordOpts(),
			existing: alice,
		},
		{
			name:     "matching login",
			profile:  map[string]any{"name": "alice", "login": "ALICE"},
			opts:     noPasswordOpts(),
			existing: alice,
		},
		{
			name:     "request email but existing user has none cannot be contradicted",
			profile:  map[string]any{"name": "alice", "email": "alice@example.com"},
			opts:     noPasswordOpts(),
			existing: existingUser{name: "alice", login: "alice"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := mockSnowflake(t, http.StatusConflict, http.StatusOK, tc.existing)

			result, nPlaintext, err := createAccountFor(t, client, tc.profile, tc.opts)

			require.NoError(t, err, "a 409 for the requester's own user must not fail the task")
			existing, ok := result.(*v2.CreateAccountResponse_AlreadyExistsResult)
			require.True(t, ok, "expected AlreadyExistsResult, got %T", result)
			require.Equal(t, "alice", existing.GetResource().GetDisplayName())
			require.Zero(t, nPlaintext, "no credentials were created for a pre-existing account")
		})
	}
}

func TestCreateAccountUserAlreadyExistsNotLinked(t *testing.T) {
	alice := existingUser{name: "alice", login: "alice", email: "alice@example.com"}

	tests := []struct {
		name           string
		profile        map[string]any
		createStatus   int
		describeStatus int
		wantCode       codes.Code
		wantDescribes  int32
	}{
		{
			name:           "existing user has a different email",
			profile:        map[string]any{"name": "alice", "email": "someone.else@example.com"},
			createStatus:   http.StatusConflict,
			describeStatus: http.StatusOK,
			wantCode:       codes.AlreadyExists,
			wantDescribes:  1,
		},
		{
			name:           "existing user has a different login",
			profile:        map[string]any{"name": "alice", "login": "someone.else"},
			createStatus:   http.StatusConflict,
			describeStatus: http.StatusOK,
			wantCode:       codes.AlreadyExists,
			wantDescribes:  1,
		},
		{
			name:           "existing user cannot be fetched",
			profile:        map[string]any{"name": "alice"},
			createStatus:   http.StatusConflict,
			describeStatus: http.StatusForbidden,
			wantCode:       codes.AlreadyExists,
			wantDescribes:  1,
		},
		{
			name:           "other create failures are unchanged and never look the user up",
			profile:        map[string]any{"name": "alice"},
			createStatus:   http.StatusInternalServerError,
			describeStatus: http.StatusOK,
			wantCode:       codes.Unavailable,
			wantDescribes:  0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, describes := mockSnowflake(t, tc.createStatus, tc.describeStatus, alice)

			result, _, err := createAccountFor(t, client, tc.profile, noPasswordOpts())

			require.Error(t, err)
			require.Nil(t, result, "must not return a result that C1 would bind to the requester")
			require.Equal(t, tc.wantCode, status.Code(err), "the original create error must be preserved: %v", err)
			require.Equal(t, tc.wantDescribes, describes.Load())
		})
	}
}
