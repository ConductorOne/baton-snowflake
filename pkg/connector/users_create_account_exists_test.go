package connector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// existingUser is the Snowflake user the mock already holds. An empty userType means PERSON.
type existingUser struct {
	name, login, email, userType string
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
		// Like the real DESCRIBE USER, unset properties are rendered as the literal "null", not "".
		_, _ = fmt.Fprintf(w, `{"code":"090001","statementHandle":"h1",
"resultSetMetaData":{"numRows":12,"rowType":[{"name":"property","type":"text"},{"name":"value","type":"text"}]},
"data":[["NAME",%q],["LOGIN_NAME",%q],["DISPLAY_NAME",%q],["FIRST_NAME","null"],["LAST_NAME","null"],
["EMAIL",%q],["DISABLED","false"],["SNOWFLAKE_LOCK","false"],["DEFAULT_ROLE","null"],["TYPE",%q],
["HAS_MFA","false"],["COMMENT","null"]]}`, u.name, u.login, u.name, describeValue(u.email), userType(u))
	}))
	t.Cleanup(srv.Close)

	client, err := snowflake.New(srv.URL, snowflake.JWTConfig{}, srv.Client())
	require.NoError(t, err)
	return client, describes
}

func userType(u existingUser) string {
	if u.userType == "" {
		return "PERSON"
	}
	return u.userType
}

// describeValue renders a property the way DESCRIBE USER does: an unset one is the literal "null".
func describeValue(v string) string {
	if v == "" {
		return "null"
	}
	return v
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
			requireNoDescribeNulls(t, existing.GetResource(), tc.existing.email)
		})
	}
}

// requireNoDescribeNulls asserts that DESCRIBE USER's literal "null" did not reach the resource: unset
// properties must read as empty, the way they do for the same user from sync.
func requireNoDescribeNulls(t *testing.T, resource *v2.Resource, wantEmail string) {
	t.Helper()
	ut, err := rs.GetUserTrait(resource)
	require.NoError(t, err)
	profile := rs.GetProfile(resource).AsMap()
	for _, key := range []string{"email", "first_name", "last_name", profileKeyComment} {
		require.NotEqual(t, "null", profile[key], "profile %s must not carry DESCRIBE USER's literal null", key)
	}
	require.Equal(t, wantEmail, profile["email"])
	if wantEmail == "" {
		require.Empty(t, ut.GetEmails(), "a user with no EMAIL must not get a primary email")
	} else {
		require.Equal(t, wantEmail, ut.GetEmails()[0].GetAddress())
	}
}

func TestCreateAccountNewUserHasNoDescribeNulls(t *testing.T) {
	client, describes := mockSnowflake(t, http.StatusOK, http.StatusOK, existingUser{name: "bob", login: "BOB"})

	result, _, err := createAccountFor(t, client, map[string]any{"name": "bob"}, noPasswordOpts())

	require.NoError(t, err)
	created, ok := result.(*v2.CreateAccountResponse_SuccessResult)
	require.True(t, ok, "expected SuccessResult, got %T", result)
	require.Equal(t, int32(1), describes.Load(), "the created user is read back with DESCRIBE USER")
	requireNoDescribeNulls(t, created.GetResource(), "")
}

func TestCreateAccountUserAlreadyExistsNotLinked(t *testing.T) {
	alice := existingUser{name: "alice", login: "alice", email: "alice@example.com"}

	tests := []struct {
		name           string
		profile        map[string]any
		createStatus   int
		describeStatus int
		existing       *existingUser
		wantCode       codes.Code
		wantMsg        string
		wantDescribes  int32
	}{
		{
			name:           "existing user has a different email",
			profile:        map[string]any{"name": "alice", "email": "someone.else@example.com"},
			createStatus:   http.StatusConflict,
			describeStatus: http.StatusOK,
			wantCode:       codes.AlreadyExists,
			wantMsg:        "its email differs from the request",
			wantDescribes:  1,
		},
		{
			name:           "existing user has a different login",
			profile:        map[string]any{"name": "alice", "login": "someone.else"},
			createStatus:   http.StatusConflict,
			describeStatus: http.StatusOK,
			wantCode:       codes.AlreadyExists,
			wantMsg:        "its login differs from the request",
			wantDescribes:  1,
		},
		{
			name:           "existing user cannot be fetched",
			profile:        map[string]any{"name": "alice"},
			createStatus:   http.StatusConflict,
			describeStatus: http.StatusForbidden,
			wantCode:       codes.PermissionDenied,
			wantMsg:        "could not be read",
			wantDescribes:  1,
		},
		{
			name:           "existing user is a SERVICE user",
			profile:        map[string]any{"name": "svc"},
			createStatus:   http.StatusConflict,
			describeStatus: http.StatusOK,
			existing:       &existingUser{name: "svc", login: "SVC", userType: "SERVICE"},
			wantCode:       codes.AlreadyExists,
			wantMsg:        "is a service user",
			wantDescribes:  1,
		},
		{
			name:           "existing user is a SERVICE_AGENT user",
			profile:        map[string]any{"name": "svc"},
			createStatus:   http.StatusConflict,
			describeStatus: http.StatusOK,
			existing:       &existingUser{name: "svc", login: "SVC", userType: "SERVICE_AGENT"},
			wantCode:       codes.AlreadyExists,
			wantMsg:        "is a service user",
			wantDescribes:  1,
		},
		{
			name:           "existing user is a LEGACY_SERVICE user",
			profile:        map[string]any{"name": "svc"},
			createStatus:   http.StatusConflict,
			describeStatus: http.StatusOK,
			existing:       &existingUser{name: "svc", login: "SVC", userType: "LEGACY_SERVICE"},
			wantCode:       codes.AlreadyExists,
			wantMsg:        "is a service user",
			wantDescribes:  1,
		},
		{
			name:           "existing service user is refused even when the request email matches",
			profile:        map[string]any{"name": "svc", "email": "svc@example.com"},
			createStatus:   http.StatusConflict,
			describeStatus: http.StatusOK,
			existing:       &existingUser{name: "svc", login: "SVC", email: "svc@example.com", userType: "service"},
			wantCode:       codes.AlreadyExists,
			wantMsg:        "is a service user",
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
			existing := alice
			if tc.existing != nil {
				existing = *tc.existing
			}
			client, describes := mockSnowflake(t, tc.createStatus, tc.describeStatus, existing)

			result, _, err := createAccountFor(t, client, tc.profile, noPasswordOpts())

			require.Error(t, err)
			require.Nil(t, result, "must not return a result that C1 would bind to the requester")
			require.Equal(t, tc.wantCode, status.Code(err), "the error code must say why the account was not linked: %v", err)
			require.Contains(t, err.Error(), tc.wantMsg)
			require.NotContains(t, err.Error(), "alice@example.com", "identifier values must not leak into the error")
			require.Equal(t, tc.wantDescribes, describes.Load())
		})
	}
}
