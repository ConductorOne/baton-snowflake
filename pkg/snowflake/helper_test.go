package snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const publicSchema = "PUBLIC"

func TestIsInsufficientPrivileges(t *testing.T) {
	t.Parallel()

	// The message Snowflake puts in a 422 body for error code 003001. Note it names neither
	// the HTTP status nor the number 422 - the discriminating case below depends on that.
	const accessControlMessage = "SQL access control error:\nInsufficient privileges"

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error is not a privilege denial",
			err:  nil,
			want: false,
		},
		{
			name: "sentinel joined by a client method",
			err: uhttp.WrapErrors(codes.PermissionDenied, "baton-snowflake: insufficient privileges for SHOW SCHEMAS IN DATABASE DB",
				ErrInsufficientPrivileges, errors.New("rpc error: code = InvalidArgument desc = 422 Unprocessable Entity")),
			want: true,
		},
		{
			// The case string matching cannot reach: SHOW GRANTS ON TABLE reports the denial
			// through Snowflake's own error text, which never mentions 422. Only the sentinel
			// identifies it, which is why the client joins one instead of formatting a string.
			name: "sentinel matches when no HTTP status text survives in the message",
			err: uhttp.WrapErrors(codes.PermissionDenied,
				fmt.Sprintf("baton-snowflake: insufficient privileges to show grants on table DB.S.T: %s", accessControlMessage),
				ErrInsufficientPrivileges),
			want: true,
		},
		{
			// Same shape, sentinel omitted: proves the case above passes because of errors.Is
			// and not because the predicate is matching on "insufficient privileges" text.
			name: "same message without the sentinel is not a privilege denial",
			err: status.Error(codes.PermissionDenied,
				fmt.Sprintf("baton-snowflake: insufficient privileges to show grants on table DB.S.T: %s", accessControlMessage)),
			want: false,
		},
		{
			// The bug this predicate must not reintroduce: Snowflake answers 422 for SQL
			// compilation errors too, which mean the connector sent a broken statement. Those
			// carry no sentinel, so they stay fatal instead of silently skipping real data.
			name: "a 422 that is not an access-control denial must stay fatal",
			err:  status.Error(codes.InvalidArgument, "422 Unprocessable Entity"),
			want: false,
		},
		{
			name: "unrelated failure",
			err:  errors.New("baton-snowflake: connection reset by peer"),
			want: false,
		},
		{
			name: "a genuine authentication failure must stay fatal",
			err:  status.Error(codes.Unauthenticated, "401 Unauthorized"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, IsInsufficientPrivileges(tt.err))
		})
	}
}

// TestIsUnprocessableEntityError pins the weaker status-only predicate used by the call sites whose
// client methods return the raw uhttp error without classifying it.
func TestIsUnprocessableEntityError(t *testing.T) {
	t.Parallel()

	assert.True(t, IsUnprocessableEntityError(status.Error(codes.InvalidArgument, "422 Unprocessable Entity")))
	assert.False(t, IsUnprocessableEntityError(nil))
	assert.False(t, IsUnprocessableEntityError(status.Error(codes.Unauthenticated, "401 Unauthorized")),
		"matching the whole status line keeps an unrelated message containing those digits from counting")
	assert.False(t, IsUnprocessableEntityError(errors.New("request id 422abc failed")))
}

// TestIsUnprocessableEntity_DelegatesToSentinel pins that the statusCode-carrying overload still
// answers on the raw status code and now also recognises the sentinel, so the six existing call
// sites that pass a statusCode keep working unchanged.
func TestIsUnprocessableEntity_DelegatesToSentinel(t *testing.T) {
	t.Parallel()

	assert.True(t, IsUnprocessableEntity(http.StatusUnprocessableEntity, nil), "raw status code alone is enough")
	assert.True(t, IsUnprocessableEntity(0, uhttp.WrapErrors(codes.PermissionDenied, "denied", ErrInsufficientPrivileges)),
		"sentinel alone is enough when no status code is available")
	assert.False(t, IsUnprocessableEntity(http.StatusNotFound, errors.New("not found")))
}

// newStatusServer answers every POST with the given HTTP status and a JSON error body carrying
// code/message so WithErrorResponse can decode it the same way the real SQL API does.
func newStatusServer(t *testing.T, statusCode int, code, message string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code":    code,
			"message": message,
		})
	}))
}

// new422Server answers every POST with 422 and the given Snowflake error body. Snowflake uses 422
// for more than access control, so the error code in the body - not the status - decides whether
// the connector may skip the object.
func new422Server(t *testing.T, code, message string) *httptest.Server {
	t.Helper()
	return newStatusServer(t, http.StatusUnprocessableEntity, code, message)
}

// TestClient_NonSkippableHTTPStatusesStayFatal is the regression gate for CXH-2193: the skip path
// must not absorb auth failures, rate limits, or server errors. Each status is exercised against
// every client method that gained isAccessControlDenial handling.
func TestClient_NonSkippableHTTPStatusesStayFatal(t *testing.T) {
	t.Parallel()

	statuses := []struct {
		name       string
		statusCode int
		code       string
		message    string
		wantCode   codes.Code
	}{
		{
			name:       "401 Unauthorized",
			statusCode: http.StatusUnauthorized,
			code:       "390144",
			message:    "JWT token is invalid",
			wantCode:   codes.Unauthenticated,
		},
		{
			name:       "403 Forbidden",
			statusCode: http.StatusForbidden,
			code:       "390189",
			message:    "Role is not authorized",
			wantCode:   codes.PermissionDenied,
		},
		{
			name:       "429 Too Many Requests",
			statusCode: http.StatusTooManyRequests,
			code:       "390100",
			message:    "rate limit exceeded",
			wantCode:   codes.Unavailable,
		},
		{
			name:       "500 Internal Server Error",
			statusCode: http.StatusInternalServerError,
			code:       "000000",
			message:    "Internal server error",
			wantCode:   codes.Unavailable,
		},
		{
			// Empty body code on a 422: without 003001 the denial helper must withhold the
			// sentinel, otherwise a truncated/unknown QueryFailureStatus would be skipped.
			name:       "422 with empty error code",
			statusCode: http.StatusUnprocessableEntity,
			code:       "",
			message:    "something went wrong",
			wantCode:   codes.InvalidArgument,
		},
	}

	methods := []struct {
		name string
		call func(ctx context.Context, client *Client) error
	}{
		{"ListSchemasInDatabase", func(ctx context.Context, c *Client) error {
			_, err := c.ListSchemasInDatabase(ctx, "DB")
			return err
		}},
		{"ListTablesInSchema", func(ctx context.Context, c *Client) error {
			_, _, err := c.ListTablesInSchema(ctx, "DB", publicSchema, "", 200)
			return err
		}},
		{"ListTableGrants", func(ctx context.Context, c *Client) error {
			_, _, err := c.ListTableGrants(ctx, nil, "DB", publicSchema, "T", "TABLE", "")
			return err
		}},
		{"ListSecrets", func(ctx context.Context, c *Client) error {
			_, err := c.ListSecrets(ctx, "DB")
			return err
		}},
		{"GetTable", func(ctx context.Context, c *Client) error {
			_, err := c.GetTable(ctx, "DB", publicSchema, "T")
			return err
		}},
		{"UserRsa", func(ctx context.Context, c *Client) error {
			_, err := c.UserRsa(ctx, "U")
			return err
		}},
	}

	for _, st := range statuses {
		t.Run(st.name, func(t *testing.T) {
			t.Parallel()

			server := newStatusServer(t, st.statusCode, st.code, st.message)
			t.Cleanup(server.Close)

			client, err := New(server.URL, JWTConfig{}, &http.Client{})
			require.NoError(t, err)

			for _, m := range methods {
				t.Run(m.name, func(t *testing.T) {
					t.Parallel()

					err := m.call(context.Background(), client)

					require.Error(t, err, "non-access-control failure must not be swallowed")
					assert.False(t, IsInsufficientPrivileges(err),
						"sentinel must only attach to access-control denials")
					assert.Equal(t, st.wantCode, status.Code(err),
						"gRPC code must survive so the SDK can classify retry vs terminal")
				})
			}
		})
	}
}

// newInsufficientPrivilegesServer answers every POST with the access-control 422, which is what the
// Statements API returns when the connector role cannot see the object.
func newInsufficientPrivilegesServer(t *testing.T) *httptest.Server {
	t.Helper()
	return new422Server(t, sqlAccessControlErrorCode, "SQL access control error:\nInsufficient privileges")
}

// TestClient_InsufficientPrivilegesErrorContract locks the contract the connector layer relies on:
// every client call that turns a 422 into a privilege denial must return an error that is both
// recognisable with errors.Is AND still carries codes.PermissionDenied. Formatting the cause into
// a status message (what this code used to do) satisfies neither - it flattens the chain, so
// errors.Is stops matching and the caller has nothing but substrings to test against.
func TestClient_InsufficientPrivilegesErrorContract(t *testing.T) {
	t.Parallel()

	server := newInsufficientPrivilegesServer(t)
	// Not defer: the parallel subtests below run after this function returns, so a deferred
	// Close would shut the server down before the first request.
	t.Cleanup(server.Close)

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	tests := []struct {
		name string
		call func(ctx context.Context) error
	}{
		{
			name: "ListSchemasInDatabase",
			call: func(ctx context.Context) error {
				_, err := client.ListSchemasInDatabase(ctx, "DB")
				return err
			},
		},
		{
			name: "ListTablesInSchema",
			call: func(ctx context.Context) error {
				_, _, err := client.ListTablesInSchema(ctx, "DB", publicSchema, "", 200)
				return err
			},
		},
		{
			name: "ListTableGrants",
			call: func(ctx context.Context) error {
				_, _, err := client.ListTableGrants(ctx, nil, "DB", publicSchema, "T", "TABLE", "")
				return err
			},
		},
		{
			name: "UserRsa",
			call: func(ctx context.Context) error {
				_, err := client.UserRsa(ctx, "U")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(context.Background())

			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrInsufficientPrivileges),
				"caller must be able to identify the denial without matching on message text")
			assert.True(t, IsInsufficientPrivileges(err))
			assert.Equal(t, codes.PermissionDenied, status.Code(err),
				"joining the sentinel must not cost the gRPC code the SDK uses to classify the failure")
		})
	}
}

// TestClient_NonAccessControl422StaysFatal is the other half of the contract above. Snowflake
// answers 422 for SQL compilation errors as well, which mean the connector sent a statement the
// server could not run - a connector bug, not data the role may not see. Classifying on the status
// alone would make the sync skip those objects and under-report access silently, so the client must
// withhold the sentinel whenever the body carries any code other than the access-control one.
func TestClient_NonAccessControl422StaysFatal(t *testing.T) {
	t.Parallel()

	server := new422Server(t, "002003", "SQL compilation error:\nObject does not exist")
	t.Cleanup(server.Close)

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	tests := []struct {
		name string
		call func(ctx context.Context) error
	}{
		{
			name: "ListSchemasInDatabase",
			call: func(ctx context.Context) error {
				_, err := client.ListSchemasInDatabase(ctx, "DB")
				return err
			},
		},
		{
			name: "ListTablesInSchema",
			call: func(ctx context.Context) error {
				_, _, err := client.ListTablesInSchema(ctx, "DB", publicSchema, "", 200)
				return err
			},
		},
		{
			name: "ListTableGrants",
			call: func(ctx context.Context) error {
				_, _, err := client.ListTableGrants(ctx, nil, "DB", publicSchema, "T", "TABLE", "")
				return err
			},
		},
		{
			// ListSecrets used to swallow every 422 (including compilation errors) after a second
			// decode of resp.Body. It must now keep non-003001 failures fatal, same as the others.
			name: "ListSecrets",
			call: func(ctx context.Context) error {
				_, err := client.ListSecrets(ctx, "DB")
				return err
			},
		},
		{
			// GetTable used to return (nil, nil) for any 422, which would turn a connector bug
			// (e.g. the old unsupported ESCAPE clause) into a silent "table not found".
			name: "GetTable",
			call: func(ctx context.Context) error {
				_, err := client.GetTable(ctx, "DB", publicSchema, "T")
				return err
			},
		},
		{
			name: "UserRsa",
			call: func(ctx context.Context) error {
				_, err := client.UserRsa(ctx, "U")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(context.Background())

			require.Error(t, err)
			assert.False(t, errors.Is(err, ErrInsufficientPrivileges),
				"a SQL compilation error is not something the role may skip over")
			assert.False(t, IsInsufficientPrivileges(err))
		})
	}
}

// TestListSecrets_AccessControlDenialReturnsEmpty pins that SHOW SECRETS on a database the role
// cannot see degrades to an empty result, matching the connector's "nothing visible" contract.
func TestListSecrets_AccessControlDenialReturnsEmpty(t *testing.T) {
	t.Parallel()

	server := newInsufficientPrivilegesServer(t)
	t.Cleanup(server.Close)

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	secrets, err := client.ListSecrets(context.Background(), "DB")
	require.NoError(t, err)
	assert.Empty(t, secrets)
}

// TestGetTable_AccessControlDenialReturnsNil pins that an undescribable table is treated as
// absent for the owner-fallback path, rather than failing the grants sync.
func TestGetTable_AccessControlDenialReturnsNil(t *testing.T) {
	t.Parallel()

	server := newInsufficientPrivilegesServer(t)
	t.Cleanup(server.Close)

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	table, err := client.GetTable(context.Background(), "DB", publicSchema, "T")
	require.NoError(t, err)
	assert.Nil(t, table)
}
