package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

// newSetUserDisabledMockServer answers every Statements API call inline, capturing the SQL
// statement text so tests can assert on the ALTER USER ... SET DISABLED = ...; the handler emits.
func newSetUserDisabledMockServer(t *testing.T, capturedSQL *string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var body struct {
			Statement string `json:"statement"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		*capturedSQL = body.Statement

		enc := json.NewEncoder(w)
		_ = enc.Encode(map[string]any{
			"statementHandle": "handle",
			"resultSetMetadata": map[string]any{
				"numRows": 0,
			},
			"data": [][]string{},
		})
	}))
}

func newTestConnector(t *testing.T, serverURL string) *Connector {
	t.Helper()
	client, err := snowflake.New(serverURL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)
	return &Connector{Client: client}
}

func TestDisableUserHandler(t *testing.T) {
	var capturedSQL string
	server := newSetUserDisabledMockServer(t, &capturedSQL)
	defer server.Close()

	c := newTestConnector(t, server.URL)

	args, err := structpb.NewStruct(map[string]any{"user_id": "testuser"})
	require.NoError(t, err)

	result, _, err := c.disableUserHandler(context.Background(), args)
	require.NoError(t, err)
	assert.Equal(t, `ALTER USER "testuser" SET DISABLED = true;`, capturedSQL)
	assert.True(t, result.Fields["success"].GetBoolValue())
}

func TestEnableUserHandler(t *testing.T) {
	var capturedSQL string
	server := newSetUserDisabledMockServer(t, &capturedSQL)
	defer server.Close()

	c := newTestConnector(t, server.URL)

	args, err := structpb.NewStruct(map[string]any{"user_id": "testuser"})
	require.NoError(t, err)

	result, _, err := c.enableUserHandler(context.Background(), args)
	require.NoError(t, err)
	assert.Equal(t, `ALTER USER "testuser" SET DISABLED = false;`, capturedSQL)
	assert.True(t, result.Fields["success"].GetBoolValue())
}

// TestDisableEnableUserHandler_EmptyUserID verifies that a present-but-blank user_id is
// rejected as InvalidArgument before any request reaches Snowflake, rather than surfacing
// as an opaque error from an ALTER USER "" statement.
func TestDisableEnableUserHandler_EmptyUserID(t *testing.T) {
	var capturedSQL string
	server := newSetUserDisabledMockServer(t, &capturedSQL)
	defer server.Close()

	c := newTestConnector(t, server.URL)

	tests := []struct {
		name    string
		handler func(context.Context, *structpb.Struct) (*structpb.Struct, any, error)
	}{
		{name: "disable_user", handler: func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, any, error) {
			return c.disableUserHandler(ctx, args)
		}},
		{name: "enable_user", handler: func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, any, error) {
			return c.enableUserHandler(ctx, args)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capturedSQL = ""
			args, err := structpb.NewStruct(map[string]any{"user_id": "   "})
			require.NoError(t, err)

			_, _, err = tt.handler(context.Background(), args)
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Empty(t, capturedSQL, "no request should reach Snowflake for a blank user_id")
		})
	}
}

// TestDisableEnableUserHandler_TrimsUserID verifies that a user_id with leading/trailing
// whitespace is trimmed before being sent to Snowflake, not just when checking for
// blankness - a padded value like "  bob  " must reach the API as "bob", since a quoted
// Snowflake identifier is an exact match and would otherwise fail with an opaque
// "user does not exist" error.
func TestDisableEnableUserHandler_TrimsUserID(t *testing.T) {
	var capturedSQL string
	server := newSetUserDisabledMockServer(t, &capturedSQL)
	defer server.Close()

	c := newTestConnector(t, server.URL)

	tests := []struct {
		name    string
		handler func(context.Context, *structpb.Struct) (*structpb.Struct, any, error)
		want    string
	}{
		{
			name: "disable_user",
			handler: func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, any, error) {
				return c.disableUserHandler(ctx, args)
			},
			want: `ALTER USER "bob" SET DISABLED = true;`,
		},
		{
			name: "enable_user",
			handler: func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, any, error) {
				return c.enableUserHandler(ctx, args)
			},
			want: `ALTER USER "bob" SET DISABLED = false;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capturedSQL = ""
			args, err := structpb.NewStruct(map[string]any{"user_id": "  bob  "})
			require.NoError(t, err)

			_, _, err = tt.handler(context.Background(), args)
			require.NoError(t, err)
			assert.Equal(t, tt.want, capturedSQL)
		})
	}
}
