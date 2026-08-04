package snowflake

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStatementsApiRequestBodyRole(t *testing.T) {
	withRole, err := json.Marshal(StatementsApiRequestBody{Statement: "SHOW ORGANIZATION ACCOUNTS;", Role: GlobalOrgAdminRole})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(withRole), `"role":"GLOBALORGADMIN"`) {
		t.Errorf("expected role in request body, got %s", withRole)
	}

	noRole, err := json.Marshal(StatementsApiRequestBody{Statement: "SHOW USERS;"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(noRole), "role") {
		t.Errorf("expected role omitted when empty, got %s", noRole)
	}
}

// A 401 must surface Snowflake's response-body reason while keeping codes.Unauthenticated,
// with no bare "401 Unauthorized" line duplicated alongside the detailed reason.
func TestListUsers_SurfacesAuthFailureReason(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":"390144","message":"JWT token is invalid. [abc-123]"}`))
	}))
	defer srv.Close()

	client, err := New(srv.URL, JWTConfig{}, srv.Client())
	require.NoError(t, err)

	_, err = client.ListUsers(context.Background(), "", 1)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Contains(t, err.Error(), "JWT token is invalid")
	assert.Equal(t, 1, strings.Count(err.Error(), "rpc error"), "expected a single error, not joined with the bare status: %s", err.Error())
}

// A non-JSON 401 body (e.g. an HTML page from a proxy in front of Snowflake) must not panic
// and must still map to codes.Unauthenticated.
func TestListUsers_NonJSONAuthFailureBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`<html><body>401 Unauthorized</body></html>`))
	}))
	defer srv.Close()

	client, err := New(srv.URL, JWTConfig{}, srv.Client())
	require.NoError(t, err)

	_, err = client.ListUsers(context.Background(), "", 1)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

// A malformed (truncated) JSON 401 body must not panic and must still map to codes.Unauthenticated.
func TestListUsers_MalformedJSONAuthFailureBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":"390144","message":`))
	}))
	defer srv.Close()

	client, err := New(srv.URL, JWTConfig{}, srv.Client())
	require.NoError(t, err)

	_, err = client.ListUsers(context.Background(), "", 1)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

// A 429 must keep the RateLimitDescription gRPC detail so retry/backoff can honor
// the server's reset time instead of falling back to linear backoff.
func TestListUsers_PreservesRateLimitDetailsOn429(t *testing.T) {
	resetAt := time.Now().Add(30 * time.Second).Unix()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Ratelimit-Limit", "100")
		w.Header().Set("X-Ratelimit-Remaining", "0")
		w.Header().Set("X-Ratelimit-Reset", strconv.FormatInt(resetAt, 10))
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"code":"390100","message":"rate limit exceeded"}`))
	}))
	defer srv.Close()

	client, err := New(srv.URL, JWTConfig{}, srv.Client())
	require.NoError(t, err)

	_, err = client.ListUsers(context.Background(), "", 1)
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))

	st, ok := status.FromError(err)
	require.True(t, ok)
	var rateLimitDetail *v2.RateLimitDescription
	for _, d := range st.Details() {
		if rl, ok := d.(*v2.RateLimitDescription); ok {
			rateLimitDetail = rl
		}
	}
	require.NotNil(t, rateLimitDetail, "expected RateLimitDescription detail to survive dedupeAPIError, got: %v", st.Details())
	assert.EqualValues(t, 100, rateLimitDetail.GetLimit())
	assert.EqualValues(t, 0, rateLimitDetail.GetRemaining())
}

// A non-JSON 401 body on the REST user-management API must still map to
// codes.Unauthenticated, mirroring TestListUsers_NonJSONAuthFailureBody for the
// SQL API. doRequest's DoOption order previously put WithJSONResponse after
// WithErrorResponse, so dedupeAPIError picked the code-less unmarshal error instead.
func TestCreateUserREST_NonJSONAuthFailureBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`<html><body>401 Unauthorized</body></html>`))
	}))
	defer srv.Close()

	client, err := New(srv.URL, JWTConfig{}, srv.Client())
	require.NoError(t, err)

	_, _, err = client.CreateUserREST(context.Background(), &CreateUserRequest{Name: "test"})
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}
