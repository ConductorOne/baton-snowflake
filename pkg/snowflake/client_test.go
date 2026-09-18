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

// TestNeedsStatementResultFetch pins the decision gate that lets a statement cost one HTTP round
// trip instead of two. Both halves matter: skipping when the result is in hand is the whole point
// of the optimisation, and NOT skipping in every other case is what keeps a response shaped
// differently from the documented one from turning into a silently empty sync.
func TestNeedsStatementResultFetch(t *testing.T) {
	inline := &StatementsApiResponseBase{}
	inline.ResultSetMetadata.RowTypes = []RowType{{Name: "name", Type: "text"}}
	// A zero-row result still describes its columns, which is why the check keys on the column
	// list rather than on len(data).
	emptyButInline := &StatementsApiResponseBase{}
	emptyButInline.ResultSetMetadata.RowTypes = []RowType{{Name: "name", Type: "text"}}
	emptyButInline.ResultSetMetadata.NumRows = 0
	handleOnly := &StatementsApiResponseBase{StatementHandle: "handle-1"}

	client := &Client{}
	for _, tc := range []struct {
		name string
		resp *http.Response
		res  statementResult
		want bool
	}{
		{"200 with result set: skip the GET", &http.Response{StatusCode: http.StatusOK}, inline, false},
		{"200 with zero rows but columns present: skip the GET", &http.Response{StatusCode: http.StatusOK}, emptyButInline, false},
		{"202 accepted: statement went async, fetch the handle", &http.Response{StatusCode: http.StatusAccepted}, inline, true},
		{"200 carrying only a handle: fetch it", &http.Response{StatusCode: http.StatusOK}, handleOnly, true},
		{"200 with no decoded response at all: fetch it", &http.Response{StatusCode: http.StatusOK}, nil, true},
		{"nil response: fetch it", nil, inline, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, client.needsStatementResultFetch(context.Background(), tc.resp, tc.res, "test"))
		})
	}
}

// TestStatementRoundTripCount is the regression guard the decision gate exists for: it counts HTTP
// requests rather than inspecting the parsed result, so a return to unconditional fetching fails
// here even though every response-shape assertion elsewhere would still pass.
func TestStatementRoundTripCount(t *testing.T) {
	// Column definitions, in the order the data rows below supply values for.
	columns := []map[string]interface{}{{"name": columnName, "type": "text"}, {"name": columnDatabaseName, "type": "text"}}

	for _, tc := range []struct {
		name         string
		postInline   bool
		wantRequests int
	}{
		{"result inline on the POST: one request", true, 1},
		{"POST returns only a handle: POST plus GET", false, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				w.Header().Set("Content-Type", "application/json")
				body := map[string]interface{}{"statementHandle": "handle-1"}
				// The POST withholds the result set only in the fallback case; the GET on the
				// handle always carries it, exactly as the live API behaves.
				if tc.postInline || r.Method == http.MethodGet {
					body["resultSetMetadata"] = map[string]interface{}{"numRows": 1, "rowType": columns}
					body["data"] = [][]string{{"SALES", "BATON_TEST_DB"}}
				}
				_ = json.NewEncoder(w).Encode(body)
			}))
			defer server.Close()

			client, err := New(server.URL, JWTConfig{}, server.Client())
			require.NoError(t, err)

			schemas, err := client.ListSchemasInDatabase(context.Background(), "BATON_TEST_DB")
			require.NoError(t, err)
			// Same data either way - the round-trip count is the only difference.
			require.Len(t, schemas, 1)
			assert.Equal(t, "SALES", schemas[0].Name)
			assert.Equal(t, tc.wantRequests, requests)
		})
	}
}
