package snowflake

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// The issuance statement is built from one format string with an optional role
// clause interpolated into it. These pin both shapes: the clause cannot simply be
// appended, because the statement ends with a semicolon and Snowflake rejects
// anything after the terminator with a SQL compilation error.
func TestCreateProgrammaticAccessTokenStatementShape(t *testing.T) {
	for _, tc := range []struct {
		name            string
		roleRestriction string
		want            string
	}{
		{
			name:            "without a role restriction",
			roleRestriction: "",
			want:            `ALTER USER "svc" ADD PROGRAMMATIC ACCESS TOKEN "c1-request-1" DAYS_TO_EXPIRY = 7;`,
		},
		{
			name:            "with a role restriction",
			roleRestriction: "svc_role",
			want:            `ALTER USER "svc" ADD PROGRAMMATIC ACCESS TOKEN "c1-request-1" ROLE_RESTRICTION = "svc_role" DAYS_TO_EXPIRY = 7;`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var statements []string
			server := newTokenCreateServer(t, &statements)
			defer server.Close()
			client, err := New(server.URL, JWTConfig{}, server.Client())
			require.NoError(t, err)

			secret, err := client.CreateProgrammaticAccessToken(
				context.Background(), "svc", "c1-request-1", tc.roleRestriction, 7,
			)
			require.NoError(t, err)
			require.Equal(t, "the-secret", secret)
			require.Equal(t, []string{tc.want}, statements)
		})
	}
}

func TestCreateProgrammaticAccessTokenRejectsNonPositiveExpiry(t *testing.T) {
	client, err := New("https://example.snowflakecomputing.com", JWTConfig{}, http.DefaultClient)
	require.NoError(t, err)

	_, err = client.CreateProgrammaticAccessToken(context.Background(), "svc", "c1-request-1", "", 0)
	require.ErrorContains(t, err, "days to expiry must be at least one")
}

// newTokenCreateServer records each statement it is sent and answers with the column
// shape a live Snowflake ADD PROGRAMMATIC ACCESS TOKEN returns: [token_name, token_secret].
func newTokenCreateServer(t *testing.T, statements *[]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var request StatementsApiRequestBody
		if err := json.Unmarshal(body, &request); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		*statements = append(*statements, request.Statement)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"resultSetMetadata": map[string]any{
				"numRows": 1,
				"rowType": []map[string]any{
					{"name": "token_name", "type": "text"},
					{"name": "token_secret", "type": "text"},
				},
			},
			"data": [][]string{{"c1-request-1", "the-secret"}},
		})
	}))
}
