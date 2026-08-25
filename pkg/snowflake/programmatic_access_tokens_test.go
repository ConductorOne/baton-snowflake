package snowflake

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
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
			recorder := &statementRecorder{}
			server := newTokenCreateServer(t, recorder)
			defer server.Close()
			client, err := New(server.URL, JWTConfig{}, server.Client())
			require.NoError(t, err)

			secret, err := client.CreateProgrammaticAccessToken(
				context.Background(), "svc", "c1-request-1", tc.roleRestriction, 7,
			)
			require.NoError(t, err)
			require.Equal(t, "the-secret", secret)
			statements, _ := recorder.snapshot()
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

// statementRecorder captures what each request carried. The handler runs on the
// server's goroutine while the test reads from its own, so the mutex is load-bearing
// under -race, not decoration.
type statementRecorder struct {
	mu         sync.Mutex
	statements []string
	roles      []string
}

func (r *statementRecorder) record(statement, role string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statements = append(r.statements, statement)
	r.roles = append(r.roles, role)
}

func (r *statementRecorder) snapshot() ([]string, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.statements...), append([]string(nil), r.roles...)
}

// newTokenCreateServer records each statement it is sent and answers with the column
// shape a live Snowflake ADD PROGRAMMATIC ACCESS TOKEN returns: [token_name, token_secret].
func newTokenCreateServer(t *testing.T, recorder *statementRecorder) *httptest.Server {
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
		recorder.record(request.Statement, request.Role)
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

// A statement that goes async reports its outcome on the follow-up GET rather than
// on the POST. Both legs must classify an access-control denial the same way, or a
// denial that arrives asynchronously is indistinguishable from a real failure and
// aborts the sync instead of skipping the object.
func TestExecuteStatementClassifiesDenialOnEitherLeg(t *testing.T) {
	for _, tc := range []struct {
		name       string
		denyOnPost bool
	}{
		{name: "denied on the POST leg", denyOnPost: true},
		{name: "denied on the follow-up GET leg", denyOnPost: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				deny := tc.denyOnPost == (r.Method == http.MethodPost)
				w.Header().Set("Content-Type", "application/json")
				if deny {
					w.WriteHeader(http.StatusUnprocessableEntity)
					_ = json.NewEncoder(w).Encode(map[string]any{
						"code":    "003001",
						"message": "SQL access control error: Insufficient privileges to operate on user",
					})
					return
				}
				// Not the denying leg: hand back a handle so the client goes async.
				_ = json.NewEncoder(w).Encode(map[string]any{"statementHandle": "handle-1"})
			}))
			defer server.Close()

			client, err := New(server.URL, JWTConfig{}, server.Client())
			require.NoError(t, err)

			_, err = client.ListProgrammaticAccessTokens(context.Background(), "svc")
			require.Error(t, err)
			require.True(t, IsInsufficientPrivileges(err),
				"denial should be classified as skippable, got %v", err)
		})
	}
}

// ALTER USER mutates another user, and the session's default role is not guaranteed to
// hold ALTER USER on other users. SetUserDisabled, CreateUserREST and DeleteUserREST all
// force USERADMIN for the same reason; these statements must too.
func TestTokenMutationsRunAsUserAdmin(t *testing.T) {
	t.Parallel()
	t.Run("create", func(t *testing.T) {
		t.Parallel()
		recorder := &statementRecorder{}
		server := newTokenCreateServer(t, recorder)
		defer server.Close()
		client, err := New(server.URL, JWTConfig{}, server.Client())
		require.NoError(t, err)

		_, err = client.CreateProgrammaticAccessToken(context.Background(), "svc", "c1-request-1", "", 7)
		require.NoError(t, err)
		_, roles := recorder.snapshot()
		require.Equal(t, []string{UserAdminRole}, roles)
	})

	t.Run("remove", func(t *testing.T) {
		t.Parallel()
		recorder := &statementRecorder{}
		server := newTokenCreateServer(t, recorder)
		defer server.Close()
		client, err := New(server.URL, JWTConfig{}, server.Client())
		require.NoError(t, err)

		require.NoError(t, client.RemoveProgrammaticAccessToken(context.Background(), "svc", "c1-request-1"))
		_, roles := recorder.snapshot()
		require.Equal(t, []string{UserAdminRole}, roles)
	})

	t.Run("read-only statements do not force a role", func(t *testing.T) {
		t.Parallel()
		recorder := &statementRecorder{}
		server := newTokenCreateServer(t, recorder)
		defer server.Close()
		client, err := New(server.URL, JWTConfig{}, server.Client())
		require.NoError(t, err)

		_, _ = client.ListProgrammaticAccessTokens(context.Background(), "svc")
		_, roles := recorder.snapshot()
		require.Equal(t, []string{""}, roles, "reads should run as the session's default role")
	})
}

// SHOW GRANTS wraps a mixed-case or spaced identifier in double quotes; DESCRIBE USER
// reports DEFAULT_ROLE bare. Comparing the two raw strings reports a granted role as
// ungranted, which blocks issuance for that user entirely.
func TestRoleGrantedToUserMatchesQuotedIdentifiers(t *testing.T) {
	for _, tc := range []struct {
		name     string
		showName string
		lookFor  string
		want     bool
	}{
		{name: "bare name", showName: "SVC_ROLE", lookFor: "SVC_ROLE", want: true},
		{name: "quoted mixed case", showName: `"Mixed Case Role"`, lookFor: "Mixed Case Role", want: true},
		{name: "case differs", showName: "SVC_ROLE", lookFor: "svc_role", want: true},
		{name: "genuinely absent", showName: "SVC_ROLE", lookFor: "OTHER_ROLE", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"resultSetMetadata": map[string]any{
						"numRows": 1,
						"rowType": []map[string]any{
							{"name": "granted_on", "type": "text"},
							{"name": "name", "type": "text"},
						},
					},
					"data": [][]string{{"ROLE", tc.showName}},
				})
			}))
			defer server.Close()
			client, err := New(server.URL, JWTConfig{}, server.Client())
			require.NoError(t, err)

			got, err := client.RoleGrantedToUser(context.Background(), "svc", tc.lookFor)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
