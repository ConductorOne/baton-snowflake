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

// The issuance statement is built from optional clauses. These pin each shape:
// clauses cannot simply be appended after the terminator, and string values must
// be literals (single quotes), not identifiers.
func TestCreateProgrammaticAccessTokenStatementShape(t *testing.T) {
	const head = `ALTER USER "svc" ADD PROGRAMMATIC ACCESS TOKEN "c1-request-1"`
	for _, tc := range []struct {
		name string
		opts ProgrammaticAccessTokenOptions
		want string
	}{
		{"days only", ProgrammaticAccessTokenOptions{DaysToExpiry: 7}, head + ` DAYS_TO_EXPIRY = 7;`},
		{"role restriction", ProgrammaticAccessTokenOptions{DaysToExpiry: 7, RoleRestriction: "svc_role"},
			head + ` ROLE_RESTRICTION = 'svc_role' DAYS_TO_EXPIRY = 7;`},
		{"bypass minutes", ProgrammaticAccessTokenOptions{DaysToExpiry: 7, MinsToBypassNetworkPolicy: 30},
			head + ` DAYS_TO_EXPIRY = 7 MINS_TO_BYPASS_NETWORK_POLICY_REQUIREMENT = 30;`},
		{"comment", ProgrammaticAccessTokenOptions{DaysToExpiry: 1, Comment: "ci token"},
			head + ` DAYS_TO_EXPIRY = 1 COMMENT = 'ci token';`},
		{"everything", ProgrammaticAccessTokenOptions{DaysToExpiry: 365, RoleRestriction: "r", MinsToBypassNetworkPolicy: 1440, Comment: "c"},
			head + ` ROLE_RESTRICTION = 'r' DAYS_TO_EXPIRY = 365 MINS_TO_BYPASS_NETWORK_POLICY_REQUIREMENT = 1440 COMMENT = 'c';`},
		{"quote and backslash escaped", ProgrammaticAccessTokenOptions{DaysToExpiry: 7, RoleRestriction: `a'b`, Comment: `it's a \ trap\`},
			head + ` ROLE_RESTRICTION = 'a''b' DAYS_TO_EXPIRY = 7 COMMENT = 'it''s a \\ trap\\';`},
		{"injection attempt stays inside the literal", ProgrammaticAccessTokenOptions{DaysToExpiry: 7, Comment: `x'; DROP USER y; --`},
			head + ` DAYS_TO_EXPIRY = 7 COMMENT = 'x''; DROP USER y; --';`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &statementRecorder{}
			server := newTokenCreateServer(t, recorder)
			defer server.Close()
			client, err := New(server.URL, JWTConfig{}, server.Client())
			require.NoError(t, err)

			secret, err := client.CreateProgrammaticAccessToken(context.Background(), "svc", "c1-request-1", tc.opts)
			require.NoError(t, err)
			require.Equal(t, "the-secret", secret)
			statements, _ := recorder.snapshot()
			require.Equal(t, []string{tc.want}, statements)
		})
	}
}

func TestQuoteStringLiteral(t *testing.T) {
	require.Equal(t, `''`, quoteStringLiteral(""))
	require.Equal(t, `'a''b'`, quoteStringLiteral("a'b"))
	require.Equal(t, `'a\\b'`, quoteStringLiteral(`a\b`))
	require.Equal(t, `'\\'''`, quoteStringLiteral(`\'`), "a quote after a backslash cannot close the literal")
}

func TestCreateProgrammaticAccessTokenRejectsInvalidOptions(t *testing.T) {
	recorder := &statementRecorder{}
	server := newTokenCreateServer(t, recorder)
	defer server.Close()
	client, err := New(server.URL, JWTConfig{}, server.Client())
	require.NoError(t, err)

	_, err = client.CreateProgrammaticAccessToken(context.Background(), "svc", "c1-request-1", ProgrammaticAccessTokenOptions{})
	require.ErrorContains(t, err, "days to expiry must be at least one")
	_, err = client.CreateProgrammaticAccessToken(context.Background(), "svc", "c1-request-1", ProgrammaticAccessTokenOptions{DaysToExpiry: 1, MinsToBypassNetworkPolicy: -1})
	require.Error(t, err)
	statements, _ := recorder.snapshot()
	require.Empty(t, statements)
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

		_, err = client.CreateProgrammaticAccessToken(context.Background(), "svc", "c1-request-1", ProgrammaticAccessTokenOptions{DaysToExpiry: 7})
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

// TestRoleGrantedToUser_FindsRoleInLaterPartition is the regression guard for a real functional
// bug this closes, not just an undercount: a user with enough grants to split SHOW GRANTS TO USER
// across partitions could have the target role land in partition 1+ rather than 0. Before
// executeStatement drained remaining partitions, that role was invisible to
// RoleGrantedToUser - a granted role reported as not granted, which blocks programmatic access
// token issuance (see pkg/connector/users.go) for a user who is actually entitled to it.
func TestRoleGrantedToUser_FindsRoleInLaterPartition(t *testing.T) {
	const handle = "handle-user-grants"
	columns := []map[string]interface{}{
		{"name": "granted_on", "type": "text"},
		{"name": "name", "type": "text"},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"statementHandle": handle,
				"resultSetMetadata": map[string]interface{}{
					"numRows":       2,
					"rowType":       columns,
					"partitionInfo": []map[string]interface{}{{"rowCount": 1}, {"rowCount": 1}},
				},
				"data": [][]string{{"ROLE", "UNRELATED_ROLE"}},
			})
		case http.MethodGet:
			require.Equal(t, "1", r.URL.Query().Get("partition"))
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"data": [][]string{{"ROLE", "TARGET_ROLE"}},
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
	}))
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, server.Client())
	require.NoError(t, err)

	got, err := client.RoleGrantedToUser(context.Background(), "svc", "TARGET_ROLE")
	require.NoError(t, err)
	require.True(t, got, "TARGET_ROLE is granted, just reported in partition 1 - it must still be found")
}
