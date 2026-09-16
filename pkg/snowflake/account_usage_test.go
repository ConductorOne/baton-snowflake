package snowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// accountUsageClient builds a client pinned to the ACCOUNT_USAGE discovery path.
func accountUsageClient(t *testing.T, baseURL string, httpClient *http.Client) *Client {
	t.Helper()
	client, err := New(baseURL, JWTConfig{}, httpClient, WithDiscoveryMode(DiscoveryModeAccountUsage))
	require.NoError(t, err)
	require.True(t, client.usesAccountUsage())
	return client
}

// Every read path has to actually change statement when the mode flips. A dispatch that
// silently kept issuing SHOW would leave the whole least-privilege story non-functional
// while looking configured, so this pins the view each one targets.
func TestAccountUsageDiscoveryReplacesEveryShowStatement(t *testing.T) {
	t.Parallel()

	show, err := New("https://example.snowflakecomputing.com", JWTConfig{}, &http.Client{})
	require.NoError(t, err)
	au, err := New("https://example.snowflakecomputing.com", JWTConfig{}, &http.Client{},
		WithDiscoveryMode(DiscoveryModeAccountUsage))
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		show     func(c *Client) string
		wantShow string
		wantView string
	}{
		{
			name:     "list users",
			show:     func(c *Client) string { return c.listUsersStatement("", 50) },
			wantShow: "SHOW USERS",
			wantView: accountUsageUsersView,
		},
		{
			name:     "list roles",
			show:     func(c *Client) string { return c.listAccountRolesStatement("", 50) },
			wantShow: "SHOW ROLES",
			wantView: accountUsageRolesView,
		},
		{
			name:     "get role",
			show:     func(c *Client) string { return c.getAccountRoleStatement("ANALYST") },
			wantShow: "SHOW ROLES LIKE",
			wantView: accountUsageRolesView,
		},
		{
			name:     "role grantees",
			show:     func(c *Client) string { return c.roleGranteesStatement("ANALYST") },
			wantShow: "SHOW GRANTS OF ROLE",
			wantView: accountUsageGrantsToUsersView,
		},
		{
			name:     "list databases",
			show:     func(c *Client) string { return c.listDatabasesStatement("", 50) },
			wantShow: "SHOW DATABASES",
			wantView: accountUsageDatabasesView,
		},
		{
			name:     "get database",
			show:     func(c *Client) string { return c.getDatabaseStatement("DB") },
			wantShow: "SHOW DATABASES LIKE",
			wantView: accountUsageDatabasesView,
		},
		{
			name:     "list schemas",
			show:     func(c *Client) string { return c.listSchemasStatement("DB") },
			wantShow: "SHOW SCHEMAS IN DATABASE",
			wantView: accountUsageSchemataView,
		},
		{
			name:     "list tables",
			show:     func(c *Client) string { return c.listTablesStatement("DB", publicSchema, "", 50) },
			wantShow: "SHOW TABLES IN SCHEMA",
			wantView: accountUsageTablesView,
		},
		{
			name:     "get table",
			show:     func(c *Client) string { return c.getTableStatement("DB", publicSchema, "T") },
			wantShow: "SHOW TABLES LIKE",
			wantView: accountUsageTablesView,
		},
		{
			name:     "table grants",
			show:     func(c *Client) string { return c.tableGrantsStatement("DB", publicSchema, "T", "TABLE") },
			wantShow: "SHOW GRANTS ON TABLE",
			wantView: accountUsageGrantsToRolesView,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, tc.show(show), tc.wantShow, "default mode must keep using SHOW")
			auStatement := tc.show(au)
			assert.Contains(t, auStatement, tc.wantView)
			assert.NotContains(t, auStatement, "SHOW ")
		})
	}
}

// Every ACCOUNT_USAGE object view retains dropped objects as tombstone rows. Without the
// tombstone filter a dropped user, role, database, schema, or table would keep syncing as a
// live resource forever, which is worse than not supporting the mode at all.
func TestAccountUsageStatementsFilterTombstones(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		statement string
		want      string
	}{
		{"users", accountUsageListUsersStatement("", 50), "DELETED_ON IS NULL"},
		{"get user", accountUsageGetUserStatement("alice"), "DELETED_ON IS NULL"},
		{"roles", accountUsageListRolesStatement("", 50), "DELETED_ON IS NULL"},
		{"get role", accountUsageGetRoleStatement("ANALYST"), "DELETED_ON IS NULL"},
		{"role grantees", accountUsageRoleGranteesStatement("ANALYST"), "DELETED_ON IS NULL"},
		{"table grants", accountUsageTableGrantsStatement("DB", publicSchema, "T", "TABLE"), "DELETED_ON IS NULL"},
		// The object views spell the column DELETED, not DELETED_ON.
		{"databases", accountUsageListDatabasesStatement("", 50), "DELETED IS NULL"},
		{"get database", accountUsageGetDatabaseStatement("DB"), "DELETED IS NULL"},
		{"schemas", accountUsageListSchemasStatement("DB"), "DELETED IS NULL"},
		{"tables", accountUsageListTablesStatement("DB", publicSchema, "", 50), "DELETED IS NULL"},
		{"get table", accountUsageGetTableStatement("DB", publicSchema, "T"), "DELETED IS NULL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, tc.statement, tc.want)
		})
	}
}

// Both halves of the role-grantee union must be filtered, or a revoked role grant keeps
// syncing as a live membership.
func TestAccountUsageRoleGranteesFiltersBothUnionBranches(t *testing.T) {
	t.Parallel()
	statement := accountUsageRoleGranteesStatement("ANALYST")
	assert.Equal(t, 2, strings.Count(statement, "DELETED_ON IS NULL"))
	assert.Contains(t, statement, accountUsageGrantsToUsersView)
	assert.Contains(t, statement, accountUsageGrantsToRolesView)
	// Role-to-role grants are USAGE ON ROLE rows in GRANTS_TO_ROLES; without both
	// predicates the union would pull in every object privilege the role holds.
	assert.Contains(t, statement, "GRANTED_ON = 'ROLE'")
	assert.Contains(t, statement, "PRIVILEGE = 'USAGE'")
}

// An identifier containing a single quote must not be able to terminate the string literal
// it is interpolated into. Every ACCOUNT_USAGE statement interpolates names into literals
// (the views store names as data, not as identifiers), so this is the whole injection
// surface of the new path.
func TestAccountUsageStatementsEscapeStringLiterals(t *testing.T) {
	t.Parallel()
	const hostile = `o'brien`
	const escaped = `o''brien`

	for _, tc := range []struct {
		name      string
		statement string
	}{
		{"users cursor", accountUsageListUsersStatement(hostile, 50)},
		{"get user", accountUsageGetUserStatement(hostile)},
		{"roles cursor", accountUsageListRolesStatement(hostile, 50)},
		{"get role", accountUsageGetRoleStatement(hostile)},
		{"role grantees", accountUsageRoleGranteesStatement(hostile)},
		{"databases cursor", accountUsageListDatabasesStatement(hostile, 50)},
		{"get database", accountUsageGetDatabaseStatement(hostile)},
		{"schemas", accountUsageListSchemasStatement(hostile)},
		{"tables database", accountUsageListTablesStatement(hostile, publicSchema, "", 50)},
		{"tables schema", accountUsageListTablesStatement("DB", hostile, "", 50)},
		{"tables cursor", accountUsageListTablesStatement("DB", publicSchema, hostile, 50)},
		{"get table", accountUsageGetTableStatement("DB", publicSchema, hostile)},
		{"table grants", accountUsageTableGrantsStatement("DB", publicSchema, hostile, "TABLE")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, tc.statement, escaped)
			// The only single quotes left must be the literal delimiters and the doubled
			// pairs; an odd count would mean a literal was left unterminated.
			assert.Zero(t, strings.Count(tc.statement, "'")%2, "unbalanced quotes: %s", tc.statement)
		})
	}
}

// The SHOW path paginates with "LIMIT n FROM '<last name>'", which is keyset pagination on
// the object name. The ACCOUNT_USAGE path has to reproduce that exactly, or a cursor stored
// by one mode silently skips or repeats a page after a mode change.
func TestAccountUsageKeysetPaginationMatchesShowSemantics(t *testing.T) {
	t.Parallel()

	first := accountUsageListUsersStatement("", 50)
	assert.NotContains(t, first, "NAME >")
	assert.Contains(t, first, "ORDER BY NAME")
	assert.Contains(t, first, "LIMIT 50")

	next := accountUsageListUsersStatement("alice", 50)
	assert.Contains(t, next, "AND NAME > 'alice'")
	assert.Contains(t, next, "ORDER BY NAME")

	// ListTablesInSchema treats a non-positive limit as unbounded on the SHOW path, so the
	// ACCOUNT_USAGE form must not silently impose one.
	assert.NotContains(t, accountUsageListTablesStatement("DB", publicSchema, "", 0), "LIMIT")
	assert.Contains(t, accountUsageListTablesStatement("DB", publicSchema, "", 25), "LIMIT 25")
}

// SHOW TABLES lists neither views nor external tables, and the kind it reports feeds back
// into the grants lookup's object kind. The ACCOUNT_USAGE form has to match, or the table
// resource set changes shape purely because of a discovery-mode flag.
func TestAccountUsageTablesMatchShowTablesScope(t *testing.T) {
	t.Parallel()
	statement := accountUsageListTablesStatement("DB", publicSchema, "", 50)
	assert.Contains(t, statement, "TABLE_TYPE NOT IN ('VIEW', 'MATERIALIZED VIEW', 'EXTERNAL TABLE')")
	assert.Contains(t, statement, `'TABLE'::TEXT AS "kind"`)
	assert.Contains(t, accountUsageGetTableStatement("DB", publicSchema, "T"),
		"TABLE_TYPE NOT IN ('VIEW', 'MATERIALIZED VIEW', 'EXTERNAL TABLE')")
}

// The object kind a table resource carries in its profile has to pick the GRANTED_ON value,
// the same way it picks between SHOW GRANTS ON TABLE and ON VIEW. Anything unrecognized
// falls back to TABLE rather than being interpolated into the statement.
func TestNormalizeObjectKind(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{
		"":                       "TABLE",
		"TABLE":                  "TABLE",
		"table":                  "TABLE",
		"TRANSIENT":              "TABLE",
		"VIEW":                   "VIEW",
		"view":                   "VIEW",
		"  View  ":               "VIEW",
		"MATERIALIZED VIEW":      "TABLE",
		"'; DROP TABLE users;--": "TABLE",
	} {
		assert.Equal(t, want, normalizeObjectKind(input), "input %q", input)
	}
	assert.Contains(t, accountUsageTableGrantsStatement("DB", publicSchema, "T", "VIEW"), "GRANTED_ON = 'VIEW'")
	assert.Contains(t, accountUsageTableGrantsStatement("DB", publicSchema, "T", "anything"), "GRANTED_ON = 'TABLE'")
}

// serveAccountUsageRows implements the Statements API for a single-partition SELECT,
// returning the given rowType layout and rows and recording the statement it was sent.
func serveAccountUsageRows(t *testing.T, capturedSQL *string, rowTypes []map[string]interface{}, rows [][]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			var req StatementsApiRequestBody
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			*capturedSQL = req.Statement
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"statementHandle": "handle",
			"resultSetMetadata": map[string]interface{}{
				"numRows": len(rows),
				"rowType": rowTypes,
			},
			"data": rows,
		})
	}))
}

// accountUsageUserRowTypes is the rowType layout an ACCOUNT_USAGE user SELECT produces,
// in the order accountUsageUserColumns selects them. The casts in that column list are what
// make these types text/timestamp_ltz rather than the views' native boolean and timestamp
// types, which ParseRow rejects.
func accountUsageUserRowTypes() []map[string]interface{} {
	return []map[string]interface{}{
		{"name": "name", "type": "text"},
		{"name": "login_name", "type": "text"},
		{"name": "display_name", "type": "text"},
		{"name": "first_name", "type": "text"},
		{"name": "last_name", "type": "text"},
		{"name": "email", "type": "text"},
		{"name": "disabled", "type": "text"},
		{"name": "snowflake_lock", "type": "text"},
		{"name": "default_role", "type": "text"},
		{"name": "has_rsa_public_key", "type": "text"},
		{"name": "has_password", "type": "text"},
		{"name": "last_success_login", "type": "timestamp_ltz"},
		{"name": "type", "type": "text"},
		{"name": "has_mfa", "type": "text"},
		{"name": "comment", "type": "text"},
	}
}

// The point of aliasing the ACCOUNT_USAGE columns to the SHOW column names is that the
// existing row parser consumes the result unchanged. This pins that end to end: a real
// ACCOUNT_USAGE-shaped response has to produce a fully populated User, including the
// booleans and the timestamp, which are the two column types the casts exist for.
func TestListUsers_AccountUsageResponseParsesIntoUser(t *testing.T) {
	var capturedSQL string
	// 1735689600 = 2025-01-01T00:00:00Z, rendered by the SQL API as epoch seconds.
	rows := [][]string{{
		"SVC_ETL", "svc_etl@example.com", "ETL Service", "", "", "svc_etl@example.com",
		"false", "false", "ANALYST", "true", "false", "1735689600.000000000", "SERVICE", "false", "nightly etl",
	}}
	server := serveAccountUsageRows(t, &capturedSQL, accountUsageUserRowTypes(), rows)
	defer server.Close()

	client := accountUsageClient(t, server.URL, server.Client())

	users, err := client.ListUsers(context.Background(), "", 50)
	require.NoError(t, err)
	require.Len(t, users, 1)

	got := users[0]
	assert.Equal(t, "SVC_ETL", got.Username)
	assert.Equal(t, "svc_etl@example.com", got.Login)
	assert.Equal(t, "ETL Service", got.DisplayName)
	assert.Equal(t, "SERVICE", got.Type)
	assert.Equal(t, "ANALYST", got.DefaultRole)
	assert.True(t, got.HasRSAPublicKey)
	assert.False(t, got.Disabled)
	assert.False(t, got.Locked)
	assert.False(t, got.HasMfa)
	assert.Equal(t, "nightly etl", got.Comment)
	assert.Equal(t, time.Unix(1735689600, 0).UTC(), got.LastSuccessLogin)

	assert.Contains(t, capturedSQL, accountUsageUsersView)
	assert.NotContains(t, capturedSQL, "SHOW USERS")
}

// A NULL cell arrives as an empty string. The boolean columns must read false rather than
// failing the parse, which is what a user row with no LAST_SUCCESS_LOGIN or COMMENT looks like.
func TestListUsers_AccountUsageHandlesNullColumns(t *testing.T) {
	var capturedSQL string
	rows := [][]string{{"NEWUSER", "newuser", "", "", "", "", "", "", "", "", "", "", "PERSON", "", ""}}
	server := serveAccountUsageRows(t, &capturedSQL, accountUsageUserRowTypes(), rows)
	defer server.Close()

	client := accountUsageClient(t, server.URL, server.Client())

	users, err := client.ListUsers(context.Background(), "", 50)
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "NEWUSER", users[0].Username)
	assert.Equal(t, "newuser", users[0].Login)
	assert.False(t, users[0].Disabled)
	assert.True(t, users[0].LastSuccessLogin.IsZero())
}

// GetUser cannot use DESCRIBE USER under ACCOUNT_USAGE - DESCRIBE USER requires OWNERSHIP on
// the target user, which is exactly the privilege this mode exists to avoid needing. It must
// read the view and parse the row shape, not the property/value shape.
func TestGetUser_AccountUsageReadsTheViewNotDescribeUser(t *testing.T) {
	var capturedSQL string
	rows := [][]string{{
		"ALICE", "alice@example.com", "Alice", "Alice", "Example", "alice@example.com",
		"false", "false", "", "false", "true", "", "PERSON", "true", "",
	}}
	server := serveAccountUsageRows(t, &capturedSQL, accountUsageUserRowTypes(), rows)
	defer server.Close()

	client := accountUsageClient(t, server.URL, server.Client())

	user, status, err := client.GetUser(context.Background(), nil, "ALICE")
	require.NoError(t, err)
	require.NotNil(t, user)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "ALICE", user.Username)
	assert.Equal(t, "alice@example.com", user.Login)
	assert.True(t, user.HasMfa)

	assert.NotContains(t, capturedSQL, "DESCRIBE USER")
	assert.Contains(t, capturedSQL, "NAME = 'ALICE'")
}

// GetUser's callers dereference the returned user whenever err is nil, so a user the view
// does not contain must be an error rather than (nil, nil) - otherwise the token-issuance
// path panics on user.Type. DESCRIBE USER behaves the same way, so both modes match.
func TestGetUser_AccountUsageMissingUserIsAnError(t *testing.T) {
	var capturedSQL string
	server := serveAccountUsageRows(t, &capturedSQL, accountUsageUserRowTypes(), [][]string{})
	defer server.Close()

	client := accountUsageClient(t, server.URL, server.Client())

	user, _, err := client.GetUser(context.Background(), nil, "GONE")
	require.Error(t, err)
	assert.Nil(t, user)
	// ACCOUNT_USAGE lag is the likely cause for a recently created user, so the error has
	// to say so rather than reading as "this user does not exist".
	assert.Contains(t, err.Error(), "three hours")
}

// Provisioning must never read through the ACCOUNT_USAGE view: it lags the live account by
// up to three hours, so a user the connector just created is reliably absent from it. The
// post-create read-back and the pre-issuance property lookup both go through DescribeUser,
// which stays on DESCRIBE USER even under ACCOUNT_USAGE discovery.
func TestDescribeUser_StaysOnDescribeUserUnderAccountUsageDiscovery(t *testing.T) {
	var capturedSQL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			var req StatementsApiRequestBody
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			capturedSQL = req.Statement
		}
		// DESCRIBE USER's property/value row shape, uppercased as Snowflake returns it.
		rows := [][]string{
			{"NAME", "SVC_NEW"}, {"LOGIN_NAME", "svc_new"}, {"DISPLAY_NAME", "svc"},
			{"FIRST_NAME", ""}, {"LAST_NAME", ""}, {"EMAIL", ""},
			{"DISABLED", "false"}, {"SNOWFLAKE_LOCK", "false"}, {"DEFAULT_ROLE", "ANALYST"},
			{"TYPE", "SERVICE"}, {"HAS_MFA", "false"}, {"COMMENT", ""},
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"statementHandle":   "handle",
			"resultSetMetadata": map[string]interface{}{"numRows": len(rows)},
			"data":              rows,
		})
	}))
	defer server.Close()

	client := accountUsageClient(t, server.URL, server.Client())

	user, _, err := client.DescribeUser(context.Background(), nil, "SVC_NEW")
	require.NoError(t, err)
	require.NotNil(t, user)
	assert.Equal(t, "SVC_NEW", user.Username)
	assert.Equal(t, "SERVICE", user.Type)
	assert.Equal(t, "ANALYST", user.DefaultRole)

	assert.Contains(t, capturedSQL, "DESCRIBE USER")
	assert.NotContains(t, capturedSQL, accountUsageUsersView)
}

// A missing ACCOUNT_USAGE grant is reported by Snowflake as a SQL compilation error, not as
// an access-control denial, so it is indistinguishable from a bad view name without help.
// The named error has to say what to grant - and must NOT be mistaken for the skippable
// insufficient-privileges condition, which would sync an empty user list and delete every
// previously synced user.
func TestAccountUsageMissingGrantProducesNamedActionableError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = fmt.Fprintf(w,
			`{"code":"002003","message":"SQL compilation error:\nObject '%s' does not exist or not authorized."}`,
			accountUsageUsersView,
		)
	}))
	defer server.Close()

	client := accountUsageClient(t, server.URL, server.Client())

	_, _, err := client.GetUser(context.Background(), nil, "ALICE")
	require.Error(t, err)
	require.True(t, IsAccountUsageUnavailable(err), "want ErrAccountUsageUnavailable, got %v", err)
	require.False(t, IsInsufficientPrivileges(err),
		"a missing ACCOUNT_USAGE grant must not be skippable: skipping it would sync an empty result")
	assert.Contains(t, err.Error(), "SECURITY_VIEWER")
	assert.Contains(t, err.Error(), "OBJECT_VIEWER")
	assert.Contains(t, err.Error(), "warehouse")
}

// The table-grant cache key includes the object kind, and normalizeObjectKind now supplies
// it. A TABLE and a VIEW of the same name must not share a cache entry.
func TestTableGrantsCacheKeyDistinguishesObjectKind(t *testing.T) {
	t.Parallel()
	tableKey := tableGrantsCacheKey("DB", publicSchema, "T", "TABLE")
	viewKey := tableGrantsCacheKey("DB", publicSchema, "T", "VIEW")
	assert.NotEqual(t, tableKey, viewKey)
	assert.Equal(t, tableKey, tableGrantsCacheKey("DB", publicSchema, "T", "transient"))
	assert.Equal(t, viewKey, tableGrantsCacheKey("DB", publicSchema, "T", "view"))
}

// ParseDiscoveryMode is the config boundary: an unset value must keep today's SHOW behavior,
// and a typo must fail at startup rather than silently falling back to a mode the operator
// did not choose.
func TestParseDiscoveryMode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in   string
		want DiscoveryMode
	}{
		{"", DiscoveryModeShow},
		{"show", DiscoveryModeShow},
		{"SHOW", DiscoveryModeShow},
		{"  Show  ", DiscoveryModeShow},
		{"account_usage", DiscoveryModeAccountUsage},
		{"ACCOUNT_USAGE", DiscoveryModeAccountUsage},
	} {
		got, err := ParseDiscoveryMode(tc.in)
		require.NoError(t, err, "input %q", tc.in)
		assert.Equal(t, tc.want, got, "input %q", tc.in)
	}

	for _, bad := range []string{"account-usage", "accountusage", "views", "yes"} {
		_, err := ParseDiscoveryMode(bad)
		require.Error(t, err, "input %q must be rejected", bad)
		assert.Contains(t, err.Error(), bad)
	}
}

// Under SHOW discovery, read failures must keep their existing shape: turning them into
// ErrAccountUsageUnavailable would be a regression for every tenant that has not opted in.
func TestClassifyReadErrorLeavesShowModeUnchanged(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = fmt.Fprint(w, `{"code":"002003","message":"SQL compilation error:\nObject does not exist."}`)
	}))
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, server.Client())
	require.NoError(t, err)

	_, err = client.ListUsers(context.Background(), "", 50)
	require.Error(t, err)
	assert.False(t, IsAccountUsageUnavailable(err))
}

// A missing ACCOUNT_USAGE grant on the list paths has to produce the actionable error too,
// not just on GetUser: ListUsers/ListAccountRoles/ListDatabases are where a sync starts, and
// they are the reads that would otherwise return an empty result set.
func TestAccountUsageListPathsClassifyMissingGrant(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		view string
		call func(c *Client) error
	}{
		{
			name: "users",
			view: accountUsageUsersView,
			call: func(c *Client) error { _, err := c.ListUsers(context.Background(), "", 50); return err },
		},
		{
			name: "roles",
			view: accountUsageRolesView,
			call: func(c *Client) error { _, err := c.ListAccountRoles(context.Background(), "", 50); return err },
		},
		{
			name: "databases",
			view: accountUsageDatabasesView,
			call: func(c *Client) error { _, err := c.ListDatabases(context.Background(), "", 50); return err },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = fmt.Fprint(w, `{"code":"002003","message":"SQL compilation error:\nObject does not exist or not authorized."}`)
			}))
			t.Cleanup(server.Close)

			client := accountUsageClient(t, server.URL, server.Client())
			err := tc.call(client)
			require.Error(t, err)
			require.True(t, IsAccountUsageUnavailable(err), "got %v", err)
			require.False(t, IsInsufficientPrivileges(err),
				"an account-wide view failure must not be skippable as invisible data")
			assert.Contains(t, err.Error(), tc.view)
		})
	}
}

// A column the account's ACCOUNT_USAGE schema does not expose is a different failure with a
// different fix than a missing grant, so it must not be reported as one.
func TestAccountUsageInvalidIdentifierIsDistinguishedFromMissingGrant(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = fmt.Fprint(w, `{"code":"000904","message":"SQL compilation error: error line 1 at position 8\ninvalid identifier 'HAS_MFA'"}`)
	}))
	defer server.Close()

	client := accountUsageClient(t, server.URL, server.Client())

	_, err := client.ListUsers(context.Background(), "", 50)
	require.Error(t, err)
	require.True(t, IsAccountUsageUnavailable(err), "got %v", err)
	assert.Contains(t, err.Error(), "does not expose it")
	assert.Contains(t, err.Error(), "--discovery-mode=show")
	// The viewer-grant remedy would be the wrong advice here.
	assert.NotContains(t, err.Error(), "SECURITY_VIEWER")
}
