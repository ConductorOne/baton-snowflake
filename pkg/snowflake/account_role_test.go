package snowflake

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureStatement returns an httptest.Server that records the "statement" field of
// the initial POST body it receives (the SQL text sent to the Statements API) into
// capturedSQL, then replies with a minimal valid response - including to any follow-up
// GET made to fetch the statement result - so the client's read path doesn't error.
func captureStatement(t *testing.T, capturedSQL *string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)

			var req StatementsApiRequestBody
			require.NoError(t, json.Unmarshal(body, &req))
			*capturedSQL = req.Statement
		}

		enc := json.NewEncoder(w)
		_ = enc.Encode(map[string]interface{}{
			"statementHandle": "handle",
			"resultSetMetadata": map[string]interface{}{
				"numRows": 0,
			},
			"data": [][]string{},
		})
	}))
}

// serveGrantees returns an httptest.Server that implements the Snowflake Statements
// API for SHOW GRANTS OF ROLE. partition0Rows is returned on the initial GET
// (partition 0); if partition1Rows is non-nil a second partition is advertised
// and served on ?partition=1.
func serveGrantees(t *testing.T, handle string, partition0Rows, partition1Rows [][]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)

		switch r.Method {
		case http.MethodPost:
			// Step 1: execute statement → return handle only.
			_ = enc.Encode(map[string]interface{}{
				"statementHandle": handle,
			})

		case http.MethodGet:
			_, hasPartition := r.URL.Query()["partition"]
			if !hasPartition {
				// Step 2: partition 0 + full partitionInfo metadata.
				partitionInfo := []map[string]interface{}{
					{"rowCount": len(partition0Rows)},
				}
				if partition1Rows != nil {
					partitionInfo = append(partitionInfo, map[string]interface{}{
						"rowCount": len(partition1Rows),
					})
				}
				_ = enc.Encode(map[string]interface{}{
					"statementHandle": handle,
					"resultSetMetadata": map[string]interface{}{
						"numRows":       len(partition0Rows) + len(partition1Rows),
						"partitionInfo": partitionInfo,
					},
					"data": partition0Rows,
				})
			} else {
				// Step 3: subsequent partition — data only, no metadata.
				require.Equal(t, "1", r.URL.Query().Get("partition"), "only partition 1 expected in this test")
				_ = enc.Encode(map[string]interface{}{
					"data": partition1Rows,
				})
			}

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}

// granteeRow builds a data row in the order GetAccountRoleGrantees expects:
// index 1 = roleName, index 2 = granteeType, index 3 = granteeName.
func granteeRow(roleName, granteeType, granteeName string) []string {
	return []string{"", roleName, granteeType, granteeName}
}

func TestListAccountRoleGrantees_SinglePartition(t *testing.T) {
	const handle = "handle-single"
	const role = "MYROLE"

	rows := [][]string{
		granteeRow(role, "USER", "alice"),
		granteeRow(role, "ROLE", "SYSADMIN"),
	}
	server := serveGrantees(t, handle, rows, nil)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	grantees, nextCursor, err := client.ListAccountRoleGrantees(context.Background(), role, "")
	require.NoError(t, err)
	assert.Empty(t, nextCursor, "single partition should produce no next cursor")
	require.Len(t, grantees, 2)
	assert.Equal(t, AccountRoleGrantee{RoleName: role, GranteeType: "USER", GranteeName: "alice"}, grantees[0])
	assert.Equal(t, AccountRoleGrantee{RoleName: role, GranteeType: "ROLE", GranteeName: "SYSADMIN"}, grantees[1])
}

// TestListAccountRoleGrantees_UnquotesGranteeName verifies the CXP-784 fix: Snowflake's
// SHOW GRANTS OF ROLE renders grantee names that require quoting (mixed case, spaces) wrapped
// in double quotes, with any embedded double quote doubled. GranteeName must come back
// unquoted so it matches the canonical (unquoted) ID that SHOW ROLES produces for the same
// role - otherwise nested-role expansion and principal-ID matching silently fail.
func TestListAccountRoleGrantees_UnquotesGranteeName(t *testing.T) {
	const handle = "handle-quoted"
	const role = "MYROLE"

	rows := [][]string{
		granteeRow(role, "ROLE", `"Data Engineer"`),
		granteeRow(role, "USER", `"He said ""hi"""`),
		granteeRow(role, "ROLE", "SYSADMIN"),
	}
	server := serveGrantees(t, handle, rows, nil)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	grantees, nextCursor, err := client.ListAccountRoleGrantees(context.Background(), role, "")
	require.NoError(t, err)
	assert.Empty(t, nextCursor)
	require.Len(t, grantees, 3)
	assert.Equal(t, "Data Engineer", grantees[0].GranteeName, "quoted mixed-case name should be unquoted")
	assert.Equal(t, `He said "hi"`, grantees[1].GranteeName, "embedded escaped quotes should be unescaped")
	assert.Equal(t, "SYSADMIN", grantees[2].GranteeName, "already-unquoted system role should be unaffected")
}

// accountRoleRowTypes matches the column layout ListAccountRoles' ParseRow expects for SHOW ROLES.
func accountRoleRowTypes() []map[string]interface{} {
	return []map[string]interface{}{
		{"name": "name", "type": "text"},
	}
}

// serveAccountRoles returns an httptest.Server implementing the Snowflake Statements API for
// SHOW ROLES, returning a single page of rows.
func serveAccountRoles(t *testing.T, handle string, rows [][]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)

		switch r.Method {
		case http.MethodPost:
			_ = enc.Encode(map[string]interface{}{
				"statementHandle": handle,
			})
		case http.MethodGet:
			_ = enc.Encode(map[string]interface{}{
				"statementHandle": handle,
				"resultSetMetadata": map[string]interface{}{
					"numRows": len(rows),
					"rowType": accountRoleRowTypes(),
				},
				"data": rows,
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}

// TestListAccountRoles_MatchesUnquotedGranteeID is the acceptance-criteria regression test for
// CXP-784: for a role whose name requires quoting in SHOW GRANTS output, the resource ID Baton
// builds from SHOW ROLES (canonical, bare name) must exactly match the principal ID built from
// the corresponding SHOW GRANTS OF ROLE grantee entry for that same role (unquoted by this fix).
// Before the fix, these diverged whenever the role name contained spaces/mixed case, silently
// breaking nested-role expansion.
func TestListAccountRoles_MatchesUnquotedGranteeID(t *testing.T) {
	const roleName = "Data Engineer"

	rolesServer := serveAccountRoles(t, "handle-roles", [][]string{{roleName}})
	defer rolesServer.Close()

	rolesClient, err := New(rolesServer.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	roles, err := rolesClient.ListAccountRoles(context.Background(), "", 100)
	require.NoError(t, err)
	require.Len(t, roles, 1)
	assert.Equal(t, roleName, roles[0].Name, "SHOW ROLES-derived name must remain bare/unquoted")

	granteesServer := serveGrantees(t, "handle-grantees", [][]string{
		granteeRow("PARENT_ROLE", "ROLE", `"Data Engineer"`),
	}, nil)
	defer granteesServer.Close()

	granteesClient, err := New(granteesServer.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	grantees, _, err := granteesClient.ListAccountRoleGrantees(context.Background(), "PARENT_ROLE", "")
	require.NoError(t, err)
	require.Len(t, grantees, 1)

	assert.Equal(t, roles[0].Name, grantees[0].GranteeName,
		"resource ID from SHOW ROLES must byte-for-byte match the principal ID derived from SHOW GRANTS OF ROLE")
}

func TestListAccountRoleGrantees_MultiPartition(t *testing.T) {
	const handle = "handle-multi"
	const role = "MYROLE"

	partition0 := [][]string{
		granteeRow(role, "USER", "alice"),
		granteeRow(role, "ROLE", "SYSADMIN"),
	}
	partition1 := [][]string{
		granteeRow(role, "USER", "bob"),
	}
	server := serveGrantees(t, handle, partition0, partition1)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	ctx := context.Background()

	// Page 1: empty cursor → executes query, returns partition 0 + cursor.
	page1, cursor1, err := client.ListAccountRoleGrantees(ctx, role, "")
	require.NoError(t, err)
	require.Len(t, page1, 2)
	assert.Equal(t, "alice", page1[0].GranteeName)
	assert.Equal(t, "USER", page1[0].GranteeType)
	assert.Equal(t, "SYSADMIN", page1[1].GranteeName)
	assert.Equal(t, "ROLE", page1[1].GranteeType)
	assert.NotEmpty(t, cursor1)

	// Page 2: cursor from page 1 → fetches ?partition=1, no further cursor.
	page2, cursor2, err := client.ListAccountRoleGrantees(ctx, role, cursor1)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, "bob", page2[0].GranteeName)
	assert.Equal(t, "USER", page2[0].GranteeType)
	assert.Empty(t, cursor2, "last partition should return empty cursor")
}

// TestListAccountRoleGrantees_EscapesRoleName verifies that a role name containing an
// embedded double quote (legal in Snowflake via a quoted identifier, e.g.
// CREATE ROLE "weird""role") is escaped before being interpolated into the
// SHOW GRANTS OF ROLE "..."; statement, rather than breaking out of the quoted identifier.
func TestListAccountRoleGrantees_EscapesRoleName(t *testing.T) {
	const role = `weird"role`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	_, _, err = client.ListAccountRoleGrantees(context.Background(), role, "")
	require.NoError(t, err)
	assert.Equal(t, `SHOW GRANTS OF ROLE "weird""role";`, capturedSQL)
}

// TestGetAccountRole_EscapesRoleName verifies that a role name containing a single quote
// is escaped before being interpolated into the SHOW ROLES LIKE '...' statement.
func TestGetAccountRole_EscapesRoleName(t *testing.T) {
	const role = `o'brien`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	_, _, err = client.GetAccountRole(context.Background(), nil, role)
	require.NoError(t, err)
	assert.Equal(t, `SHOW ROLES LIKE 'o''brien' LIMIT 1;`, capturedSQL)
}

// TestGetAccountRole_NoEscapeClauseForLikeWildcards documents a Snowflake limitation:
// SHOW ROLES' LIKE filter has no ESCAPE clause (unlike the general SQL LIKE predicate/WHERE
// usage), so there is no syntax to make an underscore or percent sign in a role name match
// literally. Only the single quote is escaped, to keep the string literal well-formed;
// _ and % are sent through untouched and remain active wildcards. A prior version of this
// code added "ESCAPE '\'" to the statement to try to neutralize these wildcards, but SHOW
// ROLES does not support that clause at all - Snowflake rejects it as a 422 Unprocessable
// Entity (SQL compilation error) on every call, not just ones with wildcard characters.
func TestGetAccountRole_NoEscapeClauseForLikeWildcards(t *testing.T) {
	const role = `DATA_ENGINEER%1`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	_, _, err = client.GetAccountRole(context.Background(), nil, role)
	require.NoError(t, err)
	assert.Equal(t, `SHOW ROLES LIKE 'DATA_ENGINEER%1' LIMIT 1;`, capturedSQL)
}

// TestGrantAccountRole_EscapesIdentifiers verifies that role and user names containing
// embedded double quotes are escaped before being interpolated into the
// GRANT ROLE "..." TO USER "..."; statement.
func TestGrantAccountRole_EscapesIdentifiers(t *testing.T) {
	const role = `weird"role`
	const user = `weird"user`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	err = client.GrantAccountRole(context.Background(), role, user)
	require.NoError(t, err)
	assert.Equal(t, `GRANT ROLE "weird""role" TO USER "weird""user";`, capturedSQL)
}

// TestRevokeAccountRole_EscapesIdentifiers verifies that role and user names containing
// embedded double quotes are escaped before being interpolated into the
// REVOKE ROLE "..." FROM USER "..."; statement.
func TestRevokeAccountRole_EscapesIdentifiers(t *testing.T) {
	const role = `weird"role`
	const user = `weird"user`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	err = client.RevokeAccountRole(context.Background(), role, user)
	require.NoError(t, err)
	assert.Equal(t, `REVOKE ROLE "weird""role" FROM USER "weird""user";`, capturedSQL)
}

// TestListAccountRoles_EscapesCursor verifies that the pagination cursor - which is the
// bare name of the last role from a previous page, and so can itself contain a single
// quote (e.g. a role created as CREATE ROLE "o'brien") - is escaped before being
// interpolated into the SHOW ROLES LIMIT ... FROM '...' statement.
func TestListAccountRoles_EscapesCursor(t *testing.T) {
	const cursor = `o'brien`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	_, err = client.ListAccountRoles(context.Background(), cursor, 100)
	require.NoError(t, err)
	assert.Equal(t, `SHOW ROLES LIMIT 100 FROM 'o''brien';`, capturedSQL)
}
