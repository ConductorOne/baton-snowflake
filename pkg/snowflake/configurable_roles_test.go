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

// The defaults are a compatibility contract, not a preference: a tenant that upgrades
// without setting the new fields must keep sending exactly the roles the connector
// hard-coded before, or every write path and license sync breaks on upgrade.
func TestClientRoleDefaults(t *testing.T) {
	t.Parallel()

	client, err := New("https://example.snowflakecomputing.com", JWTConfig{}, &http.Client{})
	require.NoError(t, err)
	assert.Equal(t, UserAdminRole, client.WriteRole)
	assert.Equal(t, GlobalOrgAdminRole, client.OrganizationRole)
	assert.Equal(t, DiscoveryModeShow, client.DiscoveryMode)

	// A blank configured value must fall back to the default rather than send an empty
	// role, which the SQL API silently treats as "run as the session's default role" -
	// a different role with different privileges, not a no-op.
	blank, err := New("https://example.snowflakecomputing.com", JWTConfig{}, &http.Client{},
		WithWriteRole("   "), WithOrganizationRole(""), WithDiscoveryMode(""))
	require.NoError(t, err)
	assert.Equal(t, UserAdminRole, blank.WriteRole)
	assert.Equal(t, GlobalOrgAdminRole, blank.OrganizationRole)
	assert.Equal(t, DiscoveryModeShow, blank.DiscoveryMode)

	// A zero-valued Client (constructed as a literal, as several tests do) must still
	// resolve to the defaults rather than sending an empty role.
	var zero Client
	assert.Equal(t, UserAdminRole, zero.writeRole())
	assert.Equal(t, GlobalOrgAdminRole, zero.organizationRole())
	assert.False(t, zero.usesAccountUsage())
}

func TestClientRoleOverrides(t *testing.T) {
	t.Parallel()

	client, err := New("https://example.snowflakecomputing.com", JWTConfig{}, &http.Client{},
		WithWriteRole("  C1_USER_LIFECYCLE  "), WithOrganizationRole("C1_ORG_READER"))
	require.NoError(t, err)
	// Surrounding whitespace from a config field must not end up in the role name.
	assert.Equal(t, "C1_USER_LIFECYCLE", client.writeRole())
	assert.Equal(t, "C1_ORG_READER", client.organizationRole())
}

// recordRoleServer answers any statement with an empty result set and records the role sent
// in the request body.
func recordRoleServer(t *testing.T, recorder *statementRecorder) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var request StatementsApiRequestBody
			require.NoError(t, json.Unmarshal(body, &request))
			recorder.record(request.Statement, request.Role)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"statementHandle":   "handle",
			"resultSetMetadata": map[string]any{"numRows": 0},
			"data":              [][]string{},
		})
	}))
}

// Every write path has to honor the override. Missing one would leave the tenant still
// needing USERADMIN granted to the service user for that single operation, which defeats
// the point of making the role configurable at all.
func TestWriteRoleOverrideAppliesToEverySQLWritePath(t *testing.T) {
	t.Parallel()
	const customRole = "C1_USER_LIFECYCLE"

	for _, tc := range []struct {
		name string
		call func(c *Client) error
	}{
		{
			name: "ALTER USER SET DISABLED",
			call: func(c *Client) error { return c.SetUserDisabled(context.Background(), "svc", true) },
		},
		{
			name: "ALTER USER REMOVE PROGRAMMATIC ACCESS TOKEN",
			call: func(c *Client) error {
				return c.RemoveProgrammaticAccessToken(context.Background(), "svc", "c1-request-1")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recorder := &statementRecorder{}
			server := recordRoleServer(t, recorder)
			defer server.Close()

			client, err := New(server.URL, JWTConfig{}, server.Client(), WithWriteRole(customRole))
			require.NoError(t, err)

			require.NoError(t, tc.call(client))
			_, roles := recorder.snapshot()
			require.NotEmpty(t, roles)
			assert.Equal(t, customRole, roles[0])
		})
	}
}

// Token creation is the third write path and needs a response shaped like a real
// ADD PROGRAMMATIC ACCESS TOKEN, so it uses the token server rather than the generic one.
func TestWriteRoleOverrideAppliesToTokenCreation(t *testing.T) {
	t.Parallel()
	const customRole = "C1_USER_LIFECYCLE"

	recorder := &statementRecorder{}
	server := newTokenCreateServer(t, recorder)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, server.Client(), WithWriteRole(customRole))
	require.NoError(t, err)

	_, err = client.CreateProgrammaticAccessToken(context.Background(), "svc", "c1-request-1", "", 7)
	require.NoError(t, err)
	_, roles := recorder.snapshot()
	require.NotEmpty(t, roles)
	assert.Equal(t, customRole, roles[0])
}

// The REST user lifecycle endpoints take the role in a header rather than the request body,
// so they are a separate code path from the SQL writes above and need their own coverage.
func TestWriteRoleOverrideAppliesToRESTUserLifecycle(t *testing.T) {
	t.Parallel()
	const customRole = "C1_USER_LIFECYCLE"

	for _, tc := range []struct {
		name string
		call func(c *Client) error
	}{
		{
			name: "create user",
			call: func(c *Client) error {
				_, _, err := c.CreateUserREST(context.Background(), &CreateUserRequest{Name: "svc"})
				return err
			},
		},
		{
			name: "delete user",
			call: func(c *Client) error {
				_, err := c.DeleteUserREST(context.Background(), "svc", nil)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotRole string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotRole = r.Header.Get(RoleHeaderKey)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "success"})
			}))
			t.Cleanup(server.Close)

			client, err := New(server.URL, JWTConfig{}, server.Client(), WithWriteRole(customRole))
			require.NoError(t, err)

			require.NoError(t, tc.call(client))
			assert.Equal(t, customRole, gotRole)
		})
	}
}

// With nothing configured the REST lifecycle must still send USERADMIN, which is the
// upgrade-compatibility half of the contract.
func TestRESTUserLifecycleDefaultsToUserAdmin(t *testing.T) {
	t.Parallel()
	var gotRole string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRole = r.Header.Get(RoleHeaderKey)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success"})
	}))
	t.Cleanup(server.Close)

	client, err := New(server.URL, JWTConfig{}, server.Client())
	require.NoError(t, err)
	_, _, err = client.CreateUserREST(context.Background(), &CreateUserRequest{Name: "svc"})
	require.NoError(t, err)
	assert.Equal(t, UserAdminRole, gotRole)
}

// SHOW ORGANIZATION ACCOUNTS is the only organization-scoped read, and it takes the role in
// the request body rather than the header.
func TestOrganizationRoleOverrideAppliesToOrganizationRead(t *testing.T) {
	t.Parallel()
	const customRole = "C1_ORG_READER"

	recorder := &statementRecorder{}
	server := recordRoleServer(t, recorder)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, server.Client(), WithOrganizationRole(customRole))
	require.NoError(t, err)

	_, _, err = client.ListOrganizationAccounts(context.Background())
	require.NoError(t, err)
	statements, roles := recorder.snapshot()
	require.NotEmpty(t, roles)
	assert.Equal(t, "SHOW ORGANIZATION ACCOUNTS;", statements[0])
	assert.Equal(t, customRole, roles[0])
}

// Overriding the write or organization role must not leak into reads: those run as the
// session's default role, and pinning a write role onto them would require the write role
// to also hold every read privilege.
func TestRoleOverridesDoNotLeakIntoReads(t *testing.T) {
	t.Parallel()

	recorder := &statementRecorder{}
	server := recordRoleServer(t, recorder)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, server.Client(),
		WithWriteRole("C1_USER_LIFECYCLE"), WithOrganizationRole("C1_ORG_READER"))
	require.NoError(t, err)

	_, err = client.ListUsers(context.Background(), "", 50)
	require.NoError(t, err)
	_, err = client.ListAccountRoles(context.Background(), "", 50)
	require.NoError(t, err)
	_, err = client.ListDatabases(context.Background(), "", 50)
	require.NoError(t, err)

	_, roles := recorder.snapshot()
	require.Len(t, roles, 3)
	for i, role := range roles {
		assert.Empty(t, role, "read %d must run as the session's default role", i)
	}
}

// DescribeUser is the provisioning read: CreateAccount reads the new user back through it
// and Issue looks up properties before minting a token. DESCRIBE USER requires OWNERSHIP on
// the target user, and it is the write role that creates users, so the write role is the one
// that owns them. Running it as the session's default role - which the documented
// least-privilege setups never grant OWNERSHIP to - would create a user successfully and
// then fail to read it back.
func TestDescribeUserRunsAsTheWriteRole(t *testing.T) {
	t.Parallel()
	const customRole = "C1_USER_LIFECYCLE"

	recorder := &statementRecorder{}
	server := recordRoleServer(t, recorder)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, server.Client(), WithWriteRole(customRole))
	require.NoError(t, err)

	// The empty result set the recorder returns makes DescribeUser fail to parse a user;
	// the role on the wire is what this test is about, so the error is not the subject.
	_, _, _ = client.DescribeUser(context.Background(), nil, "svc")

	statements, roles := recorder.snapshot()
	require.NotEmpty(t, roles)
	assert.Equal(t, `DESCRIBE USER "svc";`, statements[0])
	assert.Equal(t, customRole, roles[0], "DescribeUser must pin the write role")
}

// The discovery counterpart: GetUser serves read-only sync, whose callers hold no write
// privileges. Pinning the write role there would make a read-only sync depend on a
// provisioning role, so it stays on the session's default role.
func TestGetUserStaysOnTheSessionDefaultRole(t *testing.T) {
	t.Parallel()

	recorder := &statementRecorder{}
	server := recordRoleServer(t, recorder)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, server.Client(), WithWriteRole("C1_USER_LIFECYCLE"))
	require.NoError(t, err)

	_, _, _ = client.GetUser(context.Background(), nil, "svc")

	_, roles := recorder.snapshot()
	require.NotEmpty(t, roles)
	assert.Empty(t, roles[0], "discovery reads must run as the session's default role")
}
