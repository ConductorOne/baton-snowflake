package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	snowflake "github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newAccountRoleBuilderForServer(t *testing.T, handler http.HandlerFunc) *accountRoleBuilder {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	require.NoError(t, err)

	return newAccountRoleBuilder(client)
}

func rejectingHandler(message string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code":    "003001",
			"message": message,
		})
	}
}

func succeedingHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"statementHandle":   "handle",
		"resultSetMetadata": map[string]any{"numRows": 0},
		"data":              [][]string{},
	})
}

func accountRoleTestResources(t *testing.T) (*v2.Resource, *v2.Entitlement, *v2.Grant) {
	t.Helper()

	role, err := accountRoleResource(&snowflake.AccountRole{Name: "ACCOUNTADMIN"})
	require.NoError(t, err)

	user, err := userResource(context.Background(), &snowflake.User{Username: "ALICE", Type: "PERSON"}, secretOptions{})
	require.NoError(t, err)

	entitlement := &v2.Entitlement{Id: "account_role:ACCOUNTADMIN:assigned", Resource: role}
	grant := &v2.Grant{Id: "grant-1", Entitlement: entitlement, Principal: user}

	return user, entitlement, grant
}

func TestAccountRoleGrant_ReturnsErrorWhenSnowflakeRejects(t *testing.T) {
	builder := newAccountRoleBuilderForServer(t, rejectingHandler("SQL access control error: Insufficient privileges"))
	user, entitlement, _ := accountRoleTestResources(t)

	annos, err := builder.Grant(context.Background(), user, entitlement)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to grant account role")
	assert.Nil(t, annos)
}

func TestAccountRoleRevoke_ReturnsErrorWhenSnowflakeRejects(t *testing.T) {
	builder := newAccountRoleBuilderForServer(t, rejectingHandler("Role ACCOUNTADMIN cannot be revoked for user"))
	_, _, grant := accountRoleTestResources(t)

	annos, err := builder.Revoke(context.Background(), grant)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to revoke account role")
	assert.Nil(t, annos)
}

func TestAccountRoleGrant_SucceedsWhenSnowflakeAccepts(t *testing.T) {
	builder := newAccountRoleBuilderForServer(t, succeedingHandler)
	user, entitlement, _ := accountRoleTestResources(t)

	annos, err := builder.Grant(context.Background(), user, entitlement)

	require.NoError(t, err)
	assert.Nil(t, annos)
}

func TestAccountRoleRevoke_SucceedsWhenSnowflakeAccepts(t *testing.T) {
	builder := newAccountRoleBuilderForServer(t, succeedingHandler)
	_, _, grant := accountRoleTestResources(t)

	annos, err := builder.Revoke(context.Background(), grant)

	require.NoError(t, err)
	assert.Nil(t, annos)
}
