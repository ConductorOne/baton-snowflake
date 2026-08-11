package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

// newDatabaseGrantsMockServer serves the two statements databaseBuilder.Grants issues: SHOW
// DATABASES LIKE to resolve the database and its owner, then SHOW ROLES LIKE to resolve that owner
// into an account-role principal. Both are answered inline on the POST, as the real Statements API
// does for these. ownerRoleVisible drives whether the second one is denied with the access-control
// 422 Snowflake returns for a role the connector cannot describe.
func newDatabaseGrantsMockServer(t *testing.T, owner string, ownerRoleVisible bool) *httptest.Server {
	t.Helper()

	databaseRowTypes := []map[string]any{
		{keyName: keyName, keyType: colText},
		{keyName: colOwner, keyType: colText},
		{keyName: colKind, keyType: colText},
		{keyName: colOrigin, keyType: colText},
	}
	roleRowTypes := []map[string]any{
		{keyName: keyName, keyType: colText},
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)

		var body struct {
			Statement string `json:"statement"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)

		switch {
		case strings.Contains(body.Statement, "SHOW DATABASES LIKE"):
			_ = enc.Encode(map[string]any{
				"resultSetMetadata": map[string]any{
					"numRows":       1,
					"partitionInfo": []map[string]any{{"rowCount": 1}},
					"rowType":       databaseRowTypes,
				},
				"data": [][]string{{"DB", owner, "STANDARD", ""}},
			})
		case strings.Contains(body.Statement, "SHOW ROLES LIKE"):
			if !ownerRoleVisible {
				w.WriteHeader(http.StatusUnprocessableEntity)
				_ = enc.Encode(accessControlErrorBody)
				return
			}
			_ = enc.Encode(map[string]any{
				"resultSetMetadata": map[string]any{
					"numRows":       1,
					"partitionInfo": []map[string]any{{"rowCount": 1}},
					"rowType":       roleRowTypes,
				},
				"data": [][]string{{owner}},
			})
		default:
			t.Errorf("unexpected statement: %s", body.Statement)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}

// TestDatabaseBuilder_Grants_SkipsOwnerWhenRoleIsNotDescribable covers the sibling call site of
// CXH-2193. Databases are routinely owned by a system role (SYSADMIN, ACCOUNTADMIN) that a
// least-privilege connector role cannot describe, and Snowflake reports that as 422. This branch
// recognised the 422 but still returned PermissionDenied, which cancels the SDK's shared sync
// context - so one system-owned database discarded the whole sync exactly like the schema listing
// in the ticket. The table builder's identical owner lookup already skips it.
func TestDatabaseBuilder_Grants_SkipsOwnerWhenRoleIsNotDescribable(t *testing.T) {
	server := newDatabaseGrantsMockServer(t, "SYSADMIN", false)
	defer server.Close()

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	builder := &databaseBuilder{client: client}
	resource := &v2.Resource{Id: &v2.ResourceId{ResourceType: databaseResourceType.Id, Resource: "DB"}}

	grants, results, err := builder.Grants(context.Background(), resource, rs.SyncOpAttrs{})

	require.NoError(t, err, "an undescribable owner role must skip the grant, not fail the sync")
	assert.Empty(t, grants, "no owner principal can be resolved, so no grant is emitted")
	require.NotNil(t, results)
	assert.Empty(t, results.NextPageToken)
}

// TestDatabaseBuilder_Grants_EmitsOwnerWhenRoleIsDescribable is the control: it proves the mock
// drives the real path, so the skip above cannot pass because the grant was never reachable.
func TestDatabaseBuilder_Grants_EmitsOwnerWhenRoleIsDescribable(t *testing.T) {
	server := newDatabaseGrantsMockServer(t, "SYSADMIN", true)
	defer server.Close()

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	builder := &databaseBuilder{client: client}
	resource := &v2.Resource{Id: &v2.ResourceId{ResourceType: databaseResourceType.Id, Resource: "DB"}}

	grants, _, err := builder.Grants(context.Background(), resource, rs.SyncOpAttrs{})

	require.NoError(t, err)
	require.Len(t, grants, 1)
	assert.Equal(t, "SYSADMIN", grants[0].Principal.Id.Resource)
}
