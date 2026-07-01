package snowflake

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
