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

// TestGetDatabase_EscapesName verifies that a database name containing a single quote
// is escaped before being interpolated into the SHOW DATABASES LIKE '...' statement.
// GetDatabase resolves in a single POST (no follow-up GET), so the server must return the
// matching row directly, unlike captureStatement's generic empty-data response.
func TestGetDatabase_EscapesName(t *testing.T) {
	const database = `o'brien`

	var capturedSQL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var req StatementsApiRequestBody
		require.NoError(t, json.Unmarshal(body, &req))
		capturedSQL = req.Statement

		enc := json.NewEncoder(w)
		_ = enc.Encode(map[string]interface{}{
			"statementHandle": "handle",
			"resultSetMetadata": map[string]interface{}{
				"numRows": 1,
				"rowType": []map[string]interface{}{
					{"name": "name", "type": "text"},
					{"name": "owner", "type": "text"},
					{"name": "kind", "type": "text"},
					{"name": "origin", "type": "text"},
				},
			},
			"data": [][]string{{database, "SYSADMIN", "STANDARD", ""}},
		})
	}))
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	db, _, err := client.GetDatabase(context.Background(), database)
	require.NoError(t, err)
	assert.Equal(t, `SHOW DATABASES LIKE 'o''brien' LIMIT 1;`, capturedSQL)
	assert.Equal(t, database, db.Name)
}

// TestListDatabases_EscapesCursor verifies that the pagination cursor - the bare name of
// the last database from a previous page, which can itself contain a single quote - is
// escaped before being interpolated into the SHOW DATABASES LIMIT ... FROM '...' statement.
func TestListDatabases_EscapesCursor(t *testing.T) {
	const cursor = `o'brien`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	_, err = client.ListDatabases(context.Background(), cursor, 100)
	require.NoError(t, err)
	assert.Equal(t, `SHOW DATABASES LIMIT 100 FROM 'o''brien';`, capturedSQL)
}
