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

// serveDatabaseMatch returns an httptest.Server implementing the Snowflake Statements API
// for a single-row SHOW DATABASES LIKE ... response matching the given database name, and
// captures the "statement" field of the POST body into capturedSQL. GetDatabase resolves in
// a single POST (no follow-up GET), so the row must be returned directly.
func serveDatabaseMatch(t *testing.T, database string, capturedSQL *string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var req StatementsApiRequestBody
		require.NoError(t, json.Unmarshal(body, &req))
		*capturedSQL = req.Statement

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
}

// TestGetDatabase_EscapesName verifies that a database name containing a single quote
// is escaped before being interpolated into the SHOW DATABASES LIKE '...' statement.
func TestGetDatabase_EscapesName(t *testing.T) {
	const database = `o'brien`

	var capturedSQL string
	server := serveDatabaseMatch(t, database, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	db, _, err := client.GetDatabase(context.Background(), database)
	require.NoError(t, err)
	assert.Equal(t, `SHOW DATABASES LIKE 'o''brien' LIMIT 1;`, capturedSQL)
	assert.Equal(t, database, db.Name)
}

// TestGetDatabase_NoEscapeClauseForLikeWildcards documents a Snowflake limitation: SHOW
// DATABASES' LIKE filter has no ESCAPE clause (unlike the general SQL LIKE predicate/WHERE
// usage), so there is no syntax to make an underscore or percent sign in a database name
// match literally. Only the single quote is escaped, to keep the string literal well-formed;
// _ and % are sent through untouched and remain active wildcards. A prior version of this
// code added "ESCAPE '\'" to the statement to try to neutralize these wildcards, but SHOW
// DATABASES does not support that clause at all - Snowflake rejects it as a 422 Unprocessable
// Entity (SQL compilation error) on every call, not just ones with wildcard characters.
func TestGetDatabase_NoEscapeClauseForLikeWildcards(t *testing.T) {
	const database = `PROD_DB%1`

	var capturedSQL string
	server := serveDatabaseMatch(t, database, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	db, _, err := client.GetDatabase(context.Background(), database)
	require.NoError(t, err)
	assert.Equal(t, `SHOW DATABASES LIKE 'PROD_DB%1' LIMIT 1;`, capturedSQL)
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
