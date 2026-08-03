package snowflake

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListUsers_EscapesCursor verifies that the pagination cursor - the bare name of the
// last user from a previous page, which can itself contain a single quote (e.g. a user
// created as CREATE USER "o'brien") - is escaped before being interpolated into the
// SHOW USERS LIMIT ... FROM '...' statement.
func TestListUsers_EscapesCursor(t *testing.T) {
	const cursor = `o'brien`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	_, err = client.ListUsers(context.Background(), cursor, 100)
	require.NoError(t, err)
	assert.Equal(t, `SHOW USERS LIMIT 100 FROM 'o''brien';`, capturedSQL)
}
