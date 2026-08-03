package snowflake

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListSecrets_EscapesDatabaseName verifies that a database name containing an
// embedded double quote is escaped before being interpolated into the
// SHOW SECRETS IN DATABASE "..."; statement.
func TestListSecrets_EscapesDatabaseName(t *testing.T) {
	const database = `weird"db`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	_, err = client.ListSecrets(context.Background(), database)
	require.NoError(t, err)
	assert.Equal(t, `SHOW SECRETS IN DATABASE "weird""db";`, capturedSQL)
}

// TestUserRsa_EscapesUsername verifies that a username containing an embedded double
// quote is escaped before being interpolated into the DESCRIBE USER "..."; statement.
func TestUserRsa_EscapesUsername(t *testing.T) {
	const username = `weird"user`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	_, err = client.UserRsa(context.Background(), username)
	require.NoError(t, err)
	assert.Equal(t, `DESCRIBE USER "weird""user";`, capturedSQL)
}
