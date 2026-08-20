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

// TestSetUserDisabled_EscapesIdentifiers verifies that a username containing an embedded
// double quote is escaped before being interpolated into the ALTER USER "..." SET DISABLED
// = ...; statement.
func TestSetUserDisabled_EscapesIdentifiers(t *testing.T) {
	const user = `weird"user`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	err = client.SetUserDisabled(context.Background(), user, true)
	require.NoError(t, err)
	assert.Equal(t, `ALTER USER "weird""user" SET DISABLED = true;`, capturedSQL)
}

// TestSetUserDisabled_RendersBooleanValue pins the %t rendering of the disabled argument
// for both the enable and disable directions.
func TestSetUserDisabled_RendersBooleanValue(t *testing.T) {
	tests := []struct {
		name     string
		disabled bool
		want     string
	}{
		{name: "disable", disabled: true, want: `ALTER USER "testuser" SET DISABLED = true;`},
		{name: "enable", disabled: false, want: `ALTER USER "testuser" SET DISABLED = false;`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedSQL string
			server := captureStatement(t, &capturedSQL)
			defer server.Close()

			client, err := New(server.URL, JWTConfig{}, &http.Client{})
			require.NoError(t, err)

			err = client.SetUserDisabled(context.Background(), "testuser", tt.disabled)
			require.NoError(t, err)
			assert.Equal(t, tt.want, capturedSQL)
		})
	}
}
