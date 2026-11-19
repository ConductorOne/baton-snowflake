package snowflake

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// TestClearDescribeUserNulls walks every string field of User, so a field added later without being
// covered fails here instead of silently reaching C1 as "null".
func TestClearDescribeUserNulls(t *testing.T) {
	user := &User{}
	reflected := reflect.ValueOf(user).Elem()
	for i := 0; i < reflected.NumField(); i++ {
		if reflected.Field(i).Kind() == reflect.String {
			reflected.Field(i).SetString("null")
		}
	}

	ClearDescribeUserNulls(user)

	for i := 0; i < reflected.NumField(); i++ {
		field := reflected.Type().Field(i)
		if field.Type.Kind() != reflect.String {
			continue
		}
		if field.Name == "Username" {
			assert.Equal(t, "null", reflected.Field(i).String(), "Username is the resource ID and must be kept")
			continue
		}
		assert.Empty(t, reflected.Field(i).String(), "field %s", field.Name)
	}

	kept := &User{Email: "a@example.com", Comment: " null "}
	ClearDescribeUserNulls(kept)
	assert.Equal(t, "a@example.com", kept.Email)
	assert.Equal(t, " null ", kept.Comment, "only the exact literal is cleared")
}

// TestGetUser_AccessControlDenialIsClassified verifies that a 422/003001 from DESCRIBE USER is
// recognisable as a privilege denial while keeping the error's original gRPC code.
func TestGetUser_AccessControlDenialIsClassified(t *testing.T) {
	tests := []struct {
		name     string
		code     string
		wantPriv bool
	}{
		{name: "access control denial", code: "003001", wantPriv: true},
		{name: "user does not exist", code: "002003", wantPriv: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = fmt.Fprintf(w, `{"code":%q,"message":"denied"}`, tc.code)
			}))
			defer srv.Close()
			client, err := New(srv.URL, JWTConfig{}, srv.Client())
			require.NoError(t, err)

			_, statusCode, err := client.GetUser(context.Background(), nil, "alice")

			require.Error(t, err)
			assert.Equal(t, http.StatusUnprocessableEntity, statusCode)
			assert.Equal(t, tc.wantPriv, IsInsufficientPrivileges(err))
			assert.NotEqual(t, codes.Unknown, status.Code(err), "the original gRPC code must survive the join")
		})
	}
}
