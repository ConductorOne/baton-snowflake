package snowflake

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newTableGrantsTestClient starts an httptest server that returns a fixed HTTP
// status and JSON body for every request, then builds a Client pointing at it.
// The caller must call the returned cleanup func when done.
func newTableGrantsTestClient(t *testing.T, statusCode int, body string) (*Client, func()) {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(body))
	}))
	client, err := New(ts.URL, JWTConfig{}, ts.Client())
	require.NoError(t, err)
	return client, ts.Close
}

func TestListTableGrants_422_PermissionDenied(t *testing.T) {
	body := `{"code":"003001","message":"Insufficient privileges to operate on table"}`
	client, cleanup := newTableGrantsTestClient(t, http.StatusUnprocessableEntity, body)
	defer cleanup()

	_, err := client.ListTableGrants(context.Background(), nil, "MYDB", "PUBLIC", "MYTABLE", "TABLE")

	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	assert.False(t, errors.Is(err, ErrObjectNotFound))
}

func TestListTableGrants_422_ObjectNotFound(t *testing.T) {
	body := `{"code":"002003","message":"Object 'MYDB.PUBLIC.MYTABLE' does not exist or not authorized."}`
	client, cleanup := newTableGrantsTestClient(t, http.StatusUnprocessableEntity, body)
	defer cleanup()

	_, err := client.ListTableGrants(context.Background(), nil, "MYDB", "PUBLIC", "MYTABLE", "TABLE")

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrObjectNotFound))
	assert.NotEqual(t, codes.PermissionDenied, status.Code(err))
}

func TestListTableGrants_200_Success(t *testing.T) {
	body := `{"resultSetMetadata":{"numRows":0,"rowType":[]},"data":[],"statementHandle":""}`
	client, cleanup := newTableGrantsTestClient(t, http.StatusOK, body)
	defer cleanup()

	grants, err := client.ListTableGrants(context.Background(), nil, "MYDB", "PUBLIC", "MYTABLE", "TABLE")

	require.NoError(t, err)
	assert.Empty(t, grants)
}

// newAsyncTableGrantsTestClient starts an httptest server that simulates async execution:
// POST returns 202 with a statement handle; GET on /api/v2/statements/<handle> returns asyncStatusCode and asyncBody.
func newAsyncTableGrantsTestClient(t *testing.T, asyncStatusCode int, asyncBody string) (*Client, func()) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"statementHandle":"test-handle","resultSetMetadata":{"numRows":0,"rowType":[]},"data":[]}`))
			return
		}
		// GET /api/v2/statements/test-handle
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(asyncStatusCode)
		_, _ = w.Write([]byte(asyncBody))
	})
	ts := httptest.NewServer(mux)
	client, err := New(ts.URL, JWTConfig{}, ts.Client())
	require.NoError(t, err)
	return client, ts.Close
}

func TestListTableGrants_Async_422_PermissionDenied(t *testing.T) {
	body := `{"code":"003001","message":"Insufficient privileges to operate on table"}`
	client, cleanup := newAsyncTableGrantsTestClient(t, http.StatusUnprocessableEntity, body)
	defer cleanup()

	_, err := client.ListTableGrants(context.Background(), nil, "MYDB", "PUBLIC", "MYTABLE", "TABLE")

	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	assert.False(t, errors.Is(err, ErrObjectNotFound))
}

func TestListTableGrants_Async_422_ObjectNotFound(t *testing.T) {
	body := `{"code":"002003","message":"Object 'MYDB.PUBLIC.MYTABLE' does not exist or not authorized."}`
	client, cleanup := newAsyncTableGrantsTestClient(t, http.StatusUnprocessableEntity, body)
	defer cleanup()

	_, err := client.ListTableGrants(context.Background(), nil, "MYDB", "PUBLIC", "MYTABLE", "TABLE")

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrObjectNotFound))
	assert.NotEqual(t, codes.PermissionDenied, status.Code(err))
}
