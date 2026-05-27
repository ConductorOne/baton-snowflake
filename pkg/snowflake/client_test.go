package snowflake

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFetchStatementResultIfAsync_NilResponse(t *testing.T) {
	// &Client{} has a nil StatementsApiUrl. If the nil guard did not fire,
	// GetStatementResponse would call c.StatementsApiUrl.String() and panic.
	// A clean nil return proves the guard fires before any network call.
	c := &Client{}
	err := c.fetchStatementResultIfAsync(context.Background(), nil, "", nil)
	require.NoError(t, err)
}

func TestFetchStatementResultIfAsync_SyncResponse(t *testing.T) {
	// StatusCode 200 is not 202 — the function must return nil without polling.
	// Same nil-transport proof as above applies.
	c := &Client{}
	postResp := &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}
	err := c.fetchStatementResultIfAsync(context.Background(), postResp, "", nil)
	require.NoError(t, err)
}
