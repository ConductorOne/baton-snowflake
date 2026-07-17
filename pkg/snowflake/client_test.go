package snowflake

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStatementsApiRequestBodyRole(t *testing.T) {
	withRole, err := json.Marshal(StatementsApiRequestBody{Statement: "SHOW ORGANIZATION ACCOUNTS;", Role: GlobalOrgAdminRole})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(withRole), `"role":"GLOBALORGADMIN"`) {
		t.Errorf("expected role in request body, got %s", withRole)
	}

	noRole, err := json.Marshal(StatementsApiRequestBody{Statement: "SHOW USERS;"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(noRole), "role") {
		t.Errorf("expected role omitted when empty, got %s", noRole)
	}
}

// A 401 must surface Snowflake's response-body reason while keeping codes.Unauthenticated.
func TestListUsers_SurfacesAuthFailureReason(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":"390144","message":"JWT token is invalid. [abc-123]"}`))
	}))
	defer srv.Close()

	client, err := New(srv.URL, JWTConfig{}, srv.Client())
	require.NoError(t, err)

	_, err = client.ListUsers(context.Background(), "", 1)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Contains(t, err.Error(), "JWT token is invalid")
	assert.Contains(t, err.Error(), "390144")
}

func TestStatementsAPIError_Message(t *testing.T) {
	assert.Equal(t, "boom (code 42)", (&statementsAPIError{Code: "42", Msg: "boom"}).Message())
	assert.Equal(t, "boom", (&statementsAPIError{Msg: "boom"}).Message())
	assert.Equal(t, "42", (&statementsAPIError{Code: "42"}).Message())
}
