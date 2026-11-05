package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

// describeUserBody is a minimal DESCRIBE USER result containing every property GetUser reads.
const describeUserBody = `{"resultSetMetaData":{"numRows":0},"code":"090001","statementHandle":"h1",
"data":[["NAME","alice"],["LOGIN_NAME","alice"],["DISPLAY_NAME","alice"],["FIRST_NAME",""],["LAST_NAME",""],
["EMAIL",""],["DISABLED","false"],["SNOWFLAKE_LOCK","false"],["DEFAULT_ROLE",""],["TYPE","PERSON"],
["HAS_MFA","false"],["COMMENT",""]]}`

func createAccountWith(t *testing.T, opts *v2.LocalCredentialOptions) (map[string]any, int) {
	t.Helper()
	var createBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && r.URL.Path == "/api/v2/users" {
			raw, _ := io.ReadAll(r.Body)
			require.NoError(t, json.Unmarshal(raw, &createBody))
			_, _ = w.Write([]byte(`{"status":"created"}`))
			return
		}
		// Any statement (DESCRIBE USER) gets the canned describe result.
		resp := strings.Replace(describeUserBody, `"numRows":0`, `"numRows":12,"rowType":[{"name":"property","type":"text"},{"name":"value","type":"text"}]`, 1)
		_, _ = w.Write([]byte(resp))
	}))
	defer srv.Close()

	client, err := snowflake.New(srv.URL, snowflake.JWTConfig{}, srv.Client())
	require.NoError(t, err)
	b := newUserBuilder(client, secretOptions{})

	profile, err := structpb.NewStruct(map[string]any{"name": "alice"})
	require.NoError(t, err)
	_, plaintext, _, err := b.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, opts)
	require.NoError(t, err)
	return createBody, len(plaintext)
}

func TestCreateAccountNoPassword(t *testing.T) {
	body, nPlaintext := createAccountWith(t, &v2.LocalCredentialOptions{
		Options: &v2.LocalCredentialOptions_NoPassword_{NoPassword: &v2.LocalCredentialOptions_NoPassword{}},
	})
	require.NotNil(t, body)
	require.NotContains(t, body, "password")
	require.NotContains(t, body, "must_change_password")
	require.Zero(t, nPlaintext)
}

func TestCreateAccountRandomPasswordStillSendsPassword(t *testing.T) {
	body, nPlaintext := createAccountWith(t, &v2.LocalCredentialOptions{
		Options: &v2.LocalCredentialOptions_RandomPassword_{RandomPassword: &v2.LocalCredentialOptions_RandomPassword{Length: 16}},
	})
	require.NotEmpty(t, body["password"])
	require.Equal(t, 1, nPlaintext)
}

func TestCreateAccountCapabilityDetailsIncludesNoPassword(t *testing.T) {
	b := &userBuilder{}
	d, _, err := b.CreateAccountCapabilityDetails(context.Background())
	require.NoError(t, err)
	require.Contains(t, d.SupportedCredentialOptions, v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_NO_PASSWORD)
	require.Equal(t, v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_RANDOM_PASSWORD, d.PreferredCredentialOption)
}
