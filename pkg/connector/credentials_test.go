package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lastUsedEpoch is 2026-09-30T00:00:00Z.
const lastUsedEpoch = "1790726400.000000000"

type secretsMock struct {
	denyCredentials bool
}

func serveSecretsMock(t *testing.T, mock secretsMock) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var request snowflake.StatementsApiRequestBody
		require.NoError(t, json.Unmarshal(body, &request))
		w.Header().Set("Content-Type", "application/json")

		var rowTypes []map[string]string
		var data [][]*string
		switch {
		case strings.HasPrefix(request.Statement, "DESCRIBE USER"):
			rowTypes = textColumns("property", "value", "default", "description")
			data = [][]*string{
				{ptr("NAME"), ptr("ALICE"), nil, nil},
				{ptr("RSA_PUBLIC_KEY_FP"), ptr("SHA256:key1="), nil, nil},
				{ptr("RSA_PUBLIC_KEY_LAST_SET_TIME"), ptr("2025-04-09 10:00:00.000"), nil, nil},
				{ptr("RSA_PUBLIC_KEY_2_FP"), ptr("SHA256:key2="), nil, nil},
				{ptr("RSA_PUBLIC_KEY_2_LAST_SET_TIME"), ptr("2025-04-09 10:00:00.000"), nil, nil},
			}
		case strings.HasPrefix(request.Statement, "SHOW USER PROGRAMMATIC ACCESS TOKENS"):
			rowTypes = []map[string]string{
				{"name": "name", "type": "text"},
				{"name": "expires_at", "type": "timestamp_ltz"},
				{"name": "created_on", "type": "timestamp_ltz"},
			}
			data = [][]*string{
				{ptr("USED_TOKEN"), ptr("1893456000"), ptr("1744192800")},
				{ptr("UNUSED_TOKEN"), ptr("1893456000"), ptr("1744192800")},
			}
		case strings.Contains(request.Statement, "ACCOUNT_USAGE.CREDENTIALS"):
			if mock.denyCredentials {
				w.WriteHeader(http.StatusUnprocessableEntity)
				_ = json.NewEncoder(w).Encode(map[string]string{
					"code":    "002003",
					"message": "SQL compilation error: Database 'SNOWFLAKE' does not exist or not authorized.",
				})
				return
			}
			rowTypes = append(textColumns("USER_NAME", "TYPE", "NAME", "PUBLIC_KEY_FP"),
				map[string]string{"name": "LAST_USED_ON", "type": "timestamp_ltz"})
			data = [][]*string{
				{ptr("ALICE"), ptr("KEYPAIR"), ptr("RSA_PUBLIC_KEY"), ptr("key1="), ptr(lastUsedEpoch)},
				{ptr("ALICE"), ptr("KEYPAIR"), ptr("RSA_PUBLIC_KEY_2"), ptr("key2="), nil},
				{ptr("ALICE"), ptr("PAT"), ptr("USED_TOKEN"), nil, ptr(lastUsedEpoch)},
			}
		default:
			t.Errorf("unexpected statement: %s", request.Statement)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"resultSetMetadata": map[string]any{"numRows": len(data), "rowType": rowTypes},
			"data":              data,
		})
	}))
}

func textColumns(names ...string) []map[string]string {
	columns := make([]map[string]string, 0, len(names))
	for _, name := range names {
		columns = append(columns, map[string]string{"name": name, "type": "text"})
	}
	return columns
}

func ptr(s string) *string { return &s }

func secretTrait(t *testing.T, resource *v2.Resource) *v2.SecretTrait {
	t.Helper()
	trait := &v2.SecretTrait{}
	annos := annotations.Annotations(resource.GetAnnotations())
	ok, err := annos.Pick(trait)
	require.NoError(t, err)
	require.True(t, ok)
	return trait
}

func TestNormalizeFp(t *testing.T) {
	assert.Equal(t, "abc+/=", normalizeFp(" SHA256:abc+/= "))
	assert.Equal(t, "abc+/=", normalizeFp("sha256:abc+/="))
	assert.Equal(t, "AbC=", normalizeFp("AbC="))
	assert.Empty(t, normalizeFp(""))
}

func TestProgrammaticAccessTokenListSetsLastUsedFromCredentials(t *testing.T) {
	server := serveSecretsMock(t, secretsMock{})
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	require.NoError(t, err)

	userID := v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "ALICE"}.Build()
	resources, _, err := newProgrammaticAccessTokenBuilder(client).List(context.Background(), userID, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Len(t, resources, 2)

	used := secretTrait(t, resources[0])
	require.True(t, used.HasLastUsedAt())
	assert.Equal(t, time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), used.GetLastUsedAt().AsTime())
	require.True(t, resources[0].HasCreatedAt())
	assert.Equal(t, time.Date(2025, 4, 9, 10, 0, 0, 0, time.UTC), resources[0].GetCreatedAt().AsTime())

	unused := secretTrait(t, resources[1])
	assert.False(t, unused.HasLastUsedAt())
}

func TestProgrammaticAccessTokenListEmitsTokensWhenCredentialsAreDenied(t *testing.T) {
	server := serveSecretsMock(t, secretsMock{denyCredentials: true})
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	require.NoError(t, err)

	userID := v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "ALICE"}.Build()
	resources, _, err := newProgrammaticAccessTokenBuilder(client).List(context.Background(), userID, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Len(t, resources, 2)
	for _, resource := range resources {
		assert.Equal(t, programmaticAccessTokenID("ALICE", resource.GetDisplayName()), resource.GetId().GetResource())
		assert.False(t, secretTrait(t, resource).HasLastUsedAt())
	}
}
