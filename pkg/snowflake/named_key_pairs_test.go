package snowflake

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListNamedKeyPairsRawResponseGetKeyPairsToleratesOptionalColumns(t *testing.T) {
	response := &ListNamedKeyPairsRawResponse{
		StatementsApiResponseBase: StatementsApiResponseBase{
			ResultSetMetadata: ResultSetMetadata{RowTypes: []RowType{
				{Name: "name", Type: rowTypeString},
				{Name: "user_name", Type: rowTypeString},
				{Name: "fingerprint", Type: rowTypeString},
				{Name: "status", Type: rowTypeString},
				{Name: "created_on", Type: rowTypeString},
			}},
			Data: [][]string{{"c1_key", "svc_user", "SHA256:test", "ACTIVE", "1784682000.000000000"}},
		},
	}

	keyPairs, err := response.GetKeyPairs()
	require.NoError(t, err)
	require.Len(t, keyPairs, 1)
	require.Equal(t, "c1_key", keyPairs[0].Name)
	require.Equal(t, "svc_user", keyPairs[0].UserName)
	require.False(t, keyPairs[0].CreatedOn.IsZero())
	require.True(t, keyPairs[0].LastUsedOn.IsZero())
	require.True(t, keyPairs[0].ExpiresAt.IsZero())
}
