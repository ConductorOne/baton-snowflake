package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestSnowflakeDeclaredSecretProof exports the real action schema and SDK JWE
// response when invoked by the C1 private proof. Only provider HTTP is a fixture.
func TestSnowflakeDeclaredSecretProof(t *testing.T) {
	const expected = `{"key_value":"redacted","provider":"snowflake","header_name":"Authorization"}`
	var statements []string
	provider := newCredentialIssueMockServer(t, "SERVICE", "service_role", true, &statements)
	defer provider.Close()
	client, err := snowflake.New(provider.URL, snowflake.JWTConfig{}, provider.Client())
	require.NoError(t, err)
	ctx := context.Background()
	sdk, err := connectorbuilder.NewConnector(ctx, &Connector{Client: client, IssueCredentials: true})
	require.NoError(t, err)
	schemas, err := sdk.ListActionSchemas(ctx, v2.ListActionSchemasRequest_builder{ResourceTypeId: userResourceType.Id}.Build())
	require.NoError(t, err)
	require.Len(t, schemas.GetSchemas(), 1)
	schema := schemas.GetSchemas()[0]
	secrets := 0
	for _, field := range schema.GetReturnTypes() {
		if !field.GetIsSecret() {
			continue
		}
		secrets++
		require.True(t, field.HasSecretContentType(), "secret return %s", field.GetName())
		require.Equal(t, "api_key_v2", field.GetSecretContentType(), "secret return %s", field.GetName())
	}
	require.Equal(t, 1, secrets, "action must expose exactly one secret return")
	config, key, aad := patActionJWEConfig(t)
	request := v2.InvokeActionRequest_builder{Name: schema.GetName(), ResourceTypeId: schema.GetResourceTypeId(),
		Args: patActionArgs(t, "service-user", "request-1", nil), EncryptionConfigs: []*v2.EncryptionConfig{config},
		InlineWait: durationpb.New(5 * time.Second)}.Build()
	for _, wrongAAD := range []string{"", "opaque-old-context", `{"purpose":"c1/fk-credential-delivery","version":1,"content_type":"generic"}`,
		`{"purpose":"c1/fk-credential-delivery","version":1,"content_type":"other_type"}`,
		`{"purpose":"wrong","version":1,"content_type":"api_key_v2"}`, `{"purpose":"c1/fk-credential-delivery","version":2,"content_type":"api_key_v2"}`} {
		wrong := proto.Clone(request).(*v2.InvokeActionRequest)
		wrong.GetEncryptionConfigs()[0].GetJwkPublicKeyConfig().SetAdditionalAuthenticatedData([]byte(wrongAAD))
		_, err := sdk.InvokeAction(ctx, wrong)
		require.Error(t, err)
		require.Empty(t, statements, "downgrade refusal precedes all provider SQL")
	}
	var input struct {
		Operation string          `json:"operation"`
		Request   json.RawMessage `json:"request"`
	}
	external := os.Getenv("CONNECTOR_SECRET_PROOF_INPUT")
	if external != "" {
		encoded, err := os.ReadFile(external) //nolint:gosec // test-only port: path is supplied by the proof harness environment
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(encoded, &input))
		if input.Operation == "invoke" {
			request = new(v2.InvokeActionRequest)
			require.NoError(t, protojson.Unmarshal(input.Request, request))
			request.SetArgs(patActionArgs(t, "service-user", "request-1", nil))
			request.SetInlineWait(durationpb.New(5 * time.Second))
		}
	}
	output := map[string]any{"expected_payload_base64": base64.StdEncoding.EncodeToString([]byte(expected))}
	schemaJSON, err := protojson.Marshal(schema)
	require.NoError(t, err)
	output["schema"] = json.RawMessage(schemaJSON)
	if input.Operation != "schema" {
		response, err := sdk.InvokeAction(ctx, request)
		require.NoError(t, err)
		require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, response.GetStatus())
		require.Len(t, response.GetEncryptedData(), 1)
		require.NotContains(t, response.GetResponse().String(), "redacted")
		creates := 0
		for _, statement := range statements {
			if strings.Contains(statement, " ADD PROGRAMMATIC ACCESS TOKEN ") {
				creates++
			}
			require.NotContains(t, statement, " REMOVE PROGRAMMATIC ACCESS TOKEN ")
		}
		require.Equal(t, 1, creates)
		if external == "" {
			require.Equal(t, []byte(expected), openPATActionJWE(t, response.GetEncryptedData()[0], key, aad))
		}
		encoded, err := protojson.Marshal(response)
		require.NoError(t, err)
		output["response"] = json.RawMessage(encoded)
	}
	if path := os.Getenv("CONNECTOR_SECRET_PROOF_RESULT"); path != "" {
		encoded, err := json.MarshalIndent(output, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, encoded, 0o600)) //nolint:gosec // test-only port: path is supplied by the proof harness environment
	}
}
