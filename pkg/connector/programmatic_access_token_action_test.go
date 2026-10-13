package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwe"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func patActionArgs(t *testing.T, userID, requestID string, extra map[string]any) *structpb.Struct {
	t.Helper()
	values := map[string]any{
		patActionUserIDArg:    map[string]any{"resource_type_id": "user", "resource_id": userID},
		patActionRequestIDArg: requestID,
	}
	for key, value := range extra {
		values[key] = value
	}
	args, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatal(err)
	}
	return args
}

func patActionJWEConfig(t *testing.T) (*v2.EncryptionConfig, hpke.PrivateKey, []byte) {
	t.Helper()
	privateKey, err := hpke.MLKEM768X25519().GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	const keyID = "snowflake-action-test"
	publicJWK, err := json.Marshal(map[string]string{
		"kty": "AKP", "alg": jwe.Algorithm, "kid": keyID, "use": "enc",
		"pub": base64.RawURLEncoding.EncodeToString(privateKey.PublicKey().Bytes()),
	})
	if err != nil {
		t.Fatal(err)
	}
	aad := []byte(`{"content_type":"api_key_v2","ticket":"approved-fixture"}`)
	config := v2.EncryptionConfig_builder{
		Provider: jwe.EncryptionProvider,
		KeyId:    keyID,
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey: publicJWK, AdditionalAuthenticatedData: aad,
		}.Build(),
	}.Build()
	return config, privateKey, aad
}

func openPATActionJWE(t *testing.T, encrypted *v2.EncryptedData, privateKey hpke.PrivateKey, aad []byte) []byte {
	t.Helper()
	var message jwe.FlattenedJWE
	if err := json.Unmarshal(encrypted.GetEncryptedBytes(), &message); err != nil {
		t.Fatal(err)
	}
	decodedAAD, err := base64.RawURLEncoding.DecodeString(message.AAD)
	if err != nil || !bytes.Equal(decodedAAD, aad) {
		t.Fatalf("JWE AAD = %q, want %q (err %v)", decodedAAD, aad, err)
	}
	enc, err := base64.RawURLEncoding.DecodeString(message.EncryptedKey)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(message.Ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := hpke.NewRecipient(enc, privateKey, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recipient.Open([]byte(message.Protected+"."+base64.RawURLEncoding.EncodeToString([]byte("wrong-aad"))), ciphertext); err == nil {
		t.Fatal("JWE opened with wrong AAD")
	}
	recipient, err = hpke.NewRecipient(enc, privateKey, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), nil)
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err := recipient.Open([]byte(message.Protected+"."+message.AAD), ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	return plaintext
}

func TestProgrammaticAccessTokenActionSchemaAndGate(t *testing.T) {
	ctx := context.Background()
	for _, enabled := range []bool{false, true} {
		server, err := connectorbuilder.NewConnector(ctx, &Connector{IssueCredentials: enabled})
		if err != nil {
			t.Fatal(err)
		}
		response, err := server.ListActionSchemas(ctx, &v2.ListActionSchemasRequest{ResourceTypeId: userResourceType.Id})
		if err != nil {
			t.Fatal(err)
		}
		if !enabled {
			if len(response.GetSchemas()) != 0 {
				t.Fatalf("disabled action schemas = %#v", response.GetSchemas())
			}
			continue
		}
		if len(response.GetSchemas()) != 1 {
			t.Fatalf("action schemas = %#v", response.GetSchemas())
		}
		schema := response.GetSchemas()[0]
		if schema.GetName() != issueProgrammaticAccessTokenAction || schema.GetResourceTypeId() != userResourceType.Id ||
			len(schema.GetActionType()) != 1 || schema.GetActionType()[0] != v2.ActionType_ACTION_TYPE_DYNAMIC {
			t.Fatalf("schema identity = %#v", schema)
		}
		if len(schema.GetArguments()) != 3 || schema.GetArguments()[0].GetName() != patActionUserIDArg ||
			!schema.GetArguments()[0].GetIsRequired() || !schema.GetArguments()[0].HasResourceIdField() ||
			schema.GetArguments()[1].GetName() != patActionRequestIDArg || !schema.GetArguments()[1].GetIsRequired() ||
			schema.GetArguments()[2].GetName() != patActionExpiresAtArg {
			t.Fatalf("schema arguments = %#v", schema.GetArguments())
		}
		if len(schema.GetReturnTypes()) != 2 || schema.GetReturnTypes()[0].GetName() != patActionIssuedResourceReturn ||
			schema.GetReturnTypes()[0].GetIsSecret() || !schema.GetReturnTypes()[0].HasResourceField() ||
			schema.GetReturnTypes()[1].GetName() != patActionNativeReturnName || !schema.GetReturnTypes()[1].GetIsSecret() ||
			!schema.GetReturnTypes()[1].GetIsRequired() {
			t.Fatalf("schema returns = %#v", schema.GetReturnTypes())
		}
	}
}

func TestProgrammaticAccessTokenActionRejectsInvalidInputBeforeSnowflake(t *testing.T) {
	var statements []string
	provider := newCredentialIssueMockServer(t, "SERVICE", "service_role", true, &statements)
	defer provider.Close()
	client, err := snowflake.New(provider.URL, snowflake.JWTConfig{}, provider.Client())
	if err != nil {
		t.Fatal(err)
	}
	builder := newCredentialUserBuilder(client, secretOptions{})
	for _, tc := range []struct {
		name string
		args *structpb.Struct
	}{
		{name: "missing user", args: patActionArgs(t, "", "ticket-1", nil)},
		{name: "invalid request id", args: patActionArgs(t, "service-user", "bad id", nil)},
		{name: "unknown scopes", args: patActionArgs(t, "service-user", "ticket-1", map[string]any{"scopes": []any{"read"}})},
		{name: "malformed expiry", args: patActionArgs(t, "service-user", "ticket-1", map[string]any{patActionExpiresAtArg: "tomorrow"})},
		{name: "too soon", args: patActionArgs(t, "service-user", "ticket-1", map[string]any{patActionExpiresAtArg: time.Now().Add(2 * time.Hour).Format(time.RFC3339)})},
		{name: "too late", args: patActionArgs(t, "service-user", "ticket-1", map[string]any{patActionExpiresAtArg: time.Now().Add(400 * 24 * time.Hour).Format(time.RFC3339)})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, _, err := builder.issueProgrammaticAccessTokenAction(context.Background(), tc.args); err == nil {
				t.Fatal("invalid action arguments accepted")
			}
		})
	}
	if len(statements) != 0 {
		t.Fatalf("invalid action arguments called Snowflake: %q", statements)
	}
}

func TestProgrammaticAccessTokenActionJWEAndDelete(t *testing.T) {
	var statements []string
	provider := newCredentialIssueMockServer(t, "SERVICE", "service_role", true, &statements)
	defer provider.Close()
	client, err := snowflake.New(provider.URL, snowflake.JWTConfig{}, provider.Client())
	if err != nil {
		t.Fatal(err)
	}
	server, err := connectorbuilder.NewConnector(context.Background(), &Connector{Client: client, IssueCredentials: true})
	if err != nil {
		t.Fatal(err)
	}
	config, privateKey, aad := patActionJWEConfig(t)
	request := v2.InvokeActionRequest_builder{
		Name:              issueProgrammaticAccessTokenAction,
		ResourceTypeId:    userResourceType.Id,
		Args:              patActionArgs(t, "service-user", "request-1", nil),
		EncryptionConfigs: []*v2.EncryptionConfig{config},
		InlineWait:        durationpb.New(5 * time.Second),
	}.Build()
	ctx := context.Background()
	response, err := server.InvokeAction(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() != v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE {
		t.Fatalf("action status = %v", response.GetStatus())
	}
	issued, ok := actions.GetResourceFieldArg(response.GetResponse(), patActionIssuedResourceReturn)
	if !ok || issued.GetId().GetResourceType() != programmaticAccessTokenResourceType.Id ||
		issued.GetId().GetResource() != programmaticAccessTokenID("service-user", "c1-request-1") ||
		issued.GetParentResourceId().GetResource() != "service-user" {
		t.Fatalf("public issued resource = %#v", issued)
	}
	if strings.Contains(response.GetResponse().String(), "redacted") || len(response.GetEncryptedData()) != 1 {
		t.Fatalf("public response leaked token or wrong encrypted count: %#v", response)
	}
	encrypted := response.GetEncryptedData()[0]
	if encrypted.GetProvider() != jwe.EncryptionProvider || encrypted.GetName() != patActionNativeReturnName ||
		encrypted.GetSchema() != "api_key_v2" || len(encrypted.GetKeyIds()) != 1 {
		t.Fatalf("encrypted metadata = %#v", encrypted)
	}
	plaintext := openPATActionJWE(t, encrypted, privateKey, aad)
	const want = `{"key_value":"redacted","provider":"snowflake","header_name":"Authorization"}`
	if string(plaintext) != want {
		t.Fatalf("decrypted native bytes = %q, want %q", plaintext, want)
	}
	trait := &v2.SecretTrait{}
	issuedAnnotations := annotations.Annotations(issued.GetAnnotations())
	if found, err := issuedAnnotations.Pick(trait); err != nil || !found ||
		trait.GetIdentityId().GetResource() != "service-user" || trait.GetExpiresAt() == nil {
		t.Fatalf("issued secret trait = %#v, found %v, err %v", trait, found, err)
	}
	if _, err := newProgrammaticAccessTokenBuilder(client).Delete(ctx, issued.GetId(), nil); err != nil {
		t.Fatalf("delete issued PAT: %v", err)
	}
	if !containsStatement(statements, `ALTER USER "service-user" REMOVE PROGRAMMATIC ACCESS TOKEN "c1-request-1";`) ||
		containsStatement(statements, `REMOVE PROGRAMMATIC ACCESS TOKEN IF EXISTS`) {
		t.Fatalf("delete did not use strict provider statement: %q", statements)
	}
}

func TestProgrammaticAccessTokenActionHonorsApprovedExpiry(t *testing.T) {
	var statements []string
	provider := serveCredentialIssueMock(t, credentialIssueMock{
		userType: "SERVICE", defaultRole: "service_role", roleGranted: true,
		showTokenName: "c1-request-1", liveExpiry: true, statements: &statements,
	})
	defer provider.Close()
	client, err := snowflake.New(provider.URL, snowflake.JWTConfig{}, provider.Client())
	if err != nil {
		t.Fatal(err)
	}
	approvedExpiry := time.Now().UTC().Add(3*24*time.Hour + time.Minute)
	args := patActionArgs(t, "service-user", "request-1", map[string]any{
		patActionExpiresAtArg: approvedExpiry.Format(time.RFC3339Nano),
	})
	public, secret, _, err := newCredentialUserBuilder(client, secretOptions{}).issueProgrammaticAccessTokenAction(context.Background(), args)
	if err != nil {
		t.Fatal(err)
	}
	if len(secret) != 1 || secret[0].GetName() != patActionNativeReturnName {
		t.Fatalf("secret return = %#v", secret)
	}
	issued, ok := actions.GetResourceFieldArg(public, patActionIssuedResourceReturn)
	if !ok {
		t.Fatal("issued resource missing")
	}
	trait := &v2.SecretTrait{}
	issuedAnnotations := annotations.Annotations(issued.GetAnnotations())
	if found, err := issuedAnnotations.Pick(trait); err != nil || !found {
		t.Fatalf("secret trait found=%v, err=%v", found, err)
	}
	if trait.GetExpiresAt().AsTime().After(approvedExpiry) ||
		!containsStatement(statements, "DAYS_TO_EXPIRY = 3") {
		t.Fatalf("approved expiry = %v, trait expiry = %v, statements = %q", approvedExpiry, trait.GetExpiresAt(), statements)
	}
}

func TestProgrammaticAccessTokenActionRejectsMissingJWERecipientBeforeSnowflake(t *testing.T) {
	var statements []string
	provider := newCredentialIssueMockServer(t, "SERVICE", "service_role", true, &statements)
	defer provider.Close()
	client, err := snowflake.New(provider.URL, snowflake.JWTConfig{}, provider.Client())
	if err != nil {
		t.Fatal(err)
	}
	server, err := connectorbuilder.NewConnector(context.Background(), &Connector{Client: client, IssueCredentials: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = server.InvokeAction(context.Background(), v2.InvokeActionRequest_builder{
		Name:           issueProgrammaticAccessTokenAction,
		ResourceTypeId: userResourceType.Id,
		Args:           patActionArgs(t, "service-user", "request-1", nil),
	}.Build())
	if err == nil || !strings.Contains(err.Error(), "encryption config") {
		t.Fatalf("missing recipient error = %v", err)
	}
	if len(statements) != 0 {
		t.Fatalf("missing recipient contacted Snowflake: %q", statements)
	}
}

func TestProgrammaticAccessTokenActionPendingStatusRetainsEncryptedResult(t *testing.T) {
	var statements []string
	provider := serveCredentialIssueMock(t, credentialIssueMock{
		userType: "SERVICE", defaultRole: "service_role", roleGranted: true,
		showTokenName: "c1-request-1", preflightDelay: 100 * time.Millisecond, statements: &statements,
	})
	defer provider.Close()
	client, err := snowflake.New(provider.URL, snowflake.JWTConfig{}, provider.Client())
	if err != nil {
		t.Fatal(err)
	}
	server, err := connectorbuilder.NewConnector(context.Background(), &Connector{Client: client, IssueCredentials: true})
	if err != nil {
		t.Fatal(err)
	}
	config, privateKey, aad := patActionJWEConfig(t)
	ctx := context.Background()
	response, err := server.InvokeAction(ctx, v2.InvokeActionRequest_builder{
		Name: issueProgrammaticAccessTokenAction, ResourceTypeId: userResourceType.Id,
		Args:              patActionArgs(t, "service-user", "request-1", nil),
		EncryptionConfigs: []*v2.EncryptionConfig{config}, InlineWait: durationpb.New(time.Millisecond),
	}.Build())
	if err != nil {
		t.Fatal(err)
	}
	if response.GetId() == "" || !actions.IsInFlight(response.GetStatus()) {
		t.Fatalf("want in-flight action with ID, got %#v", response)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status, err := server.GetActionStatus(ctx, v2.GetActionStatusRequest_builder{Id: response.GetId()}.Build())
		if err != nil {
			t.Fatal(err)
		}
		if status.GetStatus() == v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE {
			if len(status.GetEncryptedData()) != 1 || status.GetResponse() == nil {
				t.Fatalf("settled status lost action outputs: %#v", status)
			}
			if got := string(openPATActionJWE(t, status.GetEncryptedData()[0], privateKey, aad)); got != `{"key_value":"redacted","provider":"snowflake","header_name":"Authorization"}` {
				t.Fatalf("status plaintext = %q", got)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("action did not settle")
}

func TestProgrammaticAccessTokenDeleteStaleNameNeedsReconciliation(t *testing.T) {
	var statements []string
	provider := serveCredentialIssueMock(t, credentialIssueMock{
		userType: "SERVICE", defaultRole: "service_role", roleGranted: true,
		showTokenName: "renamed-token", removeMissing: true, statements: &statements,
	})
	defer provider.Close()
	client, err := snowflake.New(provider.URL, snowflake.JWTConfig{}, provider.Client())
	if err != nil {
		t.Fatal(err)
	}
	id := v2.ResourceId_builder{ResourceType: programmaticAccessTokenResourceType.Id, Resource: programmaticAccessTokenID("service-user", "c1-request-1")}.Build()
	_, err = newProgrammaticAccessTokenBuilder(client).Delete(context.Background(), id, nil)
	if err == nil || !strings.Contains(err.Error(), "manual reconciliation") {
		t.Fatalf("stale name delete error = %v, want uncertain outcome", err)
	}
	if !containsStatement(statements, `ALTER USER "service-user" REMOVE PROGRAMMATIC ACCESS TOKEN "c1-request-1";`) ||
		containsStatement(statements, "SHOW USER PROGRAMMATIC ACCESS TOKENS") {
		t.Fatalf("stale name used an unsafe or privilege-expanding revoke path: %q", statements)
	}
}
