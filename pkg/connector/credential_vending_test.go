package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwe"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/require"
)

// These drive the whole connector through the SDK server rather than calling the
// credential user builder directly, so the SDK's own guards run: the
// credential-issuer registration check at build time, request validation, the
// exactly-one-plaintext cardinality the JWE profile imposes, and the encryption
// fan-out. The recipient is a real X-Wing key the test holds the private half of,
// so the round trip is proved rather than assumed.

const (
	vendingKeyID = "c1-test-recipient"
	// vendingRequestID is what C1 passes as the correlation id. The connector
	// prefixes it with c1- to name the token, so the mock has to disclose the same
	// name or the read-back reports the token as missing.
	vendingRequestID = "request-1"
	vendingTokenName = "c1-" + vendingRequestID
)

// jweRecipient is one X-Wing recipient plus the private key needed to read back
// what the connector encrypts to it.
type jweRecipient struct {
	config *v2.EncryptionConfig
	priv   hpke.PrivateKey
}

// newJWERecipient builds the encryption config C1 sends for the baton/jwe/v1
// profile: an X-Wing public JWK, plus the same key id on the config and inside
// the JWK.
func newJWERecipient(t *testing.T) jweRecipient {
	t.Helper()
	priv, err := hpke.MLKEM768X25519().GenerateKey()
	require.NoError(t, err)

	pubJWK, err := json.Marshal(map[string]string{
		"kty": "AKP",
		"alg": jwe.Algorithm,
		"kid": vendingKeyID,
		"use": "enc",
		"pub": base64.RawURLEncoding.EncodeToString(priv.PublicKey().Bytes()),
	})
	require.NoError(t, err)

	return jweRecipient{
		config: v2.EncryptionConfig_builder{
			Provider: jwe.EncryptionProvider,
			KeyId:    vendingKeyID,
			JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
				PubKey: pubJWK,
			}.Build(),
		}.Build(),
		priv: priv,
	}
}

// open decrypts the flattened JWE the provider emits and returns the plaintext.
// The HPKE additional data is the serialized protected header and the encoded aad
// joined by a dot -- the exact bytes Encrypt authenticated -- so this reads the
// wire form as transmitted rather than a re-serialization of the same members.
func (r jweRecipient) open(t *testing.T, data *v2.EncryptedData) string {
	t.Helper()
	require.Equal(t, jwe.EncryptionProvider, data.GetProvider())
	require.Equal(t, []string{vendingKeyID}, data.GetKeyIds())

	var message jwe.FlattenedJWE
	require.NoError(t, json.Unmarshal(data.GetEncryptedBytes(), &message))

	protected, err := base64.RawURLEncoding.DecodeString(message.Protected)
	require.NoError(t, err)
	var header struct {
		Algorithm string `json:"alg"`
		KeyID     string `json:"kid"`
	}
	require.NoError(t, json.Unmarshal(protected, &header))
	require.Equal(t, jwe.Algorithm, header.Algorithm)
	require.Equal(t, vendingKeyID, header.KeyID)

	encapsulated, err := base64.RawURLEncoding.DecodeString(message.EncryptedKey)
	require.NoError(t, err)
	ciphertext, err := base64.RawURLEncoding.DecodeString(message.Ciphertext)
	require.NoError(t, err)

	recipient, err := hpke.NewRecipient(encapsulated, r.priv, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), nil)
	require.NoError(t, err)
	plaintext, err := recipient.Open([]byte(message.Protected+"."+message.AAD), ciphertext)
	require.NoError(t, err)
	return string(plaintext)
}

// newVendingServer starts the credential-issue mock and wires the real connector
// through the SDK, which is where the issuer-registration and deleter checks live.
func newVendingServer(t *testing.T, mock credentialIssueMock) (types.ConnectorServer, *[]string) {
	t.Helper()
	statements := &[]string{}
	mock.statements = statements
	upstream := serveCredentialIssueMock(t, mock)
	t.Cleanup(upstream.Close)

	client, err := snowflake.New(upstream.URL, snowflake.JWTConfig{}, upstream.Client())
	require.NoError(t, err)
	server, err := connectorbuilder.NewConnector(context.Background(), &Connector{
		Client:           client,
		IssueCredentials: true,
	})
	require.NoError(t, err)
	return server, statements
}

// issueVendingRequest is the request C1 sends when it vends a token to a
// recipient: the user is the identity, the secret comes back as a programmatic
// access token, and the encryption config names the recipient.
func issueVendingRequest(recipient jweRecipient) *v2.IssueCredentialRequest {
	return v2.IssueCredentialRequest_builder{
		IdentityId: v2.ResourceId_builder{
			ResourceType: userResourceType.Id,
			Resource:     "service-user",
		}.Build(),
		CredentialOptions: v2.CredentialIssueOptions_builder{
			SecretResourceTypeId: programmaticAccessTokenResourceType.Id,
			Token:                v2.CredentialIssueOptions_Token_builder{}.Build(),
		}.Build(),
		EncryptionConfigs: []*v2.EncryptionConfig{recipient.config},
		RequestId:         vendingRequestID,
	}.Build()
}

// TestCredentialVendingAdvertisesJWEEncryption pins the capability C1 reads to
// decide it may hand the connector an X-Wing recipient at all. The SDK adds it
// for any credential issuer; this fails if that stops being true.
func TestCredentialVendingAdvertisesJWEEncryption(t *testing.T) {
	server, err := connectorbuilder.NewConnector(context.Background(), &Connector{IssueCredentials: true})
	require.NoError(t, err)
	response, err := server.GetMetadata(context.Background(), &v2.ConnectorServiceGetMetadataRequest{})
	require.NoError(t, err)

	capabilities := response.GetMetadata().GetCapabilities()
	require.Contains(t, capabilities.GetConnectorCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ISSUE)
	require.Contains(t, capabilities.GetConnectorCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ENCRYPTION_JWE_XWING_V1)

	for _, capability := range capabilities.GetResourceTypeCapabilities() {
		if capability.GetResourceType().GetId() != userResourceType.Id {
			continue
		}
		require.Contains(t, capability.GetCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ENCRYPTION_JWE_XWING_V1)
		require.NotNil(t, capability.GetCredentialIssue())
		return
	}
	t.Fatal("user resource type capability not found")
}

// TestE2EIssueCredentialVendsJWECiphertext issues a token through the SDK with a
// JWE recipient and proves the value reaches C1 only as ciphertext that the
// recipient's private key opens back to the token Snowflake disclosed.
func TestE2EIssueCredentialVendsJWECiphertext(t *testing.T) {
	const tokenSecret = "snowflake-pat-vending-secret"
	server, _ := newVendingServer(t, credentialIssueMock{
		userType: "SERVICE", defaultRole: "service_role", roleGranted: true,
		showTokenName: vendingTokenName, tokenSecret: tokenSecret,
	})
	recipient := newJWERecipient(t)

	response, err := server.IssueCredential(context.Background(), issueVendingRequest(recipient))
	require.NoError(t, err)

	// The SDK enforced the shape on the way out: the advertised secret type, the
	// discoverable mode C1 drops VIRTUAL descriptors in favour of, and the
	// correlation id echoed back.
	require.Equal(t, programmaticAccessTokenResourceType.Id, response.GetSecret().GetId().GetResourceType())
	require.Equal(t, v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE, response.GetResourceMode())
	require.Equal(t, vendingRequestID, response.GetRequestId())

	// One plaintext in, one ciphertext out. The JWE profile refuses any other
	// count, so a second value would fail issuance rather than ship unencrypted.
	require.Len(t, response.GetEncryptedData(), 1)
	encrypted := response.GetEncryptedData()[0]
	require.NotContains(t, string(encrypted.GetEncryptedBytes()), tokenSecret)

	require.Equal(t, tokenSecret, recipient.open(t, encrypted))

	// The vended resource has to be the one a later sync can find again, or C1
	// loses its revocation handle on the credential it just handed out.
	userName, tokenName, err := parseProgrammaticAccessTokenID(response.GetSecret().GetId().GetResource())
	require.NoError(t, err)
	require.Equal(t, "service-user", userName)
	require.Equal(t, vendingTokenName, tokenName)
}

// TestE2EIssueCredentialRejectsUnusableJWERecipient covers the guard that only
// exists inside the SDK: a recipient whose public key cannot be encrypted to is
// refused before the connector mints anything, so no live token is orphaned.
func TestE2EIssueCredentialRejectsUnusableJWERecipient(t *testing.T) {
	server, statements := newVendingServer(t, credentialIssueMock{
		userType: "SERVICE", defaultRole: "service_role", roleGranted: true,
		showTokenName: vendingTokenName,
	})

	broken := v2.EncryptionConfig_builder{
		Provider: jwe.EncryptionProvider,
		KeyId:    vendingKeyID,
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{
			PubKey: []byte(`{"kty":"AKP","alg":"` + jwe.Algorithm + `","kid":"` + vendingKeyID + `","use":"enc","pub":"AAAA"}`),
		}.Build(),
	}.Build()
	request := issueVendingRequest(newJWERecipient(t))
	request.SetEncryptionConfigs([]*v2.EncryptionConfig{broken})

	_, err := server.IssueCredential(context.Background(), request)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "encryption config"),
		"expected the rejection to name the encryption config, got %v", err)

	for _, statement := range *statements {
		require.NotContains(t, statement, "ADD PROGRAMMATIC ACCESS TOKEN",
			"a rejected recipient must not leave a live token behind")
	}
}
