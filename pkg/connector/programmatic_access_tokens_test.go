package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	ageprovider "github.com/conductorone/baton-sdk/pkg/crypto/providers/age"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// legacyCredentialUserBuilder models the released v0.2.1 descriptor. The SDK
// must reject a new API_KEY selector before calling its Issue method.
type legacyCredentialUserBuilder struct{ *credentialUserBuilder }

func (o *legacyCredentialUserBuilder) IssueCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error) {
	details, annotations, err := o.credentialUserBuilder.IssueCapabilityDetails(ctx)
	if err != nil {
		return nil, nil, err
	}
	details.SetOptions(details.GetOptions()[:1])
	return details, annotations, nil
}

type legacyCredentialConnector struct{ *Connector }

func (o *legacyCredentialConnector) ResourceSyncers(context.Context) []connectorbuilder.ResourceSyncerV2 {
	return []connectorbuilder.ResourceSyncerV2{
		&legacyCredentialUserBuilder{newCredentialUserBuilder(o.Client, secretOptions{})},
		newProgrammaticAccessTokenBuilder(o.Client),
	}
}

func TestCredentialIssueNativeAPIKeySDKFixtureAndLegacyRefusal(t *testing.T) {
	const sampleValue = "pat<>&\"\\\n\x01"
	const wantNativeBytes = "{\"key_value\":\"pat<>&\\\"\\\\\\n\\u0001\",\"provider\":\"snowflake\",\"header_name\":\"Authorization\"}"
	var statements []string
	provider := serveCredentialIssueMock(t, credentialIssueMock{
		userType: "SERVICE", defaultRole: "service_role", roleGranted: true,
		showTokenName: "c1-request-1", tokenSecret: sampleValue, statements: &statements,
	})
	defer provider.Close()
	client, err := snowflake.New(provider.URL, snowflake.JWTConfig{}, provider.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate age identity: %v", err)
	}
	encryption := []*v2.EncryptionConfig{v2.EncryptionConfig_builder{
		Provider: ageprovider.EncryptionProviderAge,
		AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{
			Recipient: identity.Recipient().String(),
		}.Build(),
	}.Build()}
	request := func(native bool) *v2.IssueCredentialRequest {
		options := v2.CredentialIssueOptions_builder{SecretResourceTypeId: programmaticAccessTokenResourceType.Id}
		if native {
			options.ApiKey = v2.CredentialIssueOptions_ApiKey_builder{}.Build()
		} else {
			options.Token = v2.CredentialIssueOptions_Token_builder{}.Build()
		}
		return v2.IssueCredentialRequest_builder{
			IdentityId:        v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "service-user"}.Build(),
			CredentialOptions: options.Build(),
			EncryptionConfigs: encryption,
			RequestId:         "request-1",
		}.Build()
	}
	ctx := context.Background()
	oldServer, err := connectorbuilder.NewConnector(ctx, &legacyCredentialConnector{&Connector{Client: client, IssueCredentials: true}})
	if err != nil {
		t.Fatalf("old connector: %v", err)
	}
	if _, err := oldServer.IssueCredential(ctx, request(true)); err == nil || !strings.Contains(err.Error(), "not advertised") {
		t.Fatalf("old descriptor API_KEY response = %v, want pre-mint refusal", err)
	}
	if len(statements) != 0 {
		t.Fatalf("old descriptor contacted Snowflake: %q", statements)
	}

	newServer, err := connectorbuilder.NewConnector(ctx, &Connector{Client: client, IssueCredentials: true})
	if err != nil {
		t.Fatalf("new connector: %v", err)
	}
	for _, tc := range []struct {
		name     string
		native   bool
		wantName string
		wantRaw  bool
	}{
		{name: "native API_KEY", native: true, wantName: "api_key_v2"},
		{name: "released raw TOKEN", wantName: "token", wantRaw: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response, err := newServer.IssueCredential(ctx, request(tc.native))
			if err != nil {
				t.Fatalf("IssueCredential() error = %v", err)
			}
			if response.GetSecret().GetId().GetResourceType() != programmaticAccessTokenResourceType.Id ||
				response.GetSecret().GetId().GetResource() != programmaticAccessTokenID("service-user", "c1-request-1") {
				t.Fatalf("secret handle = %#v", response.GetSecret().GetId())
			}
			if len(response.GetEncryptedData()) != 1 || response.GetEncryptedData()[0].GetName() != tc.wantName {
				t.Fatalf("encrypted data = %#v", response.GetEncryptedData())
			}
			reader, err := age.Decrypt(bytes.NewReader(response.GetEncryptedData()[0].GetEncryptedBytes()), identity)
			if err != nil {
				t.Fatalf("decrypt SDK ciphertext: %v", err)
			}
			plaintext, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("read plaintext: %v", err)
			}
			if tc.wantRaw {
				if string(plaintext) != sampleValue {
					t.Fatalf("raw TOKEN plaintext = %q", plaintext)
				}
			} else {
				if string(plaintext) != wantNativeBytes {
					t.Fatalf("native Issue bytes = %q, want %q", plaintext, wantNativeBytes)
				}
				var fields map[string]any
				if err := json.Unmarshal(plaintext, &fields); err != nil {
					t.Fatalf("native plaintext is not JSON: %v", err)
				}
				if len(fields) != 3 || fields["key_value"] != sampleValue || fields["provider"] != "snowflake" || fields["header_name"] != "Authorization" {
					t.Fatalf("native plaintext fields = %#v", fields)
				}
			}
		})
	}
}

func TestCredentialUserBuilderIssueServiceUserUsesDefaultRoleRestriction(t *testing.T) {
	var statements []string
	server := newCredentialIssueMockServer(t, "SERVICE", "service_role", true, &statements)
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = newCredentialUserBuilder(client, secretOptions{}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
		IdentityID: v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "service-user"}.Build(),
		RequestID:  "request-1",
	})
	if err != nil {
		t.Fatalf("Issue() error = %v", err)
	}
	if !containsStatement(statements, `ROLE_RESTRICTION = "service_role"`) {
		t.Fatalf("issuance statement did not restrict the token to the service user's default role: %q", statements)
	}
}

func TestCredentialUserBuilderIssueServiceUserWithUnassignedDefaultRoleFailsBeforeTokenCreation(t *testing.T) {
	var statements []string
	server := newCredentialIssueMockServer(t, "SERVICE", "service_role", false, &statements)
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = newCredentialUserBuilder(client, secretOptions{}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
		IdentityID: v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "service-user"}.Build(),
		RequestID:  "request-1",
	})
	if err == nil || !strings.Contains(err.Error(), "is not granted to the user") {
		t.Fatalf("Issue() error = %v, want actionable missing-role error", err)
	}
	if containsStatement(statements, "ADD PROGRAMMATIC ACCESS TOKEN") {
		t.Fatalf("Issue() created a token despite having no suitable role: %q", statements)
	}
}

type credentialIssueMock struct {
	userType    string
	defaultRole string
	roleGranted bool
	// showTokenName lets a test make SHOW return a name that does not match the token
	// just created, which is the "provider did not return the token" failure path.
	showTokenName string
	// tokenSecret permits exact-byte fixture tests with JSON-sensitive characters.
	tokenSecret string
	// denyPrefix makes every statement with this prefix answer 422/003001, the shape
	// Snowflake uses for an access-control denial.
	denyPrefix string
	// preflightDelay is spent inside DESCRIBE USER, standing in for the round-trip
	// latency between sampling the clock and issuing the ALTER USER.
	preflightDelay time.Duration
	// liveExpiry makes SHOW derive expires_at from the mock's own clock and the
	// statement's DAYS_TO_EXPIRY, the way Snowflake does, instead of a fixed instant.
	liveExpiry bool
	statements *[]string
}

func newCredentialIssueMockServer(t *testing.T, userType, defaultRole string, roleGranted bool, statements *[]string) *httptest.Server {
	return newCredentialIssueMockServerWithShowName(t, userType, defaultRole, roleGranted, "c1-request-1", statements)
}

func newCredentialIssueMockServerWithShowName(t *testing.T, userType, defaultRole string, roleGranted bool, showTokenName string, statements *[]string) *httptest.Server {
	return serveCredentialIssueMock(t, credentialIssueMock{
		userType: userType, defaultRole: defaultRole, roleGranted: roleGranted,
		showTokenName: showTokenName, statements: statements,
	})
}

func serveCredentialIssueMock(t *testing.T, mock credentialIssueMock) *httptest.Server {
	t.Helper()
	var days int64 = 1
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var request snowflake.StatementsApiRequestBody
		if err := json.Unmarshal(body, &request); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		*mock.statements = append(*mock.statements, request.Statement)
		w.Header().Set("Content-Type", "application/json")
		if mock.denyPrefix != "" && strings.HasPrefix(request.Statement, mock.denyPrefix) {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code":    "003001",
				"message": "SQL access control error: Insufficient privileges to operate on user",
			})
			return
		}

		switch {
		case strings.HasPrefix(request.Statement, "DESCRIBE USER"):
			time.Sleep(mock.preflightDelay)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"resultSetMetadata": map[string]any{"numRows": 12},
				"data": [][]string{
					{"NAME", "service-user"}, {"LOGIN_NAME", "service-user"}, {"DISPLAY_NAME", "Service User"},
					{"FIRST_NAME", ""}, {"LAST_NAME", ""}, {"EMAIL", ""}, {"DISABLED", "false"},
					{"SNOWFLAKE_LOCK", "false"}, {"DEFAULT_ROLE", mock.defaultRole}, {"TYPE", mock.userType},
					{"HAS_MFA", "false"}, {"COMMENT", ""},
				},
			})
		case strings.HasPrefix(request.Statement, "SHOW GRANTS TO USER"):
			data := [][]string{}
			if mock.roleGranted {
				data = append(data, []string{"ROLE", mock.defaultRole})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"resultSetMetadata": map[string]any{
					"numRows": len(data),
					"rowType": []map[string]any{{"name": "granted_on", "type": "text"}, {"name": "name", "type": "text"}},
				},
				"data": data,
			})
		case strings.Contains(request.Statement, "ADD PROGRAMMATIC ACCESS TOKEN"):
			secret := mock.tokenSecret
			if secret == "" {
				secret = "redacted"
			}
			_, clause, found := strings.Cut(request.Statement, "DAYS_TO_EXPIRY = ")
			if !found {
				t.Errorf("no DAYS_TO_EXPIRY in %q", request.Statement)
			} else if _, err := fmt.Sscanf(clause, "%d;", &days); err != nil {
				t.Errorf("parse DAYS_TO_EXPIRY from %q: %v", request.Statement, err)
			}
			// Column metadata matches a live Snowflake response: cols are
			// [token_name, token_secret]. The secret is read by name, not position.
			_ = json.NewEncoder(w).Encode(map[string]any{
				"resultSetMetadata": map[string]any{
					"numRows": 1,
					"rowType": []map[string]any{
						{"name": "token_name", "type": "text"},
						{"name": "token_secret", "type": "text"},
					},
				},
				"data": [][]string{{"C1_REQUEST_1", secret}},
			})
		case strings.HasPrefix(request.Statement, "SHOW USER PROGRAMMATIC ACCESS TOKENS"):
			expiresAt := "1893456000"
			if mock.liveExpiry {
				// Fractional seconds, the way Snowflake reports a TIMESTAMP_LTZ. Truncating
				// to whole seconds would hide sub-second clock drift, which is the whole
				// quantity the expiry-sampling test measures.
				expiresAt = strconv.FormatFloat(
					float64(time.Now().UTC().AddDate(0, 0, int(days)).UnixNano())/1e9, 'f', 6, 64,
				)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"resultSetMetadata": map[string]any{
					"numRows": 1,
					"rowType": []map[string]any{{"name": "name", "type": "text"}, {"name": "expires_at", "type": "timestamp_ltz"}},
				},
				"data": [][]string{{mock.showTokenName, expiresAt}},
			})
		case strings.Contains(request.Statement, "REMOVE PROGRAMMATIC ACCESS TOKEN"):
			_ = json.NewEncoder(w).Encode(map[string]any{"data": [][]string{}})
		default:
			t.Errorf("unexpected statement: %s", request.Statement)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}

func containsStatement(statements []string, want string) bool {
	for _, statement := range statements {
		if strings.Contains(statement, want) {
			return true
		}
	}
	return false
}

func TestProgrammaticAccessTokenIDRoundTrip(t *testing.T) {
	userName, tokenName, err := parseProgrammaticAccessTokenID(programmaticAccessTokenID(`Mixed Case User`, "c1-request_1"))
	if err != nil {
		t.Fatalf("parseProgrammaticAccessTokenID() error = %v", err)
	}
	if userName != `Mixed Case User` || tokenName != "c1-request_1" {
		t.Fatalf("round trip = (%q, %q), want (%q, %q)", userName, tokenName, `Mixed Case User`, "c1-request_1")
	}
}

func TestCredentialIssuanceCapabilitiesRegisterWithDeleter(t *testing.T) {
	server, err := connectorbuilder.NewConnector(context.Background(), &Connector{IssueCredentials: true})
	if err != nil {
		t.Fatalf("NewConnector() error = %v", err)
	}

	response, err := server.GetMetadata(context.Background(), &v2.ConnectorServiceGetMetadataRequest{})
	if err != nil {
		t.Fatalf("GetMetadata() error = %v", err)
	}
	for _, capability := range response.GetMetadata().GetCapabilities().GetResourceTypeCapabilities() {
		if capability.GetResourceType().GetId() != userResourceType.Id {
			continue
		}
		details := capability.GetCredentialIssue()
		if details == nil || len(details.GetOptions()) != 2 {
			t.Fatalf("credential issue details = %#v, want raw TOKEN and native API_KEY options", details)
		}
		for _, descriptor := range details.GetOptions() {
			if descriptor.GetSecretResourceTypeId() != programmaticAccessTokenResourceType.Id {
				t.Fatalf("secret resource type = %q, want %q", descriptor.GetSecretResourceTypeId(), programmaticAccessTokenResourceType.Id)
			}
			if descriptor.GetExpiry().GetMin().AsDuration() != programmaticAccessTokenMinLifetime || descriptor.GetExpiry().GetMax().AsDuration() != programmaticAccessTokenMaxLifetime {
				t.Fatalf("expiry = %#v, want min %v and max %v", descriptor.GetExpiry(), programmaticAccessTokenMinLifetime, programmaticAccessTokenMaxLifetime)
			}
		}
		if details.GetOptions()[0].GetOption() != v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN ||
			details.GetOptions()[1].GetOption() != v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY ||
			details.GetPreferredOption() != v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN {
			t.Fatalf("unexpected credential options: %#v", details)
		}
		return
	}
	t.Fatal("user resource type capability not found")
}

func TestIssueCapabilityDetails(t *testing.T) {
	details, _, err := newCredentialUserBuilder(nil, secretOptions{}).IssueCapabilityDetails(context.Background())
	if err != nil {
		t.Fatalf("IssueCapabilityDetails() error = %v", err)
	}
	descriptor := details.GetOptions()[0]
	if descriptor.GetOption() != v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN ||
		descriptor.GetResourceMode() != v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	if descriptor.GetExpiry().GetMin().AsDuration() != programmaticAccessTokenMinLifetime {
		t.Fatalf("minimum expiry = %v", descriptor.GetExpiry().GetMin())
	}
}

func TestCredentialUserBuilderIssueServiceUserWithNullDefaultRoleReportsMissingRole(t *testing.T) {
	// The SQL API returns every column as text, so an unset DEFAULT_ROLE arrives as the
	// literal "null". Testing the raw string for emptiness never matches, which used to
	// send the caller to the "not granted" branch and tell them to grant a role named null.
	var statements []string
	server := newCredentialIssueMockServer(t, "SERVICE", "null", false, &statements)
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = newCredentialUserBuilder(client, secretOptions{}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
		IdentityID: v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "service-user"}.Build(),
		RequestID:  "request-1",
	})
	if err == nil || !strings.Contains(err.Error(), "has no default role") {
		t.Fatalf("Issue() error = %v, want the missing-default-role error", err)
	}
	if containsStatement(statements, "ADD PROGRAMMATIC ACCESS TOKEN") {
		t.Fatalf("Issue() created a token despite the user having no default role: %q", statements)
	}
}

func TestCredentialUserBuilderIssueRemovesTokenWhenProviderDoesNotReturnIt(t *testing.T) {
	// Every failure after creation must remove the token. Otherwise the plaintext is
	// discarded, no secret resource is recorded, the SDK does not retry, and the
	// credential is left live with nothing holding a handle to revoke it.
	var statements []string
	server := newCredentialIssueMockServerWithShowName(t, "SERVICE", "service_role", true, "some-other-token", &statements)
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = newCredentialUserBuilder(client, secretOptions{}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
		IdentityID: v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "service-user"}.Build(),
		RequestID:  "request-1",
	})
	if err == nil {
		t.Fatal("Issue() error = nil, want failure when the provider does not return the token")
	}
	if !containsStatement(statements, "ADD PROGRAMMATIC ACCESS TOKEN") {
		t.Fatalf("test did not reach token creation: %q", statements)
	}
	if !containsStatement(statements, "REMOVE PROGRAMMATIC ACCESS TOKEN") {
		t.Fatalf("Issue() left the created token orphaned: %q", statements)
	}
}

func TestCredentialUserBuilderIssueKeepsTokenWhenReadBackIsDenied(t *testing.T) {
	// SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER needs ownership or MODIFY
	// PROGRAMMATIC AUTHENTICATION METHODS on the target user; creating the token
	// does not. A role holding one and not the other would otherwise create a good
	// credential and immediately destroy it, so issuance could never succeed for
	// that tenant.
	var statements []string
	server := serveCredentialIssueMock(t, credentialIssueMock{
		userType: "SERVICE", defaultRole: "service_role", roleGranted: true,
		showTokenName: "c1-request-1", denyPrefix: "SHOW USER PROGRAMMATIC ACCESS TOKENS",
		statements: &statements,
	})
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	output, err := newCredentialUserBuilder(client, secretOptions{}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
		IdentityID: v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "service-user"}.Build(),
		RequestID:  "request-1",
	})
	if err != nil {
		t.Fatalf("Issue() error = %v, want issuance to survive a read-back denial", err)
	}
	if containsStatement(statements, "REMOVE PROGRAMMATIC ACCESS TOKEN") {
		t.Fatalf("Issue() destroyed a good credential over a read-back denial: %q", statements)
	}
	// Falls back to the locally computed expiry, which is Snowflake's own arithmetic
	// from a slightly earlier clock and so is never later than the real one.
	trait := &v2.SecretTrait{}
	annos := annotations.Annotations(output.Secret.GetAnnotations())
	if ok, err := annos.Pick(trait); err != nil || !ok {
		t.Fatalf("secret trait: ok = %v, err = %v", ok, err)
	}
	want := time.Now().UTC().AddDate(0, 0, programmaticAccessTokenDefaultDays)
	got := trait.GetExpiresAt().AsTime()
	if got.Sub(want) > time.Minute || want.Sub(got) > time.Minute {
		t.Fatalf("expiry = %v, want approximately %v", got, want)
	}
}

func TestCredentialUserBuilderIssueProceedsWhenRoleCheckIsDenied(t *testing.T) {
	// The default-role pre-check only turns a Snowflake rejection into a better
	// message. A role that cannot run SHOW GRANTS TO USER must still be able to issue.
	var statements []string
	server := serveCredentialIssueMock(t, credentialIssueMock{
		userType: "SERVICE", defaultRole: "service_role", roleGranted: true,
		showTokenName: "c1-request-1", denyPrefix: "SHOW GRANTS TO USER",
		statements: &statements,
	})
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = newCredentialUserBuilder(client, secretOptions{}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
		IdentityID: v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "service-user"}.Build(),
		RequestID:  "request-1",
	})
	if err != nil {
		t.Fatalf("Issue() error = %v, want issuance to survive a role-check denial", err)
	}
	if !containsStatement(statements, `ROLE_RESTRICTION = "service_role"`) {
		t.Fatalf("issuance dropped the role restriction it could not verify: %q", statements)
	}
}

func TestCredentialUserBuilderIssueSamplesExpiryAfterPreflight(t *testing.T) {
	// Snowflake derives the expiry from its own clock at ALTER USER time. Sampling
	// before DESCRIBE USER and SHOW GRANTS makes the real expiry later than the one
	// computed here by however long those took, so a request whose remaining time sits
	// just above a whole number of days trips the "provider expiry exceeds requested"
	// guard and destroys a valid token.
	var statements []string
	server := serveCredentialIssueMock(t, credentialIssueMock{
		userType: "SERVICE", defaultRole: "service_role", roleGranted: true,
		showTokenName: "c1-request-1", preflightDelay: 150 * time.Millisecond,
		liveExpiry: true, statements: &statements,
	})
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	requested := time.Now().UTC().Add(2*24*time.Hour + 50*time.Millisecond)
	_, err = newCredentialUserBuilder(client, secretOptions{}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
		IdentityID: v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "service-user"}.Build(),
		RequestID:  "request-1",
		ExpiresAt:  timestamppb.New(requested),
	})
	if err != nil {
		t.Fatalf("Issue() error = %v, want a shorter token rather than a failed issuance", err)
	}
	if containsStatement(statements, "REMOVE PROGRAMMATIC ACCESS TOKEN") {
		t.Fatalf("Issue() created and then destroyed a token: %q", statements)
	}
}

func TestProgrammaticAccessTokenListDeniedUserFailsSync(t *testing.T) {
	// A full sync is authoritative: an error-free empty result on a per-user
	// access-control denial would make C1 delete the user's previously synced
	// tokens and drop the revocation handles on credentials that stay live in
	// Snowflake. The denial therefore fails the sync, and the error carries the
	// privilege to grant; the OptInRequired annotation is what lets a tenant
	// without the privilege keep the type off.
	var statements []string
	server := serveCredentialIssueMock(t, credentialIssueMock{
		denyPrefix: "SHOW USER PROGRAMMATIC ACCESS TOKENS",
		statements: &statements,
	})
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	resources, results, err := newProgrammaticAccessTokenBuilder(client).List(context.Background(),
		v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "no-access-user"}.Build(), rs.SyncOpAttrs{})
	if !snowflake.IsInsufficientPrivileges(err) {
		t.Fatalf("List() error = %v, want the 003001 denial to fail the sync", err)
	}
	if resources != nil || results != nil {
		t.Fatalf("List() = (%v, %v), want nil resources and results on a denial", resources, results)
	}
	if !strings.Contains(err.Error(), "MODIFY PROGRAMMATIC AUTHENTICATION METHODS") {
		t.Fatalf("List() error %q does not name the privilege to grant", err)
	}
}
