package connector

import (
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

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCredentialUserBuilderIssueServiceUserUsesDefaultRoleRestriction(t *testing.T) {
	var statements []string
	server := newCredentialIssueMockServer(t, "SERVICE", "service_role", true, &statements)
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = newCredentialUserBuilder(client, secretOptions{issueCredentials: true}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
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

	_, err = newCredentialUserBuilder(client, secretOptions{issueCredentials: true}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
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
				"data": [][]string{{"C1_REQUEST_1", "redacted"}},
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
		if details == nil || len(details.GetOptions()) != 1 {
			t.Fatalf("credential issue details = %#v, want one option", details)
		}
		descriptor := details.GetOptions()[0]
		if descriptor.GetSecretResourceTypeId() != programmaticAccessTokenResourceType.Id {
			t.Fatalf("secret resource type = %q, want %q", descriptor.GetSecretResourceTypeId(), programmaticAccessTokenResourceType.Id)
		}
		if descriptor.GetExpiry().GetMin().AsDuration() != programmaticAccessTokenMinLifetime || descriptor.GetExpiry().GetMax().AsDuration() != programmaticAccessTokenMaxLifetime {
			t.Fatalf("expiry = %#v, want min %v and max %v", descriptor.GetExpiry(), programmaticAccessTokenMinLifetime, programmaticAccessTokenMaxLifetime)
		}
		return
	}
	t.Fatal("user resource type capability not found")
}

func TestIssueCapabilityDetails(t *testing.T) {
	details, _, err := newCredentialUserBuilder(nil, secretOptions{issueCredentials: true}).IssueCapabilityDetails(context.Background())
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

	_, err = newCredentialUserBuilder(client, secretOptions{issueCredentials: true}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
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

	_, err = newCredentialUserBuilder(client, secretOptions{issueCredentials: true}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
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

func TestUserResourceAdvertisesTokenAsChildResourceType(t *testing.T) {
	// The syncer walks a child type per parent only when the parent carries this
	// annotation. Without it an issued token is never discovered by a sync, which
	// contradicts the DISCOVERABLE mode the issuer advertises. issue-credentials alone
	// must be enough: it is the flag that makes tokens exist in the first place.
	resource, err := userResource(context.Background(), &snowflake.User{Username: "service-user", Type: "SERVICE"}, secretOptions{issueCredentials: true})
	if err != nil {
		t.Fatalf("userResource() error = %v", err)
	}
	found := false
	for _, annotation := range resource.GetAnnotations() {
		child := &v2.ChildResourceType{}
		if annotation.MessageIs(child) {
			if err := annotation.UnmarshalTo(child); err != nil {
				t.Fatalf("unmarshal child resource type: %v", err)
			}
			if child.GetResourceTypeId() == programmaticAccessTokenResourceType.Id {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("user resource is missing ChildResourceType %q", programmaticAccessTokenResourceType.Id)
	}
}

func TestCredentialUserBuilderIssueKeepsTokenWhenReadBackIsDenied(t *testing.T) {
	// SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER needs ownership or MONITOR on the
	// target user; creating the token does not. A role holding one and not the other
	// would otherwise create a good credential and immediately destroy it, so issuance
	// could never succeed for that tenant.
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

	output, err := newCredentialUserBuilder(client, secretOptions{issueCredentials: true}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
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

	_, err = newCredentialUserBuilder(client, secretOptions{issueCredentials: true}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
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
	_, err = newCredentialUserBuilder(client, secretOptions{issueCredentials: true}).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
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
