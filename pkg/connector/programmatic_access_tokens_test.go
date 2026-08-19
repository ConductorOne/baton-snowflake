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
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

func TestCredentialUserBuilderIssueServiceUserUsesDefaultRoleRestriction(t *testing.T) {
	var statements []string
	server := newCredentialIssueMockServer(t, "SERVICE", "service_role", true, &statements)
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = newCredentialUserBuilder(client, true).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
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

	_, err = newCredentialUserBuilder(client, true).Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
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

func newCredentialIssueMockServer(t *testing.T, userType, defaultRole string, roleGranted bool, statements *[]string) *httptest.Server {
	t.Helper()
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
		*statements = append(*statements, request.Statement)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.HasPrefix(request.Statement, "DESCRIBE USER"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"resultSetMetadata": map[string]any{"numRows": 12},
				"data": [][]string{
					{"NAME", "service-user"}, {"LOGIN_NAME", "service-user"}, {"DISPLAY_NAME", "Service User"},
					{"FIRST_NAME", ""}, {"LAST_NAME", ""}, {"EMAIL", ""}, {"DISABLED", "false"},
					{"SNOWFLAKE_LOCK", "false"}, {"DEFAULT_ROLE", defaultRole}, {"TYPE", userType},
					{"HAS_MFA", "false"}, {"COMMENT", ""},
				},
			})
		case strings.HasPrefix(request.Statement, "SHOW GRANTS TO USER"):
			data := [][]string{}
			if roleGranted {
				data = append(data, []string{"ROLE", defaultRole})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"resultSetMetadata": map[string]any{
					"numRows": len(data),
					"rowType": []map[string]any{{"name": "granted_on", "type": "text"}, {"name": "name", "type": "text"}},
				},
				"data": data,
			})
		case strings.Contains(request.Statement, "ADD PROGRAMMATIC ACCESS TOKEN"):
			_ = json.NewEncoder(w).Encode(map[string]any{"data": [][]string{{"C1_REQUEST_1", "redacted"}}})
		case strings.HasPrefix(request.Statement, "SHOW USER PROGRAMMATIC ACCESS TOKENS"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"resultSetMetadata": map[string]any{
					"numRows": 1,
					"rowType": []map[string]any{{"name": "name", "type": "text"}, {"name": "expires_at", "type": "timestamp_ltz"}},
				},
				"data": [][]string{{"c1-request-1", "1893456000"}},
			})
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
	server, err := connectorbuilder.NewConnector(context.Background(), &Connector{SyncSecrets: true})
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
	details, _, err := newCredentialUserBuilder(nil, true).IssueCapabilityDetails(context.Background())
	if err != nil {
		t.Fatalf("IssueCapabilityDetails() error = %v", err)
	}
	descriptor := details.GetOptions()[0]
	if descriptor.GetOption() != v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN || descriptor.GetResourceMode() != v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	if descriptor.GetExpiry().GetMin().AsDuration() != programmaticAccessTokenMinLifetime {
		t.Fatalf("minimum expiry = %v", descriptor.GetExpiry().GetMin())
	}
}
