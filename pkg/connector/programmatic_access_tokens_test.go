package connector

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
)

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
