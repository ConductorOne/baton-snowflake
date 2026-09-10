package connector

import (
	"context"
	"sort"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/require"
)

func TestMissingLoginPrivilegeErr(t *testing.T) {
	tests := []struct {
		name    string
		logins  []string
		wantErr bool
	}{
		{name: "login_name populated", logins: []string{"alice"}, wantErr: false},
		{name: "login_name blank", logins: []string{""}, wantErr: true},
		{name: "login_name whitespace-only", logins: []string{"   "}, wantErr: true},
		{name: "one of many populated", logins: []string{"", "bob", ""}, wantErr: false},
		{name: "all blank", logins: []string{"", "  ", ""}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			users := make([]snowflake.User, len(tt.logins))
			for i, login := range tt.logins {
				users[i] = snowflake.User{Login: login}
			}
			err := missingLoginPrivilegeErr(users)
			if tt.wantErr && err == nil {
				t.Fatal("missingLoginPrivilegeErr() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("missingLoginPrivilegeErr() = %v, want nil", err)
			}
		})
	}
}

// sync-secrets and issue-credentials are independent and neither one registers the
// programmatic_access_token type: the builder is always registered and the type is
// OptInRequired, so an on→off flag toggle can never delete synced tokens or their
// revocation handles. The flags only choose the user syncer (issuance) and the
// secret/RSA builders.
func TestSecretFlagsGateIndependently(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		syncSecrets      bool
		issueCredentials bool
		wantIssuer       bool
		wantTypes        []string
	}{
		{
			name:      "neither",
			wantTypes: []string{"account_role", "database", "integration", "license", "programmatic_access_token", "table", "user"},
		},
		{
			name:        "inventory without minting",
			syncSecrets: true,
			wantTypes: []string{
				"account_role", "database", "integration", "license",
				"programmatic_access_token", "rsa_public_key", "secret", "table", "user",
			},
		},
		{
			name:             "minting without a full inventory",
			issueCredentials: true,
			wantIssuer:       true,
			wantTypes: []string{
				"account_role", "database", "integration", "license",
				"programmatic_access_token", "table", "user",
			},
		},
		{
			name:             "both",
			syncSecrets:      true,
			issueCredentials: true,
			wantIssuer:       true,
			wantTypes: []string{
				"account_role", "database", "integration", "license",
				"programmatic_access_token", "rsa_public_key", "secret", "table", "user",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, err := connectorbuilder.NewConnector(context.Background(), &Connector{
				SyncSecrets: tc.syncSecrets, IssueCredentials: tc.issueCredentials,
			})
			require.NoError(t, err)
			response, err := server.GetMetadata(context.Background(), &v2.ConnectorServiceGetMetadataRequest{})
			require.NoError(t, err)

			gotTypes, gotIssuer := []string{}, false
			patOptIn := true
			for _, capability := range response.GetMetadata().GetCapabilities().GetResourceTypeCapabilities() {
				gotTypes = append(gotTypes, capability.GetResourceType().GetId())
				if capability.GetResourceType().GetId() == userResourceType.Id && capability.GetCredentialIssue() != nil {
					gotIssuer = true
				}
				if capability.GetResourceType().GetId() == programmaticAccessTokenResourceType.Id {
					// C1 keeps non-opted-in types out of the sync, which is what
					// protects revocation handles from a flag toggle now that the
					// builder is registered unconditionally.
					patOptIn = capability.GetOptInRequired()
				}
			}
			sort.Strings(gotTypes)
			require.True(t, patOptIn, "programmatic_access_token must be opt-in in every flag combination")
			require.Equal(t, tc.wantTypes, gotTypes)
			require.Equal(t, tc.wantIssuer, gotIssuer, "credential issuance advertised")

			// The child annotations have to move with the resource types, or a synced
			// type is registered but never walked per user.
			resource, err := userResource(context.Background(),
				&snowflake.User{Username: "service-user", Type: "SERVICE"},
				secretOptions{syncSecrets: tc.syncSecrets})
			require.NoError(t, err)
			children := []string{}
			for _, annotation := range resource.GetAnnotations() {
				child := &v2.ChildResourceType{}
				if annotation.MessageIs(child) {
					require.NoError(t, annotation.UnmarshalTo(child))
					children = append(children, child.GetResourceTypeId())
				}
			}
			sort.Strings(children)
			want := []string{programmaticAccessTokenResourceType.Id}
			if tc.syncSecrets {
				want = append(want, rsaPublicKeyResourceType.Id)
			}
			sort.Strings(want)
			require.Equal(t, want, children)
		})
	}
}
