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

// sync-secrets and issue-credentials are independent, but not unrelated: issuance
// advertises DISCOVERABLE, so turning it on has to make the token type syncable even
// when the broader secret sync is off. Otherwise an issued credential exists with
// nothing holding a handle to revoke it.
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
			wantTypes: []string{"account_role", "database", "integration", "license", "table", "user"},
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
			for _, capability := range response.GetMetadata().GetCapabilities().GetResourceTypeCapabilities() {
				gotTypes = append(gotTypes, capability.GetResourceType().GetId())
				if capability.GetResourceType().GetId() == userResourceType.Id && capability.GetCredentialIssue() != nil {
					gotIssuer = true
				}
			}
			sort.Strings(gotTypes)
			require.Equal(t, tc.wantTypes, gotTypes)
			require.Equal(t, tc.wantIssuer, gotIssuer, "credential issuance advertised")

			// The child annotations have to move with the resource types, or a synced
			// type is registered but never walked per user.
			resource, err := userResource(context.Background(),
				&snowflake.User{Username: "service-user", Type: "SERVICE"},
				secretOptions{syncSecrets: tc.syncSecrets, issueCredentials: tc.issueCredentials})
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
			want := []string{}
			if tc.syncSecrets {
				want = append(want, rsaPublicKeyResourceType.Id)
			}
			if tc.syncSecrets || tc.issueCredentials {
				want = append(want, programmaticAccessTokenResourceType.Id)
			}
			sort.Strings(want)
			require.Equal(t, want, children)
		})
	}
}
