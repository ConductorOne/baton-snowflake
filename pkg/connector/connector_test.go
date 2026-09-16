package connector

import (
	"context"
	"fmt"
	"sort"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/require"
)

func TestInspectUserVisibility(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		logins      []string
		wantNone    bool
		wantPartial bool
	}{
		{name: "login_name populated", logins: []string{"alice"}},
		{name: "login_name blank", logins: []string{""}, wantNone: true},
		{name: "login_name whitespace-only", logins: []string{"   "}, wantNone: true},
		{name: "all blank", logins: []string{"", "  ", ""}, wantNone: true},
		// The all-or-nothing check this replaced passed here and then silently synced the
		// two blanked users with no login, no email, and no TYPE.
		{name: "one of many populated", logins: []string{"", "bob", ""}, wantPartial: true},
		{name: "all populated", logins: []string{"alice", "bob"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			users := make([]snowflake.User, len(tt.logins))
			for i, login := range tt.logins {
				users[i] = snowflake.User{Username: fmt.Sprintf("user-%d", i), Login: login}
			}
			got := inspectUserVisibility(users)
			require.Equal(t, tt.wantNone, got.None(), "None()")
			require.Equal(t, tt.wantPartial, got.Partial(), "Partial()")
			require.Equal(t, !tt.wantNone && !tt.wantPartial, got.Complete(), "Complete()")
		})
	}
}

// The diagnostic has to be matchable by callers and has to name the fix, not just the
// symptom - pointing only at MANAGE GRANTS is what sent the original customer's security
// review off the rails.
func TestNoUserVisibilityErrorIsNamedAndActionable(t *testing.T) {
	t.Parallel()
	err := noUserVisibilityError(snowflake.DiscoveryModeShow, 50)
	require.ErrorIs(t, err, ErrNoUserVisibility)
	for _, want := range []string{"GRANT OWNERSHIP ON USER", "discovery-mode=account_usage", "SECURITY_VIEWER", "MANAGE GRANTS"} {
		require.Contains(t, err.Error(), want)
	}

	// Under ACCOUNT_USAGE there is no per-user privilege to grant, so the SHOW remedies
	// would send the operator after a cause that cannot apply.
	auErr := noUserVisibilityError(snowflake.DiscoveryModeAccountUsage, 50)
	require.ErrorIs(t, auErr, ErrNoUserVisibility)
	require.Contains(t, auErr.Error(), "SNOWFLAKE.ACCOUNT_USAGE.USERS")
	require.NotContains(t, auErr.Error(), "GRANT OWNERSHIP ON USER")
	require.NotContains(t, auErr.Error(), "MANAGE GRANTS")
}

// A blanked-user list from a large account must not end up unbounded in the log field.
func TestUserVisibilityDescribeBlankedIsBounded(t *testing.T) {
	t.Parallel()
	users := make([]snowflake.User, 0, maxReportedBlankedUsers+5)
	users = append(users, snowflake.User{Username: "visible", Login: "visible"})
	for i := 0; i < maxReportedBlankedUsers+5; i++ {
		users = append(users, snowflake.User{Username: fmt.Sprintf("blanked-%d", i)})
	}
	got := inspectUserVisibility(users)
	require.True(t, got.Partial())
	require.Equal(t, maxReportedBlankedUsers+5, got.Blanked)
	require.Len(t, got.BlankedNames, maxReportedBlankedUsers)
	require.Contains(t, got.describeBlanked(), "and 5 more")
}

// A user whose columns Snowflake suppressed must not be reported as a human: that is how an
// unowned SERVICE account silently became a person in C1.
func TestPartiallyVisibleUserAccountTypeIsUnspecified(t *testing.T) {
	t.Parallel()
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_UNSPECIFIED,
		getUserAccountType(&snowflake.User{Username: "unowned"}))
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_HUMAN,
		getUserAccountType(&snowflake.User{Username: "alice", Login: "alice"}))
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_SERVICE,
		getUserAccountType(&snowflake.User{Username: "svc", Login: "svc", Type: "SERVICE"}))
	// A SERVICE user whose TYPE did survive still classifies as a service account even
	// though login_name is blank, so the suppression check must not outrank a known TYPE.
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_SERVICE,
		getUserAccountType(&snowflake.User{Username: "svc", Type: "SERVICE"}))
}

// Object-level resource types are a group toggle: tables are children of databases, so
// neither may be registered without the other, and database-scoped secrets move with them.
func TestSyncObjectResourcesToggle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		objects     bool
		syncSecrets bool
		wantTypes   []string
	}{
		{
			name:      "objects off",
			wantTypes: []string{"account_role", "integration", "license", "programmatic_access_token", "user"},
		},
		{
			name:        "objects off with sync-secrets still leaves out the database-scoped secret type",
			syncSecrets: true,
			wantTypes: []string{
				"account_role", "integration", "license", "programmatic_access_token",
				"rsa_public_key", "user",
			},
		},
		{
			name:      "objects on",
			objects:   true,
			wantTypes: []string{"account_role", "database", "integration", "license", "programmatic_access_token", "table", "user"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server, err := connectorbuilder.NewConnector(context.Background(), &Connector{
				syncObjectResources: tc.objects, SyncSecrets: tc.syncSecrets,
			})
			require.NoError(t, err)
			response, err := server.GetMetadata(context.Background(), &v2.ConnectorServiceGetMetadataRequest{})
			require.NoError(t, err)

			gotTypes := []string{}
			for _, capability := range response.GetMetadata().GetCapabilities().GetResourceTypeCapabilities() {
				gotTypes = append(gotTypes, capability.GetResourceType().GetId())
			}
			sort.Strings(gotTypes)
			require.Equal(t, tc.wantTypes, gotTypes)
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
				syncObjectResources: true,
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
