package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/config"
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

// The boundary test that TestPartiallyVisibleUserAccountTypeIsUnspecified cannot be: what
// getUserAccountType returns and what actually reaches C1 are two different things.
// NewUserTrait in baton-sdk rewrites ACCOUNT_TYPE_UNSPECIFIED to ACCOUNT_TYPE_HUMAN before
// the trait is emitted, so the suppressed-TYPE user is still delivered as a person.
//
// This pins the real, observable behavior so the documentation cannot drift away from it.
// When the SDK stops defaulting, this test fails - at which point the fix is to flip the
// expectation here and update the two docs sections that describe the behavior, not to
// silence it.
func TestPartiallyVisibleUserTraitIsHumanUntilSDKStopsDefaulting(t *testing.T) {
	t.Parallel()
	resource, err := userResource(context.Background(), &snowflake.User{Username: "unowned"}, secretOptions{})
	require.NoError(t, err)

	trait, err := rs.GetUserTrait(resource)
	require.NoError(t, err)
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_UNSPECIFIED, getUserAccountType(&snowflake.User{Username: "unowned"}),
		"the helper declines to guess")
	require.Equal(t, v2.UserTrait_ACCOUNT_TYPE_HUMAN, trait.GetAccountType(),
		"the SDK collapses UNSPECIFIED to HUMAN, so this is what C1 sees today")
}

// The capabilities literal is what generates baton_capabilities.json, and every gate on
// Connector is a bool whose zero value drops a resource type. A hand-written literal that
// missed a new gate is how database, table and secret once vanished from the committed
// metadata while every real sync still produced them.
func TestDefaultCapabilitiesConnectorAdvertisesEveryResourceType(t *testing.T) {
	t.Parallel()
	cb, err := connectorbuilder.NewConnector(context.Background(), DefaultCapabilitiesConnector())
	require.NoError(t, err)

	md, err := cb.GetMetadata(context.Background(), &v2.ConnectorServiceGetMetadataRequest{})
	require.NoError(t, err)

	var gotTypes []string
	for _, rtc := range md.GetMetadata().GetCapabilities().GetResourceTypeCapabilities() {
		gotTypes = append(gotTypes, rtc.GetResourceType().GetId())
	}
	sort.Strings(gotTypes)
	require.Equal(t, []string{
		"account_role", "database", "integration", "license",
		"programmatic_access_token", "rsa_public_key", "secret", "table", "user",
	}, gotTypes)
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

// serveUsers answers the Statements API with one SHOW USERS page built from the given
// (name, login) pairs, so Validate can be driven end to end without credentials.
func serveUsers(t *testing.T, pairs [][2]string) *httptest.Server {
	t.Helper()
	// ParseRow walks every column in userStructFieldToColumnMap and fails the whole read on
	// the first one missing, so the fixture has to carry the full SHOW USERS column set.
	columns := []string{
		"name", "login_name", "display_name", "first_name", "last_name", "email",
		"disabled", "snowflake_lock", "default_role", "has_rsa_public_key",
		"has_password", "last_success_login", "type", "has_mfa", "comment",
	}
	rowTypes := make([]map[string]any, 0, len(columns))
	for _, c := range columns {
		columnType := "text"
		if c == "last_success_login" {
			columnType = "timestamp_ltz"
		}
		rowTypes = append(rowTypes, map[string]any{"name": c, "type": columnType})
	}
	rows := make([][]string, 0, len(pairs))
	for _, p := range pairs {
		row := make([]string, len(columns))
		row[0], row[1] = p[0], p[1]
		rows = append(rows, row)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"statementHandle": "handle",
			"resultSetMetadata": map[string]any{
				"numRows":       len(rows),
				"rowType":       rowTypes,
				"partitionInfo": []map[string]any{{"rowCount": len(rows)}},
			},
			"data": rows,
		})
	}))
}

// Validate is the connector's startup gate and its error text is the whole deliverable of
// the visibility work, but nothing exercised it. These cases cover each branch the rewrite
// introduced.
func TestValidate(t *testing.T) {
	t.Parallel()

	t.Run("no users at all fails, naming the surface that was read", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			name string
			mode snowflake.DiscoveryMode
			want string
		}{
			{name: "show", mode: snowflake.DiscoveryModeShow, want: "SHOW USERS"},
			{name: "account_usage", mode: snowflake.DiscoveryModeAccountUsage, want: "SNOWFLAKE.ACCOUNT_USAGE.USERS"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				server := serveUsers(t, nil)
				defer server.Close()
				client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client(),
					snowflake.WithDiscoveryMode(tc.mode))
				require.NoError(t, err)

				_, err = (&Connector{Client: client}).Validate(context.Background())
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.want)
			})
		}
	})

	t.Run("every login blanked fails as ErrNoUserVisibility", func(t *testing.T) {
		t.Parallel()
		server := serveUsers(t, [][2]string{{"alice", ""}, {"bob", ""}})
		defer server.Close()
		client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
		require.NoError(t, err)

		_, err = (&Connector{Client: client}).Validate(context.Background())
		require.ErrorIs(t, err, ErrNoUserVisibility)
	})

	t.Run("partial visibility is a warning, not a failure", func(t *testing.T) {
		t.Parallel()
		// A tenant on the per-user OWNERSHIP model may legitimately hand over a subset of
		// users; failing here would make that configuration unusable.
		server := serveUsers(t, [][2]string{{"alice", "alice"}, {"unowned", ""}})
		defer server.Close()
		client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
		require.NoError(t, err)

		_, err = (&Connector{Client: client}).Validate(context.Background())
		require.NoError(t, err)
	})

	t.Run("fully visible passes", func(t *testing.T) {
		t.Parallel()
		server := serveUsers(t, [][2]string{{"alice", "alice"}})
		defer server.Close()
		client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
		require.NoError(t, err)

		_, err = (&Connector{Client: client}).Validate(context.Background())
		require.NoError(t, err)
	})
}

// Turning object resources off makes two other settings no-ops, and neither can be expressed
// as an SDK field relationship: all four relationship helpers are presence-based and
// sync-object-resources is a bool with a default, so it is always "present". A startup
// warning is the available signal, so it is worth pinning that it fires only when it should.
func TestIgnoredObjectResourceFlags(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		cfg  *config.Snowflake
		want []string
	}{
		{
			name: "objects on, nothing ignored",
			cfg: &config.Snowflake{
				SyncObjectResources: true,
				SyncSecrets:         true,
				ExcludedDatabases:   []string{"DB"},
			},
		},
		{
			name: "objects off ignores excluded-databases",
			cfg:  &config.Snowflake{ExcludedDatabases: []string{"DB"}},
			want: []string{"--excluded-databases is ignored"},
		},
		{
			name: "objects off ignores sync-secrets",
			cfg:  &config.Snowflake{SyncSecrets: true},
			want: []string{"no secret is synced"},
		},
		{
			name: "objects off ignores both",
			cfg:  &config.Snowflake{SyncSecrets: true, ExcludedDatabases: []string{"DB"}},
			want: []string{"--excluded-databases is ignored", "no secret is synced"},
		},
		{
			name: "objects off with neither set warns about nothing",
			cfg:  &config.Snowflake{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ignoredObjectResourceFlags(tc.cfg)
			require.Len(t, got, len(tc.want))
			for i, want := range tc.want {
				require.Contains(t, got[i], want)
			}
		})
	}
}
