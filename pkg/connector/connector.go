package connector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/conductorone/baton-snowflake/pkg/config"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
)

type Connector struct {
	Client            *snowflake.Client
	SyncSecrets       bool
	IssueCredentials  bool
	excludedDatabases []string
	// syncObjectResources gates the database/schema/table resource types as a group.
	// Off means the tenant never has to grant object-level privileges (USAGE on
	// databases and schemas, REFERENCES on tables) to the service account at all -
	// unlike excludedDatabases, which is a connector-side filter applied after the
	// privileges have already been granted and every database enumerated.
	syncObjectResources bool
}

// DefaultCapabilitiesConnector is the Connector the `capabilities` command introspects to
// generate baton_capabilities.json.
//
// It exists so the advertised capability set has exactly one definition. Every gate on this
// struct is a bool whose zero value is false, so a hand-written literal silently drops a
// resource type the moment a new gate is added - which is how database, table and secret
// once disappeared from the committed metadata while every sync still produced them. The
// capability set has to describe what the connector CAN sync, so every gate is on here
// regardless of its config default.
func DefaultCapabilitiesConnector() *Connector {
	return &Connector{
		SyncSecrets:         true,
		IssueCredentials:    true,
		syncObjectResources: true,
	}
}

// ResourceSyncers returns a ResourceSyncerV2 for each resource type that should be synced from the upstream service.
func (d *Connector) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncerV2 {
	secrets := secretOptions{syncSecrets: d.SyncSecrets}
	userSyncer := connectorbuilder.ResourceSyncerV2(newUserBuilder(d.Client, secrets))
	if d.IssueCredentials {
		userSyncer = newCredentialUserBuilder(d.Client, secrets)
	}
	builders := []connectorbuilder.ResourceSyncerV2{
		userSyncer,
		newAccountRoleBuilder(d.Client),
		newIntegrationBuilder(d.Client),
		newLicenseBuilder(d.Client),
		// The programmatic_access_token type is opt-in (OptInRequired annotation on
		// its resource type), so it is registered unconditionally: gating it behind
		// --issue-credentials would make an on→off flag toggle delete every synced
		// token, including the revocation handles for credentials C1 issued.
		newProgrammaticAccessTokenBuilder(d.Client),
	}

	// Object-level types are registered as a group: table resources are children of
	// database resources, so leaving the table builder registered without the database
	// builder would only ever produce an empty type.
	if d.syncObjectResources {
		builders = append(builders,
			newDatabaseBuilder(d.Client, d.SyncSecrets, d.excludedDatabases),
			newTableBuilder(d.Client),
		)
	}

	if d.SyncSecrets {
		// Snowflake secrets are database-scoped, so the secret builder only ever gets
		// called with a database parent. Registering it without the database builder
		// would leave a permanently empty type, so it moves with the object-level group;
		// RSA public keys are user-scoped and are unaffected.
		if d.syncObjectResources {
			builders = append(builders, newSecretBuilder(d.Client))
		}
		builders = append(builders, newRsaBuilder(d.Client))
	}

	return builders
}

// Asset takes an input AssetRef and attempts to fetch it using the connector's authenticated http client
// It streams a response, always starting with a metadata object, following by chunked payloads for the asset.
func (d *Connector) Asset(ctx context.Context, asset *v2.AssetRef) (string, io.ReadCloser, error) {
	return "", nil, nil
}

// Metadata returns metadata about the connector.
func (d *Connector) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{
		DisplayName: "Baton Snowflake",
		Description: "Connector syncing users, databases, tables, and account roles from Snowflake.",
		AccountCreationSchema: &v2.ConnectorAccountCreationSchema{
			FieldMap: map[string]*v2.ConnectorAccountCreationSchema_Field{
				profileKeyName: {
					DisplayName: "User Name",
					Required:    true,
					Description: "The name of the user (required - case-sensitive)",
					Placeholder: "username",
					Order:       0,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"login": {
					DisplayName: "Login Name",
					Required:    false,
					Description: "The login name for the user (defaults to email if not provided)",
					Placeholder: "user@example.com",
					Order:       1,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"display_name": {
					DisplayName: "Display Name",
					Required:    false,
					Description: "The display name for the user",
					Placeholder: "John Doe",
					Order:       2,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"first_name": {
					DisplayName: "First Name",
					Required:    false,
					Description: "The first name of the user",
					Placeholder: "John",
					Order:       3,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"last_name": {
					DisplayName: "Last Name",
					Required:    false,
					Description: "The last name of the user",
					Placeholder: "Doe",
					Order:       4,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"email": {
					DisplayName: "Email",
					Required:    false,
					Description: "The email address for the user",
					Placeholder: "user@example.com",
					Order:       5,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				profileKeyComment: {
					DisplayName: "Comment",
					Required:    false,
					Description: "A comment or description for the user",
					Placeholder: "User description",
					Order:       6,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"disabled": {
					DisplayName: "Disabled",
					Required:    false,
					Description: "Whether the user account should be disabled",
					Order:       7,
					Field: &v2.ConnectorAccountCreationSchema_Field_BoolField{
						BoolField: &v2.ConnectorAccountCreationSchema_BoolField{},
					},
				},
				"default_warehouse": {
					DisplayName: "Default Warehouse",
					Required:    false,
					Description: "The default warehouse to use when this user starts a session",
					Placeholder: "COMPUTE_WH",
					Order:       8,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"default_namespace": {
					DisplayName: "Default Namespace",
					Required:    false,
					Description: "The default namespace to use when this user starts a session",
					Placeholder: "DATABASE.SCHEMA",
					Order:       9,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"default_role": {
					DisplayName: "Default Role",
					Required:    false,
					Description: "The default role to use when this user starts a session",
					Placeholder: "PUBLIC",
					Order:       10,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"default_secondary_roles": {
					DisplayName: "Default Secondary Roles",
					Required:    false,
					Description: "The default secondary roles of this user to use when starting a session. Valid values: ALL or NONE. Default is ALL.",
					Placeholder: "ALL",
					Order:       11,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
			},
		},
	}, nil
}

// ErrNoUserVisibility is the named startup failure for "the connector can read the user
// list but not any user's attributes". Snowflake blanks every SHOW USERS column but name
// unless the session role holds OWNERSHIP on the user or account-level MANAGE GRANTS, so a
// sync in this state would produce users with no login, no email, and no TYPE - which the
// connector would then have to classify blind. Callers can match on it with errors.Is.
var ErrNoUserVisibility = errors.New("baton-snowflake: no user attributes are visible to the connector's role")

// userVisibility is the outcome of inspecting a sample of SHOW USERS rows for the
// blanked-column signature of a missing user privilege.
type userVisibility struct {
	Total int
	// Blanked counts sampled users whose login_name came back empty. login_name is
	// mandatory for every Snowflake user TYPE (not just PERSON), so an empty one means
	// the row was returned with its columns suppressed rather than genuinely unset.
	Blanked int
	// BlankedNames is the blanked users' names, which SHOW USERS always returns even when
	// it suppresses everything else. Bounded by maxReportedBlankedUsers so a large
	// partially-visible account cannot produce an unbounded error string.
	BlankedNames []string
}

// maxReportedBlankedUsers bounds how many blanked user names a diagnostic names explicitly.
const maxReportedBlankedUsers = 10

func inspectUserVisibility(users []snowflake.User) userVisibility {
	v := userVisibility{Total: len(users)}
	for _, user := range users {
		if strings.TrimSpace(user.Login) != "" {
			continue
		}
		v.Blanked++
		if len(v.BlankedNames) < maxReportedBlankedUsers {
			v.BlankedNames = append(v.BlankedNames, user.Username)
		}
	}
	return v
}

// Complete reports whether every sampled user's attributes were visible.
func (v userVisibility) Complete() bool { return v.Blanked == 0 }

// None reports whether NO sampled user's attributes were visible, which is the
// all-or-nothing case that must fail startup.
func (v userVisibility) None() bool { return v.Total > 0 && v.Blanked == v.Total }

// Partial reports whether some but not all sampled users were visible. This is the state a
// tenant using the per-user OWNERSHIP model lands in when some users were never handed over:
// the sync can still run, but the unowned users would sync with blank attributes.
func (v userVisibility) Partial() bool { return v.Blanked > 0 && v.Blanked < v.Total }

// describeBlanked renders the blanked user names for a diagnostic, noting truncation.
func (v userVisibility) describeBlanked() string {
	if len(v.BlankedNames) == 0 {
		return ""
	}
	s := strings.Join(v.BlankedNames, ", ")
	if v.Blanked > len(v.BlankedNames) {
		s = fmt.Sprintf("%s (and %d more)", s, v.Blanked-len(v.BlankedNames))
	}
	return s
}

// noUserVisibilityError is the actionable form of ErrNoUserVisibility. It names the exact
// grants that fix the condition rather than pointing only at MANAGE GRANTS, because
// per-user OWNERSHIP is the least-privilege alternative and ACCOUNT_USAGE discovery avoids
// the requirement altogether. The remedies are mode-specific: under ACCOUNT_USAGE discovery
// there is no per-user privilege to grant, so suggesting OWNERSHIP there would send the
// operator after a privilege that cannot be the cause.
func noUserVisibilityError(mode snowflake.DiscoveryMode, sampled int) error {
	if mode == snowflake.DiscoveryModeAccountUsage {
		return fmt.Errorf(
			"%w: %s returned %d user(s) with an empty login_name. LOGIN_NAME is mandatory for every "+
				"Snowflake user type and this view has no per-user privilege requirement, so this is "+
				"not a missing grant. Check that the connector is reading the account you expect and "+
				"that the view is populated - ACCOUNT_USAGE lags the live account by up to 3 hours, so "+
				"a very recently provisioned account can legitimately read empty",
			ErrNoUserVisibility, "SNOWFLAKE.ACCOUNT_USAGE.USERS", sampled,
		)
	}
	return fmt.Errorf(
		"%w: SHOW USERS returned %d user(s) but login_name was empty on every one, which is how "+
			"Snowflake reports that the session role may not read user properties. Fix with any one of: "+
			"(a) GRANT OWNERSHIP ON USER <name> TO ROLE <connector role> for each in-scope user, "+
			"(b) set --discovery-mode=account_usage and grant the connector role the "+
			"SNOWFLAKE.ACCOUNT_USAGE SECURITY_VIEWER database role, or "+
			"(c) GRANT MANAGE GRANTS ON ACCOUNT TO ROLE <connector role> (account-wide and "+
			"self-escalating - prefer (a) or (b)). Also confirm the service account actually has a "+
			"role granted to it: a DEFAULT_ROLE that was never granted produces this same signature",
		ErrNoUserVisibility, sampled,
	)
}

// Validate is called to ensure that the connector is properly configured. It should exercise any API credentials
// to be sure that they are valid.
//
// The check is deliberately a diagnostic, not a gate on full account visibility: it samples
// one page of users and fails only when NO user's attributes are readable. A partially
// visible account (the per-user OWNERSHIP model with some users unowned) is a warning, not
// a failure - it is legitimate for a tenant to hand over a subset of users - and the
// per-user detection in userResource marks each affected user at sync time.
func (d *Connector) Validate(ctx context.Context) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	users, err := d.Client.ListUsers(ctx, "", resourcePageSize)
	if err != nil {
		return nil, fmt.Errorf("baton-snowflake: validation request failed: %w", err)
	}

	if len(users) == 0 {
		if d.Client.DiscoveryMode == snowflake.DiscoveryModeAccountUsage {
			return nil, fmt.Errorf(
				"baton-snowflake: SNOWFLAKE.ACCOUNT_USAGE.USERS returned no rows; check that the " +
					"connector is reading the account you expect and that the view is populated",
			)
		}
		return nil, fmt.Errorf(
			"baton-snowflake: SHOW USERS returned no users; the connector's role cannot see any " +
				"user in the account",
		)
	}

	visibility := inspectUserVisibility(users)
	switch {
	case visibility.None():
		return nil, noUserVisibilityError(d.Client.DiscoveryMode, visibility.Total)
	case visibility.Partial():
		l.Warn(
			"baton-snowflake: some users are only partially visible to the connector's role; "+
				"their login, email, and TYPE will sync blank, and because their TYPE is "+
				"suppressed a service account among them cannot be distinguished from a person. "+
				"Grant OWNERSHIP on these users, or use --discovery-mode=account_usage, to sync "+
				"them fully",
			zap.Int("sampled_users", visibility.Total),
			zap.Int("partially_visible_users", visibility.Blanked),
			zap.String("examples", visibility.describeBlanked()),
		)
	}

	return nil, nil
}

// New returns a new instance of the connector.
func New(ctx context.Context, cfg *config.Snowflake, _ *cli.ConnectorOpts) (connectorbuilder.ConnectorBuilderV2, []connectorbuilder.Opt, error) {
	if cfg.PrivateKeyPath == "" && len(cfg.PrivateKey) == 0 {
		return nil, nil, fmt.Errorf("private-key or private-key-path is required")
	}
	if cfg.PrivateKeyPath != "" && len(cfg.PrivateKey) > 0 {
		return nil, nil, fmt.Errorf("only one of private-key or private-key-path can be provided")
	}
	var privateKeyValue any
	if cfg.PrivateKeyPath != "" {
		var err error
		privateKeyValue, err = snowflake.ReadPrivateKey(cfg.PrivateKeyPath)
		if err != nil {
			return nil, nil, err
		}
	}
	if len(cfg.PrivateKey) > 0 {
		var err error
		privateKeyValue, err = snowflake.ParsePrivateKey(cfg.PrivateKey)
		if err != nil {
			return nil, nil, err
		}
	}

	var jwtConfig = snowflake.JWTConfig{
		AccountIdentifier: cfg.AccountIdentifier,
		UserIdentifier:    cfg.UserIdentifier,
		PrivateKeyValue:   privateKeyValue,
	}
	noAuth := uhttp.NoAuth{}
	baseHttpClient, err := noAuth.GetClient(ctx)
	if err != nil {
		return nil, nil, err
	}
	ts := snowflake.NewJWTTokenSource(&jwtConfig)
	ctx = context.WithValue(ctx, oauth2.HTTPClient, baseHttpClient)
	httpClient := oauth2.NewClient(ctx, ts)

	discoveryMode, err := snowflake.ParseDiscoveryMode(cfg.DiscoveryMode)
	if err != nil {
		return nil, nil, err
	}

	client, err := snowflake.New(cfg.AccountUrl, jwtConfig, httpClient,
		snowflake.WithWriteRole(cfg.WriteRole),
		snowflake.WithOrganizationRole(cfg.OrganizationRole),
		snowflake.WithDiscoveryMode(discoveryMode),
	)
	if err != nil {
		return nil, nil, err
	}

	warnIgnoredObjectResourceFlags(ctx, cfg)

	return &Connector{
		Client:              client,
		SyncSecrets:         cfg.SyncSecrets,
		IssueCredentials:    cfg.IssueCredentials,
		excludedDatabases:   cfg.ExcludedDatabases,
		syncObjectResources: cfg.SyncObjectResources,
	}, nil, nil
}

// warnIgnoredObjectResourceFlags logs the two combinations in which turning object resources
// off silently makes another setting a no-op.
//
// These cannot be expressed as SDK field relationships: all four relationship helpers are
// presence-based, and sync-object-resources is a bool with a default, so it is always
// "present". A log line at startup is the available signal.
func warnIgnoredObjectResourceFlags(ctx context.Context, cfg *config.Snowflake) {
	if cfg.SyncObjectResources {
		return
	}
	l := ctxzap.Extract(ctx)
	if len(cfg.ExcludedDatabases) > 0 {
		l.Warn(
			"baton-snowflake: --excluded-databases is ignored because --sync-object-resources "+
				"is false; no database, schema, or table is enumerated at all, so there is "+
				"nothing for the filter to exclude",
			zap.Strings("excluded_databases", cfg.ExcludedDatabases),
		)
	}
	if cfg.SyncSecrets {
		l.Warn(
			"baton-snowflake: --sync-secrets is set but Snowflake secrets are database-scoped, " +
				"so no secret is synced while --sync-object-resources is false. User-scoped RSA " +
				"public keys are unaffected and still sync",
		)
	}
}
