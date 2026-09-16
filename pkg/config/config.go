package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"

	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

var (
	AccountUrlField = field.StringField(
		"account-url",
		field.WithDisplayName("Account URL"),
		field.WithRequired(true),
		field.WithDescription("The full URL for your Snowflake account."),
	)
	AccountIdentifierField = field.StringField(
		"account-identifier",
		field.WithDisplayName("Account ID / Locator"),
		field.WithRequired(true),
		field.WithDescription("Your Snowflake account identifier. This can be either the Account ID (UUID format) or Account Locator (shorter identifier)."),
	)
	UserIdentifierField = field.StringField(
		"user-identifier",
		field.WithDisplayName("User Identifier"),
		field.WithRequired(true),
		field.WithDescription("The Snowflake username for the service account that will be used to authenticate."),
	)
	// PrivateKeyField: file upload for c1 UI.
	PrivateKeyField = field.FileUploadField(
		"private-key",
		[]string{".p8", ".pem", ".key"},
		field.WithDisplayName("Private Key"),
		field.WithDescription("Select the unencrypted private key file in PEM format."),
		field.WithIsSecret(true),
	)
	// PrivateKeyPathField: file path for CLI only.
	PrivateKeyPathField = field.StringField(
		"private-key-path",
		field.WithDisplayName("Private Key Path"),
		field.WithDescription("Path to the unencrypted private key file in PEM format (CLI only)."),
		field.WithIsSecret(true),
		field.WithExportTarget(field.ExportTargetCLIOnly),
	)
	SyncSecrets = field.BoolField(
		"sync-secrets",
		field.WithDisplayName("Sync Secrets"),
		field.WithDescription("Enable synchronization of Snowflake secrets. When enabled, the connector will sync secrets from your Snowflake account."),
		field.WithDefaultValue(false),
	)
	IssueCredentials = field.BoolField(
		"issue-credentials",
		field.WithDisplayName("Issue Credentials"),
		field.WithDescription(
			"Enable issuing Snowflake programmatic access tokens for existing users. Independent of "+
				"Sync Secrets: this also syncs the tokens it issues so they can be revoked, but no other secrets.",
		),
		field.WithDefaultValue(false),
	)
	ExcludedDatabases = field.StringSliceField(
		"excluded-databases",
		field.WithDisplayName("Excluded Databases"),
		field.WithDescription("Database names to exclude from sync (case-insensitive). Can be specified multiple times. When set, matching databases and all their tables are skipped entirely."),
	)
	// SyncObjectResources gates the object-level resource types as a group. Excluded
	// Databases is a connector-side filter that still requires the privileges to be
	// granted and still requires every database to be enumerated; this turns the whole
	// database and table surface off so a tenant never has to grant object-level
	// privileges at all.
	SyncObjectResources = field.BoolField(
		"sync-object-resources",
		field.WithDisplayName("Sync Databases and Tables"),
		field.WithDescription(
			"Sync database and table resources and their grants. Disable to run a "+
				"users, roles, and role-grants only sync, so no object-level privileges "+
				"(USAGE on databases and schemas, REFERENCES on tables) need to be granted to "+
				"the service account at all. Snowflake secrets are database-scoped and are not "+
				"synced while this is disabled.",
		),
		field.WithDefaultValue(true),
	)
	// WriteRoleField: the role the SQL/REST write paths run as. Defaults to Snowflake's
	// USERADMIN system role, which is the role the connector previously hard-coded.
	WriteRoleField = field.StringField(
		"write-role",
		field.WithDisplayName("Write Role"),
		field.WithDescription(
			"Snowflake role used for user lifecycle (create, delete, enable, disable) and "+
				"programmatic access token operations. Defaults to the USERADMIN system role. "+
				"Set this to a customer-defined role holding only CREATE USER on the account and "+
				"MODIFY on the in-scope users to avoid granting USERADMIN to the service account.",
		),
		field.WithDefaultValue(snowflake.UserAdminRole),
	)
	// OrganizationRoleField: the role organization-scoped reads run as. Defaults to
	// GLOBALORGADMIN, which is the role the connector previously hard-coded.
	OrganizationRoleField = field.StringField(
		"organization-role",
		field.WithDisplayName("Organization Role"),
		field.WithDescription(
			"Snowflake role used for organization-scoped reads (SHOW ORGANIZATION ACCOUNTS, "+
				"which backs license sync). Defaults to the GLOBALORGADMIN system role. Set this "+
				"to a delegated organization role to avoid granting the top-level org admin role "+
				"to the service account.",
		),
		field.WithDefaultValue(snowflake.GlobalOrgAdminRole),
	)
	// DiscoveryModeField selects the inventory read path. "show" preserves the existing
	// behavior; "account_usage" is the least-privilege path that needs no MANAGE GRANTS.
	DiscoveryModeField = field.StringField(
		"discovery-mode",
		field.WithDisplayName("Discovery Mode"),
		field.WithDescription(
			"How the connector discovers inventory. \"show\" (default) uses SHOW commands, which "+
				"only return objects the service account's role holds a privilege on - full "+
				"account visibility therefore needs per-object OWNERSHIP or account-level MANAGE "+
				"GRANTS. \"account_usage\" reads the SNOWFLAKE.ACCOUNT_USAGE views instead, which "+
				"needs no MANAGE GRANTS at all (only the ACCOUNT_USAGE SECURITY_VIEWER and "+
				"OBJECT_VIEWER database roles) but is subject to Snowflake's documented "+
				"ACCOUNT_USAGE latency of 90 minutes to 3 hours and requires a running warehouse.",
		),
		field.WithDefaultValue(string(snowflake.DiscoveryModeShow)),
		// The accepted values are declared as a rule so they reach the generated config
		// schema and are rejected at config-validation time rather than only inside
		// connector.New. ParseDiscoveryMode stays as defence in depth: it also folds case
		// and trims whitespace, which this rule does not.
		field.WithString(func(r *field.StringRuler) {
			r.In([]string{
				string(snowflake.DiscoveryModeShow),
				string(snowflake.DiscoveryModeAccountUsage),
			})
		}),
	)

	fieldRelationships = []field.SchemaFieldRelationship{
		field.FieldsMutuallyExclusive(
			PrivateKeyPathField,
			PrivateKeyField,
		),
		field.FieldsAtLeastOneUsed(
			PrivateKeyPathField,
			PrivateKeyField,
		),
	}

	configurationFields = []field.SchemaField{
		AccountIdentifierField,
		AccountUrlField,
		PrivateKeyField,
		PrivateKeyPathField,
		UserIdentifierField,
		SyncSecrets,
		IssueCredentials,
		ExcludedDatabases,
		SyncObjectResources,
		WriteRoleField,
		OrganizationRoleField,
		DiscoveryModeField,
	}

	Configuration = field.NewConfiguration(
		configurationFields,
		field.WithConstraints(fieldRelationships...),
		field.WithConnectorDisplayName("Snowflake"),
		field.WithHelpUrl("/docs/baton/snowflake-v2"),
		field.WithIconUrl("/static/app-icons/snowflake.svg"),
	)
)

func ConfigurationSchema() field.Configuration {
	return Configuration
}
