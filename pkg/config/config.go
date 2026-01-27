package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
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
