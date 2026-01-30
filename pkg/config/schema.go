//go:generate go run gen/gen.go

package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

var (
	AccountUrlField = field.StringField(
		"account-url",
		field.WithDisplayName("Account URL"),
		field.WithRequired(true),
		field.WithDescription("Account URL."),
	)
	AccountIdentifierField = field.StringField(
		"account-identifier",
		field.WithDisplayName("Account Identifier"),
		field.WithRequired(true),
		field.WithDescription("Account Identifier."),
	)
	UserIdentifierField = field.StringField(
		"user-identifier",
		field.WithDisplayName("User Identifier"),
		field.WithRequired(true),
		field.WithDescription("User Identifier."),
	)
	PrivateKeyPathField = field.StringField(
		"private-key-path",
		field.WithDisplayName("Private Key Path"),
		field.WithDescription("Private Key Path."),
		field.WithIsSecret(true),
	)
	PrivateKeyField = field.StringField(
		"private-key",
		field.WithDisplayName("Private Key"),
		field.WithDescription("Private Key (PEM format)."),
		field.WithIsSecret(true),
	)
	SyncSecretsField = field.BoolField(
		"sync-secrets",
		field.WithDisplayName("Sync Secrets"),
		field.WithDescription("Whether to sync secrets or not"),
		field.WithDefaultValue(false),
	)

	Config = field.NewConfiguration(
		[]field.SchemaField{
			AccountIdentifierField,
			AccountUrlField,
			PrivateKeyField,
			PrivateKeyPathField,
			UserIdentifierField,
			SyncSecretsField,
		},
		field.FieldsMutuallyExclusive(
			PrivateKeyPathField,
			PrivateKeyField,
		),
		field.FieldsAtLeastOneUsed(
			PrivateKeyPathField,
			PrivateKeyField,
		),
		field.WithConnectorDisplayName("Snowflake"),
		field.WithHelpUrl("/docs/baton/snowflake"),
		field.WithIconUrl("/static/app-icons/snowflake.svg"),
	)
)
