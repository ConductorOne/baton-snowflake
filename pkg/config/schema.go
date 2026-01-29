//go:generate go run gen/gen.go

package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

var (
	AccountUrlField = field.StringField(
		"account-url",
		field.WithRequired(true),
		field.WithDescription("Account URL."),
	)
	AccountIdentifierField = field.StringField(
		"account-identifier",
		field.WithRequired(true),
		field.WithDescription("Account Identifier."),
	)
	UserIdentifierField = field.StringField(
		"user-identifier",
		field.WithRequired(true),
		field.WithDescription("User Identifier."),
	)
	PrivateKeyPathField = field.StringField(
		"private-key-path",
		field.WithDescription("Private Key Path."),
	)
	PrivateKeyField = field.StringField(
		"private-key",
		field.WithDescription("Private Key (PEM format)."),
	)
	SyncSecretsField = field.BoolField(
		"sync-secrets",
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
	)
)
