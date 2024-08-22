package main

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
	PublicKeyFingerprintField = field.StringField(
		"public-key-fingerprint",
		field.WithRequired(true),
		field.WithDescription("Public Key Fingerprint."),
	)
	PrivateKeyPathField = field.StringField(
		"private-key-path",
		field.WithDescription("Private Key Path."),
	)
	PrivateKeyField = field.StringField(
		"private-key",
		field.WithDescription("Private Key (PEM format)."),
	)
	configurationSchema = field.NewConfiguration(
		[]field.SchemaField{
			AccountIdentifierField,
			AccountUrlField,
			PrivateKeyField,
			PrivateKeyPathField,
			PublicKeyFingerprintField,
			UserIdentifierField,
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
