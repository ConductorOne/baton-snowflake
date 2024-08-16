package main

import (
	"context"
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	configSchema "github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-snowflake/pkg/connector"
)

const (
	version              = "dev"
	connectorName        = "baton-snowflake"
	accountUrl           = "account-url"
	accountIdentifier    = "account-identifier"
	userIdentifier       = "user-identifier"
	publicKeyFingerPrint = "public-key-fingerprint"
	privateKeyPath       = "private-key-path"
	privateKey           = "private-key"
)

var (
	AccountUrlField           = field.StringField(accountUrl, field.WithRequired(true), field.WithDescription("Account URL."))
	AccountIdentifierField    = field.StringField(accountIdentifier, field.WithRequired(true), field.WithDescription("Account Identifier."))
	UserIdentifierField       = field.StringField(userIdentifier, field.WithRequired(true), field.WithDescription("User Identifier."))
	PublicKeyFingerprintField = field.StringField(publicKeyFingerPrint, field.WithRequired(true), field.WithDescription("Public Key Fingerprint."))
	PrivateKeyPathField       = field.StringField(privateKeyPath, field.WithRequired(false), field.WithDescription("Private Key Path."))
	PrivateKeyField           = field.StringField(privateKey, field.WithRequired(false), field.WithDescription("Private Key (PEM format)."))
	configurationFields       = []field.SchemaField{AccountUrlField, AccountIdentifierField, UserIdentifierField, PublicKeyFingerprintField, PrivateKeyPathField, PrivateKeyField}
)

func main() {
	ctx := context.Background()
	_, cmd, err := configSchema.DefineConfiguration(ctx,
		connectorName,
		getConnector,
		field.NewConfiguration(configurationFields),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	cmd.Version = version
	err = cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func getConnector(ctx context.Context, cfg *viper.Viper) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)
	cb, err := connector.New(ctx,
		cfg.GetString(accountUrl),
		cfg.GetString(accountIdentifier),
		cfg.GetString(userIdentifier),
		cfg.GetString(publicKeyFingerPrint),
		cfg.GetString(privateKeyPath),
		cfg.GetString(privateKey),
	)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	c, err := connectorbuilder.NewConnector(ctx, cb)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	return c, nil
}
