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
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

const (
	version           = "dev"
	connectorName     = "baton-snowflake"
	batonCacheDisable = "cache-disable"
	batonCacheTTL     = "cache-ttl"
	batonCacheMaxSize = "cache-max-size"
)

var (
	AccountUrl           = field.StringField(snowflake.AccountUrl, field.WithRequired(true), field.WithDescription("Account URL."))
	AccountIdentifier    = field.StringField(snowflake.AccountIdentifier, field.WithRequired(true), field.WithDescription("Account Identifier."))
	UserIdentifier       = field.StringField(snowflake.UserIdentifier, field.WithRequired(true), field.WithDescription("User Identifier."))
	PublicKeyFingerprint = field.StringField(snowflake.PublicKeyFingerPrint, field.WithRequired(true), field.WithDescription("Public Key Fingerprint."))
	PrivateKeyPath       = field.StringField(snowflake.PrivateKeyPath, field.WithRequired(false), field.WithDescription("Private Key Path."))
	PrivateKey           = field.StringField(snowflake.PrivateKey, field.WithRequired(false), field.WithDescription("Private Key (PEM format)."))
	configurationFields  = []field.SchemaField{AccountUrl, AccountIdentifier, UserIdentifier, PublicKeyFingerprint, PrivateKeyPath, PrivateKey}
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
	cb, err := connector.New(ctx, cfg)
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
