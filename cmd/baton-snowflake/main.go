package main

import (
	"context"
	"fmt"
	"os"

	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	configSchema "github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-snowflake/pkg/connector"
)

const (
	version       = "dev"
	connectorName = "baton-snowflake"
)

func main() {
	ctx := context.Background()
	_, cmd, err := configSchema.DefineConfiguration(ctx,
		connectorName,
		getConnector,
		configurationSchema,
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
	cb, err := connector.New(
		ctx,
		cfg.GetString(AccountUrlField.FieldName),
		cfg.GetString(AccountIdentifierField.FieldName),
		cfg.GetString(UserIdentifierField.FieldName),
		cfg.GetString(PublicKeyFingerprintField.FieldName),
		cfg.GetString(PrivateKeyPathField.FieldName),
		cfg.GetString(PrivateKeyField.FieldName),
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
