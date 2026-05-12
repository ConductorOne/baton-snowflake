package main

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
	cfg "github.com/conductorone/baton-snowflake/pkg/config"
	"github.com/conductorone/baton-snowflake/pkg/connector"
)

const (
	version       = "dev"
	connectorName = "baton-snowflake"
)

func main() {
	ctx := context.Background()
	config.RunConnector(ctx, connectorName, version, cfg.ConfigurationSchema(), connector.New, connectorrunner.WithSessionStoreEnabled())
}
