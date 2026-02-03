package main

import (
	"context"

	cfg "github.com/conductorone/baton-snowflake/pkg/config"
	"github.com/conductorone/baton-snowflake/pkg/connector"
	"github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorrunner"
)

var version = "dev"

func main() {
	ctx := context.Background()
	config.RunConnector(ctx, "baton-snowflake", version, cfg.Config, connector.NewConnector, connectorrunner.WithSessionStoreEnabled(), connectorrunner.WithDefaultCapabilitiesConnectorBuilder(&connector.Connector{}))
}
