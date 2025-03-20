package connector

import (
	"context"
	"fmt"
	"io"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	snowflake "github.com/conductorone/baton-snowflake/pkg/snowflake"
)

type Connector struct {
	Client      *snowflake.Client
	syncSecrets bool
}

// ResourceSyncers returns a ResourceSyncer for each resource type that should be synced from the upstream service.
func (d *Connector) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	builders := []connectorbuilder.ResourceSyncer{
		newUserBuilder(d.Client, d.syncSecrets),
		newAccountRoleBuilder(d.Client),
		newDatabaseBuilder(d.Client, d.syncSecrets),
	}

	if d.syncSecrets {
		builders = append(
			builders,
			newSecretBuilder(d.Client),
			newRsaBuilder(d.Client),
		)
	}

	return builders
}

// Asset takes an input AssetRef and attempts to fetch it using the connector's authenticated http client
// It streams a response, always starting with a metadata object, following by chunked payloads for the asset.
func (d *Connector) Asset(ctx context.Context, asset *v2.AssetRef) (string, io.ReadCloser, error) {
	return "", nil, nil
}

// Metadata returns metadata about the connector.
func (d *Connector) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{
		DisplayName: "Baton Snowflake",
		Description: "Connector syncing users, databases and account roles from Snowflake.",
	}, nil
}

// Validate is called to ensure that the connector is properly configured. It should exercise any API credentials
// to be sure that they are valid.
func (d *Connector) Validate(ctx context.Context) (annotations.Annotations, error) {
	users, _, err := d.Client.ListUsers(ctx, "", 1)
	if err != nil {
		return nil, err
	}

	if len(users) == 0 {
		return nil, fmt.Errorf("no users found")
	}

	return nil, nil
}

// New returns a new instance of the connector.
func New(
	ctx context.Context,
	accountUrl,
	accountIdentifier,
	userIdentifier,
	privateKeyPath,
	privateKey string,
	syncSecrets bool,
) (*Connector, error) {
	if privateKeyPath == "" && privateKey == "" {
		return nil, fmt.Errorf("private-key or private-key-path is required")
	}
	if privateKeyPath != "" && privateKey != "" {
		return nil, fmt.Errorf("only one of private-key or private-key-path can be provided")
	}
	var privateKeyValue any
	if privateKeyPath != "" {
		var err error
		privateKeyValue, err = snowflake.ReadPrivateKey(privateKeyPath)
		if err != nil {
			return nil, err
		}
	}
	if privateKey != "" {
		var err error
		privateKeyValue, err = snowflake.ParsePrivateKey([]byte(privateKey))
		if err != nil {
			return nil, err
		}
	}

	var jwtConfig = snowflake.JWTConfig{
		AccountIdentifier: accountIdentifier,
		UserIdentifier:    userIdentifier,
		PrivateKeyValue:   privateKeyValue,
	}
	token, err := jwtConfig.GenerateBearerToken()
	if err != nil {
		return nil, err
	}
	httpClient, err := uhttp.NewBearerAuth(token).GetClient(ctx)
	if err != nil {
		return nil, err
	}

	client, err := snowflake.New(accountUrl, jwtConfig, httpClient)
	if err != nil {
		return nil, err
	}

	return &Connector{
		Client:      client,
		syncSecrets: syncSecrets,
	}, nil
}
