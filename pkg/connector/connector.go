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
	Client *snowflake.Client
}

// ResourceSyncers returns a ResourceSyncer for each resource type that should be synced from the upstream service.
func (d *Connector) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	return []connectorbuilder.ResourceSyncer{
		newUserBuilder(d.Client),
		newAccountRoleBuilder(d.Client),
		newDatabaseBuilder(d.Client),
	}
}

// Asset takes an input AssetRef and attempts to fetch it using the connector's authenticated http client
// It streams a response, always starting with a metadata object, following by chunked payloads for the asset.
func (d *Connector) Asset(ctx context.Context, asset *v2.AssetRef) (string, io.ReadCloser, error) {
	return "", nil, nil
}

// Metadata returns metadata about the connector.
func (d *Connector) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{
		DisplayName: "My Baton Connector",
		Description: "The template implementation of a baton connector",
	}, nil
}

// Validate is called to ensure that the connector is properly configured. It should exercise any API credentials
// to be sure that they are valid.
func (d *Connector) Validate(ctx context.Context) (annotations.Annotations, error) {
	users, _, err := d.Client.ListUsers(ctx, 0, 1)
	if err != nil {
		return nil, err
	}

	if len(users) == 0 {
		return nil, fmt.Errorf("no users found")
	}

	return nil, nil
}

// New returns a new instance of the connector.
func New(ctx context.Context, accountUrl, accountIdentifier, userIdentifier, publicKeyFingerPrint, privateKeyPath string) (*Connector, error) {
	privateKeyValue, err := snowflake.ReadPrivateKey(privateKeyPath)
	if err != nil {
		return nil, err
	}

	var jwtConfig = snowflake.JWTConfig{
		AccountIdentifier:    accountIdentifier,
		UserIdentifier:       userIdentifier,
		PublicKeyFingerPrint: publicKeyFingerPrint,
		PrivateKeyValue:      privateKeyValue,
	}
	token, err := jwtConfig.GenerateBearerTokenRsaKey(privateKeyPath)
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
		Client: client,
	}, nil
}
