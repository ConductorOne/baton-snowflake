package connector

import (
	"context"
	"fmt"
	"io"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/conductorone/baton-snowflake/pkg/config"
	snowflake "github.com/conductorone/baton-snowflake/pkg/snowflake"
)

type Connector struct {
	Client      *snowflake.Client
	syncSecrets bool
}

// ResourceSyncers returns a ResourceSyncerV2 for each resource type that should be synced from the upstream service.
func (d *Connector) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncerV2 {
	builders := []connectorbuilder.ResourceSyncerV2{
		newUserBuilder(d.Client, d.syncSecrets),
		newAccountRoleBuilder(d.Client),
		newDatabaseBuilder(d.Client, d.syncSecrets),
		newTableBuilder(d.Client),
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
		Description: "Connector syncing users, databases, tables, and account roles from Snowflake.",
		AccountCreationSchema: &v2.ConnectorAccountCreationSchema{
			FieldMap: map[string]*v2.ConnectorAccountCreationSchema_Field{
				"name": {
					DisplayName: "User Name",
					Required:    true,
					Description: "The name of the user (required). Can be provided via login or profile.name",
					Placeholder: "username",
					Order:       0,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"login": {
					DisplayName: "Login Name",
					Required:    false,
					Description: "The login name for the user (defaults to email if not provided)",
					Placeholder: "user@example.com",
					Order:       1,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"display_name": {
					DisplayName: "Display Name",
					Required:    false,
					Description: "The display name for the user",
					Placeholder: "John Doe",
					Order:       2,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"first_name": {
					DisplayName: "First Name",
					Required:    false,
					Description: "The first name of the user",
					Placeholder: "John",
					Order:       3,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"last_name": {
					DisplayName: "Last Name",
					Required:    false,
					Description: "The last name of the user",
					Placeholder: "Doe",
					Order:       4,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"email": {
					DisplayName: "Email",
					Required:    false,
					Description: "The email address for the user",
					Placeholder: "user@example.com",
					Order:       5,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"comment": {
					DisplayName: "Comment",
					Required:    false,
					Description: "A comment or description for the user",
					Placeholder: "User description",
					Order:       6,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"disabled": {
					DisplayName: "Disabled",
					Required:    false,
					Description: "Whether the user account should be disabled",
					Order:       8,
					Field: &v2.ConnectorAccountCreationSchema_Field_BoolField{
						BoolField: &v2.ConnectorAccountCreationSchema_BoolField{},
					},
				},
				"must_change_password": {
					DisplayName: "Must Change Password",
					Required:    false,
					Description: "Whether the user must change their password on next login",
					Order:       9,
					Field: &v2.ConnectorAccountCreationSchema_Field_BoolField{
						BoolField: &v2.ConnectorAccountCreationSchema_BoolField{},
					},
				},
				"default_warehouse": {
					DisplayName: "Default Warehouse",
					Required:    false,
					Description: "The default warehouse to use when this user starts a session",
					Placeholder: "COMPUTE_WH",
					Order:       10,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"default_namespace": {
					DisplayName: "Default Namespace",
					Required:    false,
					Description: "The default namespace to use when this user starts a session",
					Placeholder: "DATABASE.SCHEMA",
					Order:       11,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"default_role": {
					DisplayName: "Default Role",
					Required:    false,
					Description: "The default role to use when this user starts a session",
					Placeholder: "PUBLIC",
					Order:       12,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
				"default_secondary_roles": {
					DisplayName: "Default Secondary Roles",
					Required:    false,
					Description: "The default secondary roles of this user to use when starting a session. Valid values: ALL or NONE. Default is ALL.",
					Placeholder: "ALL",
					Order:       13,
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
				},
			},
		},
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
func New(ctx context.Context, cfg *config.Snowflake) (*Connector, error) {
	if cfg.PrivateKeyPath == "" && len(cfg.PrivateKey) == 0 {
		return nil, fmt.Errorf("private-key or private-key-path is required")
	}
	if cfg.PrivateKeyPath != "" && len(cfg.PrivateKey) > 0 {
		return nil, fmt.Errorf("only one of private-key or private-key-path can be provided")
	}
	var privateKeyValue any
	if cfg.PrivateKeyPath != "" {
		var err error
		privateKeyValue, err = snowflake.ReadPrivateKey(cfg.PrivateKeyPath)
		if err != nil {
			return nil, err
		}
	}
	if len(cfg.PrivateKey) > 0 {
		var err error
		privateKeyValue, err = snowflake.ParsePrivateKey(cfg.PrivateKey)
		if err != nil {
			return nil, err
		}
	}

	var jwtConfig = snowflake.JWTConfig{
		AccountIdentifier: cfg.AccountIdentifier,
		UserIdentifier:    cfg.UserIdentifier,
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

	client, err := snowflake.New(cfg.AccountUrl, jwtConfig, httpClient)
	if err != nil {
		return nil, err
	}

	return &Connector{
		Client:      client,
		syncSecrets: cfg.SyncSecrets,
	}, nil
}
