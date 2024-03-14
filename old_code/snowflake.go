package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"gitlab.com/ductone/c1/pkg/connector"
	"gitlab.com/ductone/c1/pkg/envconfig"
	"gitlab.com/ductone/c1/pkg/mkms"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	sf "github.com/snowflakedb/gosnowflake"
)

const (
	dbDriver         = "snowflake"
	publicRole       = "PUBLIC"
	resourceTypeRole = "role"
)

var (
	_ connector_v1.ConnectorServer     = (*Snowflake)(nil)
	_ connector_v1.EntitlementsServer  = (*Snowflake)(nil)
	_ connector_v1.GrantsServer        = (*Snowflake)(nil)
	_ connector_v1.ResourceTypesServer = (*Snowflake)(nil)
	_ connector_v1.ResourcesServer     = (*Snowflake)(nil)
	_ connector_v1.UsersServer         = (*Snowflake)(nil)
)

type Snowflake struct {
	db      *sql.DB
	account string
	role    string
}

func NewSnowflakeConnectorFromEnv(ctx context.Context, envConfig *mdapp_v1.EnvConfig, opener mkms.Opener) (*Snowflake, error) {
	cfg, err := envconfig.EnvConfigToInternalMap(ctx, builtin_connector.SnowflakeConfigSchema, envConfig, builtin_connector.SnowflakeID, opener)
	if err != nil {
		return nil, err
	}

	account := cfg[builtin_connector.SnowflakeAccountFieldName]
	if account == "" {
		return nil, errors.New("snowflake: missing account")
	}
	user := cfg[builtin_connector.SnowflakeUserFieldName]
	if user == "" {
		return nil, errors.New("snowflake: missing user")
	}
	password := cfg[builtin_connector.SnowflakePasswordFieldName]
	if password == "" {
		return nil, errors.New("snowflake: missing password")
	}

	config := &sf.Config{
		Account:  account,
		User:     user,
		Password: password,
		Role:     publicRole,
	}

	snowflakeRoleFieldFromConfig, ok := cfg[builtin_connector.SnowflakeUserRoleFieldName]
	if ok && len(snowflakeRoleFieldFromConfig) > 0 {
		config.Role = snowflakeRoleFieldFromConfig
	}

	dsn, err := sf.DSN(config)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(dbDriver, dsn)
	if err != nil {
		return nil, err
	}

	return &Snowflake{
		db:      db,
		account: config.Account,
		role:    config.Role,
	}, nil
}

func (s *Snowflake) Info(ctx context.Context, req *connector_v1.ConnectorInfoRequest) (*connector_v1.ConnectorInfo, error) {
	_, err := s.db.ExecContext(ctx, "USE ROLE IDENTIFIER(?)", fmt.Sprintf(`"%s"`, s.role))
	if err != nil {
		return nil, fmt.Errorf("snowflake-connector: failed to assume required role: %w", err)
	}

	var annotations connector.Annotations
	annotations.WithAppMetadata(&annotationspb.AppMetadata{
		AccountId: s.account,
	})
	return &connector_v1.ConnectorInfo{
		DisplayName: "Snowflake",
		Description: "",
		Version:     "0.0.1",
		Annotations: annotations,
	}, nil
}

func (s *Snowflake) userStatus(ctx context.Context, disabled bool) *connector_v1.UserStatus {
	status := &connector_v1.UserStatus{
		Status: connector_v1.UserStatus_STATUS_ENABLED,
	}
	if disabled {
		status.Status = connector_v1.UserStatus_STATUS_DISABLED
	}
	return status
}

func (s *Snowflake) ListUsers(ctx context.Context, req *connector_v1.ListUsersRequest) (*connector_v1.ListUsersResponse, error) {
	type userRow struct {
		loginName   string
		displayName string
		email       string
		disabled    bool
	}
	var ignoreColumn sql.RawBytes

	rows, err := s.db.QueryContext(ctx, "SHOW USERS") //nolint:execinquery // Unclear of the semantics of snowflake
	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var users []*userRow

	for rows.Next() {
		var user userRow

		// TODO(aaron): "SHOW" statements can't select on particular columns (and limited to 10k results).
		// Other options could be using RESULT_SCAN or querying info_schema.
		// See: https://docs.snowflake.com/en/sql-reference/sql/show-users.html
		//      https://docs.snowflake.com/en/sql-reference/info-schema.html
		// However, these require an active 'warehouse'. I'd generally think you should be able to review access regardless
		// of whether you have active warehouses. Hence the kinda hacky approach to ignoring extra columns.
		// For now, only scan columns we care about.
		cols := make([]interface{}, len(columns))
		for i := 0; i < len(columns); i++ {
			switch columns[i] {
			case "login_name":
				cols[i] = &user.loginName
			case "display_name":
				cols[i] = &user.displayName
			case "email":
				cols[i] = &user.email
			case "disabled":
				cols[i] = &user.disabled
			default:
				cols[i] = &ignoreColumn
			}
		}

		err := rows.Scan(cols...)
		if err != nil {
			return nil, err
		}
		users = append(users, &user)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}

	rv := make([]*connector_v1.User, 0, len(users))
	for _, user := range users {
		displayName := user.displayName
		if displayName == "" {
			displayName = user.loginName
		}
		rv = append(rv, &connector_v1.User{
			Id:          strings.ToUpper(user.loginName),
			Email:       user.email,
			DisplayName: displayName,
			Status:      s.userStatus(ctx, user.disabled),
		})
	}

	// TODO(aaron):pagination? Can't do with SHOW commands (no LIMIT parameter)

	return &connector_v1.ListUsersResponse{
		List: rv,
	}, nil
}

func (s *Snowflake) ListResourceTypes(ctx context.Context, req *connector_v1.ListResourceTypesRequest) (*connector_v1.ListResourceTypesResponse, error) {
	return &connector_v1.ListResourceTypesResponse{
		List: []*connector_v1.ResourceType{
			{
				Id:          resourceTypeRole,
				DisplayName: cases.Title(language.AmericanEnglish).String(resourceTypeRole),
			},
		},
	}, nil
}

func (s *Snowflake) ListResources(ctx context.Context, req *connector_v1.ListResourcesRequest) (*connector_v1.ListResourcesResponse, error) {
	type roleRow struct {
		name    string
		comment string
	}
	var ignoreColumn sql.RawBytes

	rv := make([]*connector_v1.Resource, 0)

	switch req.ResourceType.Id {
	case resourceTypeRole:
		rows, err := s.db.QueryContext(ctx, "SHOW ROLES") //nolint:execinquery // Unclear of the semantics of snowflake
		if err != nil {
			return nil, err
		}

		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		var roles []*roleRow

		for rows.Next() {
			var role roleRow

			// TODO(aaron): See prev comment about SHOW limitations.
			cols := make([]interface{}, len(columns))
			for i := 0; i < len(columns); i++ {
				switch columns[i] {
				case "name":
					cols[i] = &role.name
				case "comment":
					cols[i] = &role.comment
				default:
					cols[i] = &ignoreColumn
				}
			}

			err := rows.Scan(cols...)
			if err != nil {
				return nil, err
			}
			roles = append(roles, &role)
		}
		if rows.Err() != nil {
			return nil, rows.Err()
		}

		for _, role := range roles {
			rv = append(rv, &connector_v1.Resource{
				Id:           role.name,
				ResourceType: req.ResourceType,
				DisplayName:  role.name,
			})
		}
	default:
		return nil, fmt.Errorf("snowflake-connector: invalid resource type: '%s'", req.ResourceType.Id)
	}

	// TODO(aaron):pagination? Can't do with SHOW commands (no LIMIT parameter)

	return &connector_v1.ListResourcesResponse{
		List: rv,
	}, nil
}

func (s *Snowflake) ListEntitlements(ctx context.Context, req *connector_v1.ListEntitlementsRequest) (*connector_v1.ListEntitlementsResponse, error) {
	switch req.Resource.ResourceType.Id {
	case resourceTypeRole:
		return &connector_v1.ListEntitlementsResponse{
			List: []*connector_v1.Entitlement{
				{
					Id:          fmt.Sprintf("role:%s", req.Resource.Id),
					DisplayName: fmt.Sprintf("Has the %s role in Snowflake", req.Resource.DisplayName),
					Resource:    req.Resource,
					Slug:        "member",
				},
			},
			NextPageToken: "",
		}, nil
	default:
		return nil, fmt.Errorf("list entitlements not implemented for resource type %s", req.Resource.ResourceType.Id)
	}
}

func (s *Snowflake) ListGrants(ctx context.Context, req *connector_v1.ListGrantsRequest) (*connector_v1.ListGrantsResponse, error) {
	type grantRow struct {
		user string
	}
	var ignoreColumn sql.RawBytes

	switch req.Resource.ResourceType.Id {
	case resourceTypeRole:
		rows, err := s.db.QueryContext(ctx, `SHOW GRANTS OF ROLE IDENTIFIER(?)`, fmt.Sprintf(`"%s"`, req.Resource.Id)) //nolint:execinquery  // Unclear of the semantics of snowflake
		if err != nil {
			return nil, err
		}

		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		var grants []*grantRow

		for rows.Next() {
			var grant grantRow

			// TODO(aaron): See prev comment about SHOW limitations.
			cols := make([]interface{}, len(columns))
			for i := 0; i < len(columns); i++ {
				switch columns[i] {
				case "grantee_name":
					cols[i] = &grant.user
				default:
					cols[i] = &ignoreColumn
				}
			}

			err := rows.Scan(cols...)
			if err != nil {
				return nil, err
			}
			grants = append(grants, &grant)
		}
		if rows.Err() != nil {
			return nil, rows.Err()
		}

		entitlement := &connector_v1.Entitlement{
			Id:       fmt.Sprintf("role:%s", req.Resource.Id),
			Resource: req.Resource,
		}

		var rv []*connector_v1.Grant
		for _, grant := range grants {
			rv = append(rv, &connector_v1.Grant{
				Id:          connector.GrantID(entitlement, grant.user),
				Entitlement: entitlement,
				Principal: &connector_v1.Principal{
					TypeId: connector.PrincipalTypeUser,
					Id:     strings.ToUpper(grant.user),
				},
			})
		}

		// TODO(aaron):pagination? Can't do this for SHOW commands (no LIMIT )
		return &connector_v1.ListGrantsResponse{
			List: rv,
		}, nil

	default:
		return nil, fmt.Errorf("snowflake-connector: invalid grant resource: '%s'", req.Resource.Id)
	}
}

func (s *Snowflake) ListFieldOptions(ctx context.Context, request *connector_v1.ListFieldOptionsRequest) (*connector_v1.ListFieldOptionsResponse, error) {
	switch request.FieldName {
	case bc.SnowflakeUserRoleFieldName:
		roles, err := s.ListResources(ctx, &connector_v1.ListResourcesRequest{
			ResourceType: &connector_v1.ResourceType{
				Id: resourceTypeRole,
			},
		})
		if err != nil {
			return nil, err
		}

		options := make([]*connector_v1.Option, 0, len(roles.List))
		for _, role := range roles.List {
			o := &connector_v1.Option{
				Id:    role.Id,
				Name:  role.DisplayName,
				Value: role.Id,
			}
			options = append(options, o)
		}
		return &connector_v1.ListFieldOptionsResponse{
			List: options,
		}, nil
	default:
		return nil, fmt.Errorf("Snowflake: unknown config option field name `%s`", request.FieldName)
	}
}
