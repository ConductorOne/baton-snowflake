package connector

import (
	"context"
	"fmt"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

const (
	grantedToRole = "ROLE"
	grantedToUser = "USER"
)

type tableBuilder struct {
	client *snowflake.Client
}

func (o *tableBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return tableResourceType
}

func tableResource(_ context.Context, table *snowflake.Table, id *v2.ResourceId) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"name":          table.Name,
		"schema_name":   table.SchemaName,
		"database_name": table.DatabaseName,
		"kind":          table.Kind,
		"comment":       table.Comment,
		"owner":         table.Owner,
		"created_on":    table.CreatedOn.Format("2006-01-02 15:04:05.999"),
	}

	tableTraits := []rs.AppTraitOption{
		rs.WithAppProfile(profile),
	}

	// Use a unique identifier that includes database and schema
	tableId := fmt.Sprintf("%s.%s.%s", table.DatabaseName, table.SchemaName, table.Name)

	resource, err := rs.NewAppResource(
		table.Name,
		tableResourceType,
		tableId,
		tableTraits,
		rs.WithParentResourceID(id),
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (o *tableBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, opts rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentResourceID == nil {
		return nil, &rs.SyncOpResults{}, nil
	}

	if parentResourceID.ResourceType != databaseResourceType.Id {
		return nil, nil, wrapError(fmt.Errorf("invalid parent resource type: %s", parentResourceID.ResourceType), "invalid parent resource type")
	}

	databaseName := parentResourceID.Resource

	// SHOW TABLES IN ACCOUNT returns tables from all DBs; we paginate, filter to this database, and return only its tables.
	var allForDB []*v2.Resource
	cursor := opts.PageToken.Token
	const accountPageSize = 200
	for {
		tables, nextCursor, _, err := o.client.ListTablesInAccount(ctx, cursor, accountPageSize)
		if err != nil {
			return nil, nil, wrapError(err, "failed to list tables in account")
		}
		for i := range tables {
			t := &tables[i]
			if strings.EqualFold(t.DatabaseName, databaseName) {
				resource, err := tableResource(ctx, t, parentResourceID)
				if err != nil {
					return nil, nil, wrapError(err, "failed to create table resource")
				}
				allForDB = append(allForDB, resource)
			}
		}
		if nextCursor == "" {
			break
		}
		cursor = nextCursor
	}

	return allForDB, &rs.SyncOpResults{}, nil
}

// Entitlements returns entitlements for tables.
func (o *tableBuilder) Entitlements(ctx context.Context, resource *v2.Resource, opts rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	var rv []*v2.Entitlement

	// Parse the resource ID which is in format "database.schema.table"
	parts := strings.Split(resource.Id.Resource, ".")
	if len(parts) != 3 {
		return nil, nil, wrapError(fmt.Errorf("invalid table resource ID format: %s", resource.Id.Resource), "expected format: database.schema.table")
	}

	databaseName := parts[0]
	schemaName := parts[1]
	tableName := parts[2]

	// Get all grants on the table to determine which entitlements exist
	tableGrants, err := o.client.ListTableGrants(ctx, databaseName, schemaName, tableName)
	if err != nil {
		// If we can't get grants, still return the owner entitlement
		// Log the error but continue to return at least the owner entitlement
		l := ctxzap.Extract(ctx)
		l.Debug("snowflake-connector: failed to list table grants, returning owner entitlement only",
			zap.String("table", resource.Id.Resource),
			zap.Error(err))
		rv = append(rv, ent.NewAssignmentEntitlement(
			resource,
			ownerEntitlement,
			ent.WithGrantableTo(userResourceType),
			ent.WithDescription(fmt.Sprintf("Is owned by %s", resource.DisplayName)),
			ent.WithDisplayName(fmt.Sprintf("Is owner of %s", resource.DisplayName)),
		))
		return rv, &rs.SyncOpResults{}, nil
	}

	// Collect unique privileges to create entitlements
	privileges := make(map[string]bool)
	for _, tg := range tableGrants {
		if tg.GrantedTo == grantedToRole || tg.GrantedTo == grantedToUser {
			privilege := strings.ToLower(tg.Privilege)
			privileges[privilege] = true
		}
	}

	// Create entitlements for each unique privilege
	for privilege := range privileges {
		rv = append(rv, ent.NewAssignmentEntitlement(
			resource,
			privilege,
			ent.WithGrantableTo(userResourceType, accountRoleResourceType),
			ent.WithDescription(fmt.Sprintf("Has %s privilege on %s", privilege, resource.DisplayName)),
			ent.WithDisplayName(fmt.Sprintf("%s on %s", strings.ToUpper(privilege), resource.DisplayName)),
		))
	}

	// Always add owner entitlement
	rv = append(rv, ent.NewAssignmentEntitlement(
		resource,
		ownerEntitlement,
		ent.WithGrantableTo(userResourceType),
		ent.WithDescription(fmt.Sprintf("Is owned by %s", resource.DisplayName)),
		ent.WithDisplayName(fmt.Sprintf("Is owner of %s", resource.DisplayName)),
	))

	return rv, &rs.SyncOpResults{}, nil
}

// Grants returns grants for tables, showing all permissions granted on each table.
func (o *tableBuilder) Grants(ctx context.Context, resource *v2.Resource, opts rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	l := ctxzap.Extract(ctx)

	// Parse the resource ID which is in format "database.schema.table"
	parts := strings.Split(resource.Id.Resource, ".")
	if len(parts) != 3 {
		return nil, nil, wrapError(fmt.Errorf("invalid table resource ID format: %s", resource.Id.Resource), "expected format: database.schema.table")
	}

	databaseName := parts[0]
	schemaName := parts[1]
	tableName := parts[2]

	// Get all grants on the table using SHOW GRANTS ON TABLE
	tableGrants, err := o.client.ListTableGrants(ctx, databaseName, schemaName, tableName)
	if err != nil {
		return nil, nil, wrapError(err, "failed to list table grants")
	}

	if len(tableGrants) == 0 {
		return nil, &rs.SyncOpResults{}, nil
	}

	var grants []*v2.Grant

	// Process each grant
	for _, tg := range tableGrants {
		entitlementID := strings.ToLower(tg.Privilege)
		var principalResource *v2.Resource
		var roleNameForExpandable string // set when principal is an account role (incl. system roles)

		switch tg.GrantedTo {
		case grantedToRole:
			// Process grants to roles (system-defined like ACCOUNTADMIN, SYSADMIN, or custom)
			owner, _, err := o.client.GetAccountRole(ctx, tg.GranteeName)
			if err != nil {
				l.Debug("snowflake-connector: account role not found for grant",
					zap.String("role", tg.GranteeName),
					zap.String("privilege", tg.Privilege),
					zap.Error(err))
				continue
			}

			if owner == nil {
				// Skip if role is not found (e.g., system roles like SNOWFLAKE)
				l.Debug("snowflake-connector: skipping grant for role not found",
					zap.String("role", tg.GranteeName),
					zap.String("privilege", tg.Privilege))
				continue
			}

			principalResource, err = accountRoleResource(owner)
			if err != nil {
				l.Debug("snowflake-connector: failed to create role resource",
					zap.String("role", owner.Name),
					zap.Error(err))
				continue
			}
			roleNameForExpandable = owner.Name
		case grantedToUser:
			// Process grants to users directly
			user, _, err := o.client.GetUser(ctx, tg.GranteeName)
			if err != nil {
				l.Debug("snowflake-connector: user not found for grant",
					zap.String("user", tg.GranteeName),
					zap.String("privilege", tg.Privilege),
					zap.Error(err))
				continue
			}

			if user == nil {
				l.Debug("snowflake-connector: skipping grant for user not found",
					zap.String("user", tg.GranteeName),
					zap.String("privilege", tg.Privilege))
				continue
			}

			principalResource, err = userResource(ctx, user, false)
			if err != nil {
				l.Debug("snowflake-connector: failed to create user resource",
					zap.String("user", user.Username),
					zap.Error(err))
				continue
			}
		default:
			// Skip other grant types (e.g., SHARE)
			l.Debug("snowflake-connector: skipping grant with unsupported GrantedTo type",
				zap.String("granted_to", tg.GrantedTo),
				zap.String("grantee", tg.GranteeName),
				zap.String("privilege", tg.Privilege))
			continue
		}

		// Create grant for this privilege; add expandable when principal is a role so the UI can show who has that role
		var grantOpts []grant.GrantOption
		if roleNameForExpandable != "" {
			grantOpts = append(grantOpts, grant.WithAnnotation(
				&v2.GrantExpandable{
					EntitlementIds:  []string{fmt.Sprintf("account_role:%s:%s", roleNameForExpandable, assignedEntitlement)},
					Shallow:         true,
					ResourceTypeIds: []string{accountRoleResourceType.Id, userResourceType.Id},
				},
			))
		}
		g := grant.NewGrant(resource, entitlementID, principalResource.Id, grantOpts...)
		grants = append(grants, g)
	}

	// Also check for owner grant if owner is a valid role
	table, _, err := o.client.GetTable(ctx, databaseName, schemaName, tableName)
	if err == nil && table.Owner != "" && table.Owner != "SNOWFLAKE" {
		owner, _, err := o.client.GetAccountRole(ctx, table.Owner)
		if err == nil && owner != nil {
			roleResource, err := accountRoleResource(owner)
			if err == nil {
				// Check if we already have a grant for this role (Entitlement.Id is resourceType:resourceId:permission)
				ownerEntitlementID := fmt.Sprintf("%s:%s:%s", tableResourceType.Id, resource.Id.Resource, ownerEntitlement)
				hasOwnerGrant := false
				for _, g := range grants {
					if g.Principal.Id.Resource == owner.Name && g.Entitlement.Id == ownerEntitlementID {
						hasOwnerGrant = true
						break
					}
				}

				if !hasOwnerGrant {
					grants = append(grants, grant.NewGrant(
						resource,
						ownerEntitlement,
						roleResource.Id,
						grant.WithAnnotation(
							&v2.GrantExpandable{
								EntitlementIds:  []string{fmt.Sprintf("account_role:%s:%s", owner.Name, assignedEntitlement)},
								Shallow:         true,
								ResourceTypeIds: []string{accountRoleResourceType.Id, userResourceType.Id},
							},
						),
					))
				}
			}
		}
	}

	return grants, &rs.SyncOpResults{}, nil
}

func newTableBuilder(client *snowflake.Client) *tableBuilder {
	return &tableBuilder{
		client: client,
	}
}
