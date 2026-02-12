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
	grantedToRole     = "ROLE"
	grantedToUser     = "USER"
	privilegeOwner    = "ownership"
	defaultObjectKind = "TABLE"
)

func accountRoleAssignedEntitlementID(roleName string) string {
	return fmt.Sprintf("%s:%s:%s", accountRoleResourceType.Id, roleName, assignedEntitlement)
}

func addExpandableOpts(roleName string) []grant.GrantOption {
	if roleName == "" {
		return nil
	}
	return []grant.GrantOption{
		grant.WithAnnotation(
			&v2.GrantExpandable{
				EntitlementIds:  []string{accountRoleAssignedEntitlementID(roleName)},
				Shallow:         true,
				ResourceTypeIds: []string{accountRoleResourceType.Id, userResourceType.Id},
			},
		),
	}
}

func getTableProfileField(resource *v2.Resource, field string) interface{} {
	appTrait, err := rs.GetAppTrait(resource)
	if err != nil || appTrait.GetProfile() == nil {
		return nil
	}
	if f := appTrait.GetProfile().GetFields()[field]; f != nil {
		return f.AsInterface()
	}
	return nil
}

func getObjectKind(resource *v2.Resource) string {
	if v := getTableProfileField(resource, "kind"); v != nil {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return defaultObjectKind
}

func (o *tableBuilder) isDBSharedOrSystem(ctx context.Context, resource *v2.Resource, databaseName string) bool {
	if v := getTableProfileField(resource, "database_is_shared_system"); v != nil {
		switch val := v.(type) {
		case bool:
			return val
		case float64:
			return val != 0
		case string:
			return val == "true" || val == "1"
		}
	}
	db, resp, err := o.client.GetDatabase(ctx, databaseName)
	if err != nil || db == nil || snowflake.IsUnprocessableEntity(resp, err) {
		return true
	}
	return db.IsSharedOrSystem()
}

type tableBuilder struct {
	client *snowflake.Client
}

func (o *tableBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return tableResourceType
}

func tableResource(_ context.Context, table *snowflake.Table, id *v2.ResourceId, isSharedOrSystemDB bool) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"name":                      table.Name,
		"schema_name":               table.SchemaName,
		"database_name":             table.DatabaseName,
		"kind":                      table.Kind,
		"comment":                   table.Comment,
		"owner":                     table.Owner,
		"created_on":                table.CreatedOn.Format("2006-01-02 15:04:05.999"),
		"database_is_shared_system": isSharedOrSystemDB,
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

	parentDB, _, err := o.client.GetDatabase(ctx, databaseName)
	if err != nil {
		return nil, nil, wrapError(err, "failed to get parent database")
	}
	isSharedOrSystemDB := parentDB != nil && parentDB.IsSharedOrSystem()

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
				resource, err := tableResource(ctx, t, parentResourceID, isSharedOrSystemDB)
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

func parseTableResourceID(resource *v2.Resource) (string, string, string, error) {
	parts := strings.Split(resource.Id.Resource, ".")
	if len(parts) != 3 {
		return "", "", "", wrapError(fmt.Errorf("invalid table resource ID format: %s", resource.Id.Resource), "expected format: database.schema.table")
	}
	return parts[0], parts[1], parts[2], nil
}

func ownerEntitlementOnly(resource *v2.Resource) []*v2.Entitlement {
	return []*v2.Entitlement{
		ent.NewAssignmentEntitlement(
			resource,
			ownerEntitlement,
			ent.WithGrantableTo(userResourceType),
			ent.WithDescription(fmt.Sprintf("Is owned by %s", resource.DisplayName)),
			ent.WithDisplayName(fmt.Sprintf("Is owner of %s", resource.DisplayName)),
		),
	}
}

func grantsContainPrincipal(grants []*v2.Grant, principalID *v2.ResourceId, entitlementID string) bool {
	for _, g := range grants {
		if g.Principal.Id.Resource == principalID.Resource && g.Entitlement.Id == entitlementID {
			return true
		}
	}
	return false
}

func (o *tableBuilder) Entitlements(ctx context.Context, resource *v2.Resource, opts rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	databaseName, schemaName, tableName, err := parseTableResourceID(resource)
	if err != nil {
		return nil, nil, err
	}
	var rv []*v2.Entitlement

	if o.isDBSharedOrSystem(ctx, resource, databaseName) {
		return append(rv, ownerEntitlementOnly(resource)...), &rs.SyncOpResults{}, nil
	}

	objectKind := getObjectKind(resource)
	tableGrants, err := o.client.ListTableGrants(ctx, databaseName, schemaName, tableName, objectKind)
	if err != nil {
		ctxzap.Extract(ctx).Debug("list table grants failed, returning owner entitlement only", zap.String("table", resource.Id.Resource), zap.Error(err))
		return append(rv, ownerEntitlementOnly(resource)...), &rs.SyncOpResults{}, nil
	}

	privileges := make(map[string]bool)
	for _, tg := range tableGrants {
		if tg.GrantedTo == grantedToRole || tg.GrantedTo == grantedToUser {
			privileges[strings.ToLower(tg.Privilege)] = true
		}
	}
	for privilege := range privileges {
		rv = append(rv, ent.NewAssignmentEntitlement(
			resource,
			privilege,
			ent.WithGrantableTo(userResourceType, accountRoleResourceType),
			ent.WithDescription(fmt.Sprintf("Has %s privilege on %s", privilege, resource.DisplayName)),
			ent.WithDisplayName(fmt.Sprintf("%s on %s", strings.ToUpper(privilege), resource.DisplayName)),
		))
	}

	rv = append(rv, ownerEntitlementOnly(resource)...)
	return rv, &rs.SyncOpResults{}, nil
}

func (o *tableBuilder) Grants(ctx context.Context, resource *v2.Resource, opts rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	databaseName, schemaName, tableName, err := parseTableResourceID(resource)
	if err != nil {
		return nil, nil, err
	}

	if o.isDBSharedOrSystem(ctx, resource, databaseName) {
		return nil, &rs.SyncOpResults{}, nil
	}

	objectKind := getObjectKind(resource)
	tableGrants, err := o.client.ListTableGrants(ctx, databaseName, schemaName, tableName, objectKind)
	if err != nil {
		return nil, nil, wrapError(err, "failed to list table grants")
	}
	if len(tableGrants) == 0 {
		return nil, &rs.SyncOpResults{}, nil
	}

	var grants []*v2.Grant
	var ownerPrincipalID *v2.ResourceId
	var ownerExpandableRoleName string

	for _, tg := range tableGrants {
		entitlementID := strings.ToLower(tg.Privilege)
		var principalResource *v2.Resource
		var roleNameForExpandable string

		switch tg.GrantedTo {
		case grantedToRole:
			role, resp, err := o.client.GetAccountRole(ctx, tg.GranteeName)
			if err != nil {
				if snowflake.IsUnprocessableEntity(resp, err) {
					principalId, idErr := rs.NewResourceID(accountRoleResourceType, tg.GranteeName)
					if idErr != nil {
						continue
					}
					grants = append(grants, grant.NewGrant(resource, entitlementID, principalId, addExpandableOpts(tg.GranteeName)...))
					if entitlementID == privilegeOwner {
						ownerPrincipalID = principalId
						ownerExpandableRoleName = tg.GranteeName
					}
				}
				continue
			}
			if role == nil {
				continue
			}
			principalResource, err = accountRoleResource(role)
			if err != nil {
				continue
			}
			roleNameForExpandable = role.Name
			if entitlementID == privilegeOwner {
				ownerPrincipalID = principalResource.Id
				ownerExpandableRoleName = role.Name
			}
		case grantedToUser:
			user, _, err := o.client.GetUser(ctx, tg.GranteeName)
			if err != nil || user == nil {
				continue
			}
			principalResource, err = userResource(ctx, user, false)
			if err != nil {
				continue
			}
		default:
			continue
		}

		g := grant.NewGrant(resource, entitlementID, principalResource.Id, addExpandableOpts(roleNameForExpandable)...)
		grants = append(grants, g)
	}

	ownerEntitlementID := fmt.Sprintf("%s:%s:%s", tableResourceType.Id, resource.Id.Resource, ownerEntitlement)
	if ownerPrincipalID != nil && !grantsContainPrincipal(grants, ownerPrincipalID, ownerEntitlementID) {
		grants = append(grants, grant.NewGrant(resource, ownerEntitlement, ownerPrincipalID, addExpandableOpts(ownerExpandableRoleName)...))
	}

	if ownerPrincipalID == nil {
		table, _, _ := o.client.GetTable(ctx, databaseName, schemaName, tableName)
		if table != nil && table.Owner != "" && table.Owner != "SNOWFLAKE" {
			owner, ownerResp, err := o.client.GetAccountRole(ctx, table.Owner)
			if snowflake.IsUnprocessableEntity(ownerResp, err) {
				// system role, skip
			} else if err == nil && owner != nil {
				roleResource, err := accountRoleResource(owner)
				if err == nil && !grantsContainPrincipal(grants, roleResource.Id, ownerEntitlementID) {
					grants = append(grants, grant.NewGrant(resource, ownerEntitlement, roleResource.Id, addExpandableOpts(owner.Name)...))
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
