package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type databaseBuilder struct {
	resourceType *v2.ResourceType
	client       *snowflake.Client
	syncSecrets  bool
}

func (o *databaseBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return databaseResourceType
}

func databaseResource(database *snowflake.Database, syncSecrets bool) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"name": database.Name,
	}

	databaseTraits := []rs.AppTraitOption{
		rs.WithAppProfile(profile),
	}

	var opts []rs.ResourceOption
	if syncSecrets {
		opts = append(opts, rs.WithAnnotation(&v2.ChildResourceType{ResourceTypeId: secretResourceType.Id}))
	}

	resource, err := rs.NewAppResource(
		database.Name,
		databaseResourceType,
		database.Name,
		databaseTraits,
		opts...,
	)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (o *databaseBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, opts rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	bag, cursor, err := parseCursorFromToken(opts.PageToken.Token, &v2.ResourceId{ResourceType: o.resourceType.Id})
	if err != nil {
		return nil, &rs.SyncOpResults{}, wrapError(err, "failed to get next page offset")
	}

	databases, _, err := o.client.ListDatabases(ctx, cursor, resourcePageSize)
	if err != nil {
		return nil, &rs.SyncOpResults{}, wrapError(err, "failed to list databases")
	}

	var resources []*v2.Resource
	for _, database := range databases {
		resource, err := databaseResource(&database, o.syncSecrets) // #nosec G601
		if err != nil {
			return nil, &rs.SyncOpResults{}, wrapError(err, "failed to create database resource")
		}

		resources = append(resources, resource)
	}

	if isLastPage(len(databases), resourcePageSize) {
		return resources, &rs.SyncOpResults{}, nil
	}

	nextCursor, err := bag.NextToken(databases[len(databases)-1].Name)
	if err != nil {
		return nil, &rs.SyncOpResults{}, wrapError(err, "failed to create next page cursor")
	}

	return resources, &rs.SyncOpResults{NextPageToken: nextCursor}, nil
}

func (o *databaseBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	var rv []*v2.Entitlement

	rv = append(rv, ent.NewAssignmentEntitlement(
		resource,
		ownerEntitlement,
		ent.WithGrantableTo(userResourceType),
		ent.WithDescription(fmt.Sprintf("Is owned by %s", resource.DisplayName)),
		ent.WithDisplayName(fmt.Sprintf("Is owner of %s", resource.DisplayName)),
	))

	return rv, &rs.SyncOpResults{}, nil
}

func (o *databaseBuilder) Grants(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	l := ctxzap.Extract(ctx)
	database, _, err := o.client.GetDatabase(ctx, resource.Id.Resource)
	if err != nil {
		return nil, &rs.SyncOpResults{}, wrapError(err, "failed to get database")
	}

	if database.Owner == "" {
		return nil, &rs.SyncOpResults{}, nil
	}

	owner, _, err := o.client.GetAccountRole(ctx, database.Owner)
	if err != nil {
		return nil, &rs.SyncOpResults{}, wrapError(err, "failed to get owner account role")
	}

	if owner == nil {
		l.Warn("snowflake-connector: account role not found", zap.String("role", database.Owner))
		return nil, &rs.SyncOpResults{}, nil
	}

	roleResource, err := accountRoleResource(owner)
	if err != nil {
		return nil, &rs.SyncOpResults{}, wrapError(err, "failed to create owner account role resource")
	}

	var grants = []*v2.Grant{
		grant.NewGrant(
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
		),
	}

	return grants, &rs.SyncOpResults{}, nil
}

func newDatabaseBuilder(client *snowflake.Client, syncSecrets bool) *databaseBuilder {
	return &databaseBuilder{
		resourceType: databaseResourceType,
		client:       client,
		syncSecrets:  syncSecrets,
	}
}
