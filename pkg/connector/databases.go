package connector

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

type databaseBuilder struct {
	resourceType *v2.ResourceType
	client       *snowflake.Client
}

func (o *databaseBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return databaseResourceType
}

func databaseResource(database *snowflake.Database) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"name": database.Name,
	}

	databaseTraits := []rs.AppTraitOption{
		rs.WithAppProfile(profile),
	}

	resource, err := rs.NewAppResource(database.Name, databaseResourceType, database.Name, databaseTraits)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (o *databaseBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	bag, offset, err := parseOffsetFromToken(pToken.Token, &v2.ResourceId{ResourceType: o.resourceType.Id})
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to get next page offset")
	}

	databases, _, err := o.client.ListDatabases(ctx, offset, resourcePageSize)
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to list databases")
	}

	var resources []*v2.Resource
	for _, database := range databases {
		resource, err := databaseResource(&database) // #nosec G601
		if err != nil {
			return nil, "", nil, wrapError(err, "failed to create database resource")
		}

		resources = append(resources, resource)
	}

	if isLastPage(len(databases), resourcePageSize) {
		return resources, "", nil, nil
	}

	nextPage, err := handleNextPage(bag, offset+resourcePageSize)
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to create next page cursor")
	}

	return resources, nextPage, nil, nil
}

func (o *databaseBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (o *databaseBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newDatabaseBuilder(client *snowflake.Client) *databaseBuilder {
	return &databaseBuilder{
		resourceType: databaseResourceType,
		client:       client,
	}
}
