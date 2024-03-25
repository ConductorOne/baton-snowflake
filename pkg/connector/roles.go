package connector

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	snowflake "github.com/conductorone/baton-snowflake/pkg/snowflake"
)

type accountRoleBuilder struct {
	resourceType *v2.ResourceType
	client       *snowflake.Client
}

func (o *accountRoleBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return accountRoleResourceType
}

func accountRoleResource(ctx context.Context, accountRole *snowflake.AccountRole) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"name": accountRole.Name,
	}

	roleTraits := []rs.RoleTraitOption{
		rs.WithRoleProfile(profile),
	}

	resource, err := rs.NewRoleResource(accountRole.Name, accountRoleResourceType, accountRole.Name, roleTraits)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (o *accountRoleBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	bag, offset, err := parseOffsetFromToken(pToken.Token, &v2.ResourceId{ResourceType: o.resourceType.Id})
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to get next page offset")
	}

	accountRoles, _, err := o.client.ListAccountRoles(ctx, offset, resourcePageSize)
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to list account roles")
	}

	var resources []*v2.Resource
	for _, role := range accountRoles {
		resource, err := accountRoleResource(ctx, &role) // #nosec G601
		if err != nil {
			return nil, "", nil, wrapError(err, "failed to create account role resource")
		}

		resources = append(resources, resource)
	}

	if isLastPage(len(accountRoles), resourcePageSize) {
		return resources, "", nil, nil
	}

	nextPage, err := handleNextPage(bag, offset+resourcePageSize)
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to create next page cursor")
	}

	return resources, nextPage, nil, nil
}

func (o *accountRoleBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (o *accountRoleBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newAccountRoleBuilder(client *snowflake.Client) *accountRoleBuilder {
	return &accountRoleBuilder{
		resourceType: userResourceType,
		client:       client,
	}
}
