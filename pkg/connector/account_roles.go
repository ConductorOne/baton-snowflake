package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	grant "github.com/conductorone/baton-sdk/pkg/types/grant"
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

func accountRoleResource(accountRole *snowflake.AccountRole) (*v2.Resource, error) {
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
		resource, err := accountRoleResource(&role) // #nosec G601
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
	var rv []*v2.Entitlement

	rv = append(rv, ent.NewAssignmentEntitlement(
		resource,
		assignedEntitlement,
		ent.WithGrantableTo(userResourceType),
		ent.WithDescription(fmt.Sprintf("Has %s account role assigned", resource.DisplayName)),
		ent.WithDisplayName(fmt.Sprintf("%s account role %s", resource.DisplayName, assignedEntitlement)),
	))
	rv = append(rv, ent.NewAssignmentEntitlement(
		resource,
		assignedEntitlement,
		ent.WithGrantableTo(accountRoleResourceType),
		ent.WithDescription(fmt.Sprintf("Has %s account role assigned", resource.DisplayName)),
		ent.WithDisplayName(fmt.Sprintf("%s account role %s", resource.DisplayName, assignedEntitlement)),
	))

	return rv, "", nil, nil
}

func (o *accountRoleBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	bag, offset, err := parseOffsetFromToken(pToken.Token, &v2.ResourceId{ResourceType: o.resourceType.Id})
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to get next page offset")
	}

	accountRoleGrantees, _, err := o.client.ListAccountRoleGrantees(ctx, resource.DisplayName, offset, resourcePageSize)
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to list account role grantees")
	}
	var grants []*v2.Grant
	for _, grantee := range accountRoleGrantees {
		switch grantee.GranteeType {
		case "USER":
			g, err := o.GrantUser(ctx, resource, grantee.GranteeName)
			if err != nil {
				return nil, "", nil, wrapError(err, "failed to grant user")
			}
			grants = append(grants, g)
		case "ROLE":
			g, err := o.GrantRole(ctx, resource, grantee.GranteeName)
			if err != nil {
				return nil, "", nil, wrapError(err, "failed to grant role")
			}
			grants = append(grants, g)
		}
	}

	if isLastPage(len(accountRoleGrantees), resourcePageSize) {
		return grants, "", nil, nil
	}

	nextPage, err := handleNextPage(bag, offset+resourcePageSize)
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to create next page cursor")
	}

	return grants, nextPage, nil, nil
}

func (o *accountRoleBuilder) GrantUser(ctx context.Context, resource *v2.Resource, granteeName string) (*v2.Grant, error) {
	user, _, err := o.client.GetUser(ctx, granteeName)
	if err != nil {
		return nil, wrapError(err, "failed to get user")
	}

	userResource, err := userResource(ctx, user)
	if err != nil {
		return nil, wrapError(err, "failed to create user resource")
	}

	return grant.NewGrant(resource, assignedEntitlement, userResource.Id), nil
}

func (o *accountRoleBuilder) GrantRole(ctx context.Context, resource *v2.Resource, granteeName string) (*v2.Grant, error) {
	roleResource, err := accountRoleResource(&snowflake.AccountRole{Name: granteeName})
	if err != nil {
		return nil, wrapError(err, "failed to create role resource")
	}

	return grant.NewGrant(resource, assignedEntitlement, roleResource.Id), nil
}

func newAccountRoleBuilder(client *snowflake.Client) *accountRoleBuilder {
	return &accountRoleBuilder{
		resourceType: userResourceType,
		client:       client,
	}
}
