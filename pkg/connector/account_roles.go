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
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
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
			rsId, err := rs.NewResourceID(userResourceType, grantee.GranteeName)
			if err != nil {
				return nil, "", nil, wrapError(err, "unable to create user resource id")
			}
			g := grant.NewGrant(resource, assignedEntitlement, rsId)
			grants = append(grants, g)
		case "ROLE":
			rsId, err := rs.NewResourceID(accountRoleResourceType, grantee.GranteeName)
			if err != nil {
				return nil, "", nil, wrapError(err, "unable to create role resource id")
			}
			g := grant.NewGrant(resource, assignedEntitlement, rsId)
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

func (o *accountRoleBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if principal.Id.ResourceType != userResourceType.Id {
		err := fmt.Errorf("baton-snowflake: account roles can only be granted to users")

		l.Warn(
			"failed to grant account role to principal",
			zap.Error(err),
			zap.String("principal_type", principal.Id.ResourceType),
			zap.String("principal_id", principal.Id.Resource),
		)

		return nil, err
	}

	_, err := o.client.GrantAccountRole(ctx, entitlement.Resource.Id.Resource, principal.Id.Resource)
	if err != nil {
		err = wrapError(err, "failed to grant account role")

		l.Error(
			err.Error(),
			zap.String("account_role", entitlement.Resource.Id.Resource),
			zap.String("user", principal.Id.Resource),
		)
	}

	return nil, nil
}

func (o *accountRoleBuilder) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if grant.Principal.Id.ResourceType != userResourceType.Id {
		err := fmt.Errorf("baton-snowflake: only users can be revoked from account roles")

		l.Warn(
			err.Error(),
			zap.String("principal_type", grant.Principal.Id.ResourceType),
			zap.String("principal_id", grant.Principal.Id.Resource),
		)

		return nil, err
	}

	_, err := o.client.RevokeAccountRole(ctx, grant.Entitlement.Resource.Id.Resource, grant.Principal.Id.Resource)
	if err != nil {
		err = wrapError(err, "failed to revoke account role")

		l.Error(
			err.Error(),
			zap.String("account_role", grant.Entitlement.Resource.Id.Resource),
			zap.String("user", grant.Principal.Id.Resource),
		)
	}

	return nil, nil
}

func newAccountRoleBuilder(client *snowflake.Client) *accountRoleBuilder {
	return &accountRoleBuilder{
		resourceType: userResourceType,
		client:       client,
	}
}
