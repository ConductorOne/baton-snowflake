package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	grant "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	snowflake "github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
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
		profileKeyName: accountRole.Name,
	}

	resource, err := rs.NewRoleResource(accountRole.Name, accountRoleResourceType, accountRole.Name, nil, rs.WithResourceProfile(profile))
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (o *accountRoleBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, opts rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	bag, cursor, err := parseCursorFromToken(opts.PageToken.Token, &v2.ResourceId{ResourceType: o.resourceType.Id})
	if err != nil {
		return nil, nil, wrapError(err, "failed to get next page offset")
	}

	accountRoles, err := o.client.ListAccountRoles(ctx, cursor, resourcePageSize)
	if err != nil {
		return nil, nil, wrapError(err, "failed to list account roles")
	}

	if err := o.client.CacheAccountRoles(ctx, opts.Session, accountRoles); err != nil {
		return nil, nil, wrapError(err, "failed to seed account role cache")
	}

	var resources []*v2.Resource
	for _, role := range accountRoles {
		resource, err := accountRoleResource(&role) // #nosec G601
		if err != nil {
			return nil, nil, wrapError(err, "failed to create account role resource")
		}

		resources = append(resources, resource)
	}

	if isLastPage(len(accountRoles), resourcePageSize) {
		return resources, nil, nil
	}

	nextCursor, err := bag.NextToken(accountRoles[len(accountRoles)-1].Name)
	if err != nil {
		return nil, nil, wrapError(err, "failed to create next page cursor")
	}

	return resources, &rs.SyncOpResults{NextPageToken: nextCursor}, nil
}

func (o *accountRoleBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
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

	return rv, &rs.SyncOpResults{}, nil
}

// Grants() pages must fit the 6MB Lambda response limit, which counts bytes, not grants.
// A grant's size grows with both names: every grant embeds the full role resource,
// the role name appears again in the entitlement and grant IDs, and the grantee name appears in
// the grant ID, the principal ID and, for a ROLE grantee, the expansion annotation. Snowflake
// identifiers can be 255 characters of up to 4 bytes each, so 2000 grants of maximum-length
// names would be ~17MB. accountRoleGrantBytes overestimates a grant's serialized size, and pages
// stop at accountRoleGrantsMaxBytes of that estimate - ~4MB once the transport base64-encodes
// it, leaving headroom under 6MB. TestAccountRoleGrantPagesFitLambdaLimit checks the estimate
// against real grants from this builder.
const (
	accountRoleGrantsMaxRows  = 2000
	accountRoleGrantsMaxBytes = 3 * 1000 * 1000
)

// accountRoleGrantBytes returns an upper bound on the serialized size of one grant Grants() emits
// for granteeName on role, given the serialized size of role's resource. Measured costs are ~2
// bytes per byte of role ID outside the embedded resource, 2-3 per byte of grantee name, and
// ~160-280 bytes of fixed framing; the coefficients below round each of those up.
func accountRoleGrantBytes(roleResourceBytes int, roleID, granteeName string) int {
	return 512 + roleResourceBytes + 3*len(roleID) + 4*len(granteeName)
}

func accountRoleGrantsPageLimit(role *v2.Resource) snowflake.GranteePageLimit {
	roleResourceBytes := proto.Size(role)
	roleID := role.GetId().GetResource()
	return snowflake.GranteePageLimit{
		MaxRows:  accountRoleGrantsMaxRows,
		MaxBytes: accountRoleGrantsMaxBytes,
		RowBytes: func(grantee snowflake.AccountRoleGrantee) int {
			return accountRoleGrantBytes(roleResourceBytes, roleID, grantee.GranteeName)
		},
	}
}

func (o *accountRoleBuilder) Grants(ctx context.Context, resource *v2.Resource, opts rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	bag, cursor, err := parseCursorFromToken(opts.PageToken.Token, &v2.ResourceId{ResourceType: o.resourceType.Id})
	if err != nil {
		return nil, nil, wrapError(err, "failed to get next page offset")
	}

	accountRoleGrantees, nextCursor, err := o.client.ListAccountRoleGrantees(ctx, resource.DisplayName, cursor, accountRoleGrantsPageLimit(resource))
	if err != nil {
		return nil, nil, wrapError(err, "failed to list account role grantees")
	}

	var grants []*v2.Grant
	for _, grantee := range accountRoleGrantees {
		g, err := accountRoleGrant(resource, grantee)
		if err != nil {
			return nil, nil, err
		}
		if g != nil {
			grants = append(grants, g)
		}
	}

	if nextCursor == "" {
		return grants, nil, nil
	}

	nextToken, err := bag.NextToken(nextCursor)
	if err != nil {
		return nil, nil, wrapError(err, "failed to create next page cursor")
	}

	return grants, &rs.SyncOpResults{NextPageToken: nextToken}, nil
}

// accountRoleGrant builds the grant of role to grantee, or returns nil for a grantee type that is
// not synced.
func accountRoleGrant(role *v2.Resource, grantee snowflake.AccountRoleGrantee) (*v2.Grant, error) {
	switch grantee.GranteeType {
	case "USER":
		rsId, err := rs.NewResourceID(userResourceType, grantee.GranteeName)
		if err != nil {
			return nil, wrapError(err, "unable to create user resource id")
		}
		return grant.NewGrant(role, assignedEntitlement, rsId), nil
	case "ROLE":
		rsId, err := rs.NewResourceID(accountRoleResourceType, grantee.GranteeName)
		if err != nil {
			return nil, wrapError(err, "unable to create role resource id")
		}
		return grant.NewGrant(role, assignedEntitlement, rsId, addExpandableOpts(grantee.GranteeName)...), nil
	default:
		return nil, nil
	}
}

func (o *accountRoleBuilder) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if principal.Id.ResourceType != userResourceType.Id {
		err := fmt.Errorf("baton-snowflake: account roles can only be granted to users")

		l.Debug(
			"failed to grant account role to principal",
			zap.Error(err),
			zap.String("principal_type", principal.Id.ResourceType),
			zap.String("principal_id", principal.Id.Resource),
		)

		return nil, err
	}

	err := o.client.GrantAccountRole(ctx, entitlement.Resource.Id.Resource, principal.Id.Resource)
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

		l.Debug(
			err.Error(),
			zap.String("principal_type", grant.Principal.Id.ResourceType),
			zap.String("principal_id", grant.Principal.Id.Resource),
		)

		return nil, err
	}

	err := o.client.RevokeAccountRole(ctx, grant.Entitlement.Resource.Id.Resource, grant.Principal.Id.Resource)
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
		resourceType: accountRoleResourceType,
		client:       client,
	}
}
