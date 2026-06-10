package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type patBuilder struct {
	client *snowflake.Client
}

func (o *patBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return programmaticAccessTokenResourceType
}

// patResource builds a Secret resource from a single PAT metadata record.
// The resource ID is "<username>/<pat-name>" to namespace per user.
// Source: https://docs.snowflake.com/en/sql-reference/sql/show-user-programmatic-access-tokens
func patResource(_ context.Context, pat *snowflake.ProgrammaticAccessToken, parentID *v2.ResourceId) (*v2.Resource, error) {
	userResourceID, err := rs.NewResourceID(userResourceType, pat.UserName)
	if err != nil {
		return nil, err
	}

	secretTraits := []rs.SecretTraitOption{
		rs.WithSecretType(v2.SecretTrait_CREDENTIAL_TYPE_STATIC_SECRET),
		rs.WithSecretDetail("snowflake.pat"),
		rs.WithSecretIdentityID(userResourceID),
	}

	if !pat.CreatedOn.IsZero() {
		secretTraits = append(secretTraits, rs.WithSecretCreatedAt(pat.CreatedOn))
	}
	if !pat.ExpiresAt.IsZero() {
		secretTraits = append(secretTraits, rs.WithSecretExpiresAt(pat.ExpiresAt))
	}

	resourceID := fmt.Sprintf("%s/%s", pat.UserName, pat.Name)

	return rs.NewSecretResource(
		pat.Name,
		programmaticAccessTokenResourceType,
		resourceID,
		secretTraits,
		rs.WithParentResourceID(parentID),
	)
}

// List returns all PATs for the user identified by parentResourceID.
// Parent must be a user resource; the connector iterates users and fans out
// one SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER call per user.
func (o *patBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	l := ctxzap.Extract(ctx)

	if parentResourceID == nil {
		return nil, nil, nil
	}

	if parentResourceID.ResourceType != userResourceType.Id {
		return nil, nil, fmt.Errorf("invalid parent resource type: %s", parentResourceID.ResourceType)
	}

	username := parentResourceID.Resource

	pats, err := o.client.ListProgrammaticAccessTokens(ctx, username)
	if err != nil {
		return nil, nil, err
	}

	l.Debug("listed PATs for user", zap.String("username", username), zap.Int("count", len(pats)))

	var resources []*v2.Resource
	for i := range pats {
		resource, err := patResource(ctx, &pats[i], parentResourceID)
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, resource)
	}

	return resources, nil, nil
}

func (o *patBuilder) Entitlements(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func (o *patBuilder) Grants(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func newPATBuilder(client *snowflake.Client) *patBuilder {
	return &patBuilder{client: client}
}
