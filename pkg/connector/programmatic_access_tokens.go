package connector

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type programmaticAccessTokenBuilder struct {
	client *snowflake.Client
}

func newProgrammaticAccessTokenBuilder(client *snowflake.Client) *programmaticAccessTokenBuilder {
	return &programmaticAccessTokenBuilder{client: client}
}

func (o *programmaticAccessTokenBuilder) ResourceType(context.Context) *v2.ResourceType {
	return programmaticAccessTokenResourceType
}

func (o *programmaticAccessTokenBuilder) List(ctx context.Context, parentID *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parentID == nil || parentID.GetResourceType() != userResourceType.Id {
		return nil, nil, nil
	}
	tokens, err := o.client.ListProgrammaticAccessTokens(ctx, parentID.GetResource())
	if err != nil {
		// SHOW USER PROGRAMMATIC ACCESS TOKENS needs ownership or MONITOR on the target
		// user. Without it Snowflake answers 422/003001, which means "nothing visible
		// here" rather than a failure - one unprivileged user must not abort the sync.
		if snowflake.IsInsufficientPrivileges(err) {
			ctxzap.Extract(ctx).Debug("skipping programmatic access tokens: insufficient privileges",
				zap.String("username", parentID.GetResource()))
			return nil, &rs.SyncOpResults{}, nil
		}
		return nil, nil, fmt.Errorf("baton-snowflake: list programmatic access tokens: %w", err)
	}
	resources := make([]*v2.Resource, 0, len(tokens))
	for _, token := range tokens {
		resource, err := newProgrammaticAccessTokenResource(parentID, token.Name, token.ExpiresAt)
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, resource)
	}
	return resources, nil, nil
}

func (o *programmaticAccessTokenBuilder) Entitlements(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func (o *programmaticAccessTokenBuilder) Grants(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func (o *programmaticAccessTokenBuilder) Delete(ctx context.Context, resourceID *v2.ResourceId, _ *v2.ResourceId) (annotations.Annotations, error) {
	userName, tokenName, err := parseProgrammaticAccessTokenID(resourceID.GetResource())
	if err != nil {
		return nil, err
	}
	if err := o.client.RemoveProgrammaticAccessToken(ctx, userName, tokenName); err != nil {
		return nil, fmt.Errorf("baton-snowflake: remove programmatic access token: %w", err)
	}
	return nil, nil
}

func newProgrammaticAccessTokenResource(identityID *v2.ResourceId, tokenName string, expiresAt time.Time) (*v2.Resource, error) {
	return rs.NewSecretResource(
		tokenName,
		programmaticAccessTokenResourceType,
		programmaticAccessTokenID(identityID.Resource, tokenName),
		[]rs.SecretTraitOption{
			rs.WithSecretCreatedByID(identityID),
			rs.WithSecretIdentityID(identityID),
			rs.WithSecretExpiresAt(expiresAt),
			rs.WithSecretType(v2.SecretTrait_CREDENTIAL_TYPE_STATIC_SECRET),
			rs.WithSecretDetail("snowflake.programmatic_access_token"),
		},
		rs.WithParentResourceID(identityID),
	)
}

func programmaticAccessTokenID(userName, tokenName string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(userName)) + "." + tokenName
}

func parseProgrammaticAccessTokenID(resourceID string) (string, string, error) {
	parts := strings.SplitN(resourceID, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("baton-snowflake: invalid programmatic access token resource id")
	}
	userName, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(userName) == 0 {
		return "", "", fmt.Errorf("baton-snowflake: invalid programmatic access token user id")
	}
	if strings.ContainsAny(parts[1], "\";") {
		return "", "", fmt.Errorf("baton-snowflake: invalid programmatic access token name")
	}
	return string(userName), parts[1], nil
}
