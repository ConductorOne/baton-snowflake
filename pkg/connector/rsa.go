package connector

import (
	"context"
	"fmt"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RsaIndex int

const (
	RsaIndex1 RsaIndex = 1
	RsaIndex2 RsaIndex = 2
)

func (i RsaIndex) String() string {
	return fmt.Sprintf("rsa_%d", i)
}

type rsaBuilder struct {
	client *snowflake.Client
}

func (o *rsaBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return rsaPublicKeyResourceType
}

func rsaResource(_ context.Context, user *snowflake.UserRsa, rsaIdx RsaIndex, id *v2.ResourceId) (*v2.Resource, error) {
	var rsaTime *time.Time

	switch rsaIdx {
	case RsaIndex1:
		rsaTime = user.RsaPublicKeyLastSetTime
	case RsaIndex2:
		rsaTime = user.RsaPublicKeyLastSetTime2
	default:
		return nil, fmt.Errorf("invalid rsa index: %d", rsaIdx)
	}

	if rsaTime == nil {
		return nil, nil
	}

	secretTraits := []rs.SecretTraitOption{
		rs.WithSecretLastUsedAt(*rsaTime),
		rs.WithSecretCreatedByID(id),
	}

	rsaId := fmt.Sprintf("%s-%s", user.Username, rsaIdx.String())

	resource, err := rs.NewSecretResource(
		rsaId,
		rsaPublicKeyResourceType,
		rsaId,
		secretTraits,
		rs.WithParentResourceID(id),
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

// List returns all the users from the database as resource objects.
// Users include a UserTrait because they are the 'shape' of a standard user.
func (o *rsaBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if parentResourceID == nil {
		// ignore parentResourceID
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != userResourceType.Id {
		return nil, "", nil, fmt.Errorf("invalid parent resource type: %s", parentResourceID.ResourceType)
	}

	userName := parentResourceID.Resource

	user, err := o.client.UserRsa(ctx, userName)
	if err != nil {
		if status.Code(err) == codes.Unknown {
			// Ignore user that don't have permission to describe user
			// TODO: api return 422 when user doesn't have permission to describe user
			l.Warn("UserRsa failed", zap.String("username", userName), zap.Error(err))
			return nil, "", nil, nil
		}
		return nil, "", nil, err
	}

	l.Debug("UserRsa", zap.Any("user", user))

	indx := []RsaIndex{RsaIndex1, RsaIndex2}
	var resources []*v2.Resource

	for _, idx := range indx {
		resource, err := rsaResource(ctx, user, idx, parentResourceID)
		if err != nil {
			return nil, "", nil, err
		}

		if resource != nil {
			resources = append(resources, resource)
		}
	}

	return resources, "", nil, nil
}

// Entitlements always returns an empty slice for users.
func (o *rsaBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

// Grants always returns an empty slice for users since they don't have any entitlements.
func (o *rsaBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newRsaBuilder(client *snowflake.Client) *rsaBuilder {
	return &rsaBuilder{
		client: client,
	}
}
