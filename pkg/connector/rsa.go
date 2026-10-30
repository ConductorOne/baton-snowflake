package connector

import (
	"context"
	"fmt"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
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

func rsaResource(_ context.Context, user *snowflake.UserRsa, rsaIdx RsaIndex, id *v2.ResourceId, credentials []snowflake.Credential) (*v2.Resource, error) {
	var lastSetTime *time.Time
	var fp string

	switch rsaIdx {
	case RsaIndex1:
		lastSetTime = user.RsaPublicKeyLastSetTime
		fp = normalizeFp(user.RsaPublicKeyFp)
	case RsaIndex2:
		lastSetTime = user.RsaPublicKeyLastSetTime2
		fp = normalizeFp(user.RsaPublicKeyFp2)
	default:
		return nil, fmt.Errorf("invalid rsa index: %d", rsaIdx)
	}

	if lastSetTime == nil && fp == "" {
		return nil, nil
	}

	secretTraits := []rs.SecretTraitOption{
		rs.WithSecretCreatedByID(id),
		rs.WithSecretType(v2.SecretTrait_CREDENTIAL_TYPE_ASYMMETRIC_KEY),
		rs.WithSecretDetail("snowflake.rsa_key_pair"),
	}
	if fp != "" {
		credential := findCredential(credentials, func(c snowflake.Credential) bool {
			return c.Type == snowflake.CredentialTypeKeyPair && normalizeFp(c.PublicKeyFp) == fp
		})
		if credential != nil && credential.LastUsedOn != nil {
			secretTraits = append(secretTraits, rs.WithSecretLastUsedAt(*credential.LastUsedOn))
		}
	}

	resourceOpts := []rs.ResourceOption{rs.WithParentResourceID(id)}
	if lastSetTime != nil {
		resourceOpts = append(resourceOpts, rs.WithResourceCreatedAt(*lastSetTime))
	}

	rsaId := fmt.Sprintf("%s-%s", user.Username, rsaIdx.String())

	resource, err := rs.NewSecretResource(
		rsaId,
		rsaPublicKeyResourceType,
		rsaId,
		secretTraits,
		resourceOpts...,
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

// List returns all the users from the database as resource objects.
// Users include a UserTrait because they are the 'shape' of a standard user.
func (o *rsaBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, opts rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	l := ctxzap.Extract(ctx)

	if parentResourceID == nil {
		// ignore parentResourceID
		return nil, nil, nil
	}

	if parentResourceID.ResourceType != userResourceType.Id {
		return nil, nil, fmt.Errorf("invalid parent resource type: %s", parentResourceID.ResourceType)
	}

	userName := parentResourceID.Resource

	user, err := o.client.UserRsa(ctx, userName)
	if err != nil {
		// DESCRIBE USER answers 422/003001 when the connector role lacks MONITOR on that user.
		// Matching the HTTP status text alone misses this: uhttp formats the body message, which
		// never contains "422 Unprocessable Entity".
		if snowflake.IsInsufficientPrivileges(err) {
			l.Debug("skipping rsa keys: insufficient privileges to describe user",
				zap.String("username", userName))
			return nil, &rs.SyncOpResults{}, nil
		}
		return nil, nil, err
	}

	l.Debug("UserRsa", zap.Any("user", user))

	credentials, _, err := o.client.UserCredentials(ctx, opts.Session, userName)
	if err != nil {
		return nil, nil, fmt.Errorf("baton-snowflake: list credentials for user %q: %w", userName, err)
	}

	indx := []RsaIndex{RsaIndex1, RsaIndex2}
	var resources []*v2.Resource

	for _, idx := range indx {
		resource, err := rsaResource(ctx, user, idx, parentResourceID, credentials)
		if err != nil {
			return nil, nil, err
		}

		if resource != nil {
			resources = append(resources, resource)
		}
	}

	return resources, nil, nil
}

// Entitlements always returns an empty slice for users.
func (o *rsaBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

// Grants always returns an empty slice for users since they don't have any entitlements.
func (o *rsaBuilder) Grants(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func newRsaBuilder(client *snowflake.Client) *rsaBuilder {
	return &rsaBuilder{
		client: client,
	}
}
