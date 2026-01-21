package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

type secretBuilder struct {
	client *snowflake.Client
}

func (o *secretBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return secretResourceType
}

func secretResource(_ context.Context, secret *snowflake.Secret, id *v2.ResourceId) (*v2.Resource, error) {
	secretOwner, err := rs.NewResourceID(userResourceType, secret.Owner)
	if err != nil {
		return nil, err
	}

	secretTraits := []rs.SecretTraitOption{
		rs.WithSecretCreatedAt(secret.CreatedOn),
		rs.WithSecretCreatedByID(secretOwner),
	}

	secretId := fmt.Sprintf("%s-%s", secret.DatabaseName, secret.Name)

	resource, err := rs.NewSecretResource(
		secret.Name,
		secretResourceType,
		secretId,
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
func (o *secretBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if parentResourceID == nil {
		// ignore parentResourceID
		return nil, "", nil, nil
	}

	if parentResourceID.ResourceType != databaseResourceType.Id {
		return nil, "", nil, fmt.Errorf("invalid parent resource type: %s", parentResourceID.ResourceType)
	}

	databaseName := parentResourceID.Resource

	secrets, err := o.client.ListSecrets(ctx, databaseName)
	if err != nil {
		return nil, "", nil, err
	}

	var resources []*v2.Resource

	for _, secret := range secrets {
		resource, err := secretResource(ctx, &secret, parentResourceID)
		if err != nil {
			return nil, "", nil, err
		}
		resources = append(resources, resource)
	}

	return resources, "", nil, nil
}

// Entitlements always returns an empty slice for users.
func (o *secretBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

// Grants always returns an empty slice for users since they don't have any entitlements.
func (o *secretBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newSecretBuilder(client *snowflake.Client) *secretBuilder {
	return &secretBuilder{
		client: client,
	}
}
