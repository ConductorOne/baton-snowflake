package connector

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

type userBuilder struct {
	resourceType *v2.ResourceType
	client       *snowflake.Client
}

func (o *userBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return userResourceType
}

func userResource(ctx context.Context, user *snowflake.User) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"email":      user.Email,
		"login":      user.Email,
		"first_name": user.FirstName,
		"last_name":  user.LastName,
	}

	userTraits := []rs.UserTraitOption{
		rs.WithUserProfile(profile),
		rs.WithUserLogin(user.Email),
		rs.WithStatus(getUserStatus(user)),
	}

	resource, err := rs.NewUserResource(user.Email, userResourceType, user.Username, userTraits)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func getUserStatus(user *snowflake.User) v2.UserTrait_Status_Status {
	if user.Disabled {
		return v2.UserTrait_Status_STATUS_DISABLED
	}

	return v2.UserTrait_Status_STATUS_ENABLED
}

// List returns all the users from the database as resource objects.
// Users include a UserTrait because they are the 'shape' of a standard user.
func (o *userBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	bag, offset, err := parseOffsetFromToken(pToken.Token, &v2.ResourceId{ResourceType: o.resourceType.Id})
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to get next page offset")
	}

	users, _, err := o.client.ListUsers(ctx, offset, resourcePageSize)
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to list users")
	}

	var resources []*v2.Resource
	for _, user := range users {
		resource, err := userResource(ctx, &user) // #nosec G601
		if err != nil {
			return nil, "", nil, wrapError(err, "failed to create user resource")
		}

		resources = append(resources, resource)
	}

	if isLastPage(len(users), resourcePageSize) {
		return resources, "", nil, nil
	}

	nextPage, err := handleNextPage(bag, offset+resourcePageSize)
	if err != nil {
		return nil, "", nil, wrapError(err, "failed to create next page cursor")
	}

	return resources, nextPage, nil, nil
}

// Entitlements always returns an empty slice for users.
func (o *userBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

// Grants always returns an empty slice for users since they don't have any entitlements.
func (o *userBuilder) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func newUserBuilder(client *snowflake.Client) *userBuilder {
	return &userBuilder{
		resourceType: userResourceType,
		client:       client,
	}
}
