package connector

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

type userBuilder struct {
	resourceType *v2.ResourceType
	client       *snowflake.Client
	syncSecrets  bool
}

func (o *userBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return userResourceType
}

func userResource(_ context.Context, user *snowflake.User, syncSecrets bool) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"email":        user.Email,
		"login":        user.Login,
		"display_name": user.DisplayName,
		"first_name":   user.FirstName,
		"last_name":    user.LastName,
		"comment":      user.Comment,
	}

	userTraits := []rs.UserTraitOption{
		rs.WithUserProfile(profile),
		rs.WithUserLogin(user.Login),
		rs.WithMFAStatus(&v2.UserTrait_MFAStatus{MfaEnabled: user.HasMfa}),
		rs.WithAccountType(getUserAccountType(user)),
		rs.WithDetailedStatus(getUserStatus(user), getUserDetailedStatus(user)),
	}

	if user.Email != "" {
		userTraits = append(userTraits, rs.WithEmail(user.Email, true))
	}

	if !user.LastSuccessLogin.IsZero() {
		userTraits = append(userTraits, rs.WithLastLogin(user.LastSuccessLogin))
	}

	displayName := user.DisplayName
	if displayName == "" {
		displayName = user.FirstName + " " + user.LastName
		if displayName == " " {
			displayName = user.Login
		}
	}

	var opts []rs.ResourceOption
	if syncSecrets {
		opts = append(opts, rs.WithAnnotation(&v2.ChildResourceType{ResourceTypeId: rsaPublicKeyResourceType.Id}))
	}

	resource, err := rs.NewUserResource(
		displayName,
		userResourceType,
		user.Username,
		userTraits,
		opts...,
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

func getUserAccountType(user *snowflake.User) v2.UserTrait_AccountType {
	// https://docs.snowflake.com/en/sql-reference/sql/create-user#label-user-type-property
	//	TYPE = PERSON | SERVICE | LEGACY_SERVICE | NULL
	if user.Type == "LEGACY_SERVICE" || user.Type == "SERVICE" {
		return v2.UserTrait_ACCOUNT_TYPE_SERVICE
	}
	return v2.UserTrait_ACCOUNT_TYPE_HUMAN
}

func getUserStatus(user *snowflake.User) v2.UserTrait_Status_Status {
	if user.Disabled || user.Locked {
		return v2.UserTrait_Status_STATUS_DISABLED
	}

	return v2.UserTrait_Status_STATUS_ENABLED
}

func getUserDetailedStatus(user *snowflake.User) string {
	if user.Disabled {
		return "disabled"
	}
	if user.Locked {
		return "locked"
	}
	return ""
}

// List returns all the users from the database as resource objects.
// Users include a UserTrait because they are the 'shape' of a standard user.
func (o *userBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, opts rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	bag, cursor, err := parseCursorFromToken(opts.PageToken.Token, &v2.ResourceId{ResourceType: o.resourceType.Id})
	if err != nil {
		return nil, nil, wrapError(err, "failed to get next page cursor")
	}

	users, _, err := o.client.ListUsers(ctx, cursor, resourcePageSize)
	if err != nil {
		return nil, nil, wrapError(err, "failed to list users")
	}

	if len(users) == 0 {
		return nil, nil, nil
	}

	var resources []*v2.Resource
	for _, user := range users {
		resource, err := userResource(ctx, &user, o.syncSecrets) // #nosec G601
		if err != nil {
			return nil, nil, wrapError(err, "failed to create user resource")
		}

		resources = append(resources, resource)
	}

	if isLastPage(len(users), resourcePageSize) {
		return resources, nil, nil
	}

	nextCursor, err := bag.NextToken(users[len(users)-1].Username)
	if err != nil {
		return nil, nil, wrapError(err, "failed to create next page cursor")
	}

	return resources, &rs.SyncOpResults{NextPageToken: nextCursor}, nil
}

// Entitlements always returns an empty slice for users.
func (o *userBuilder) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

// Grants always returns an empty slice for users since they don't have any entitlements.
func (o *userBuilder) Grants(ctx context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func newUserBuilder(client *snowflake.Client, syncSecrets bool) *userBuilder {
	return &userBuilder{
		resourceType: userResourceType,
		client:       client,
		syncSecrets:  syncSecrets,
	}
}
