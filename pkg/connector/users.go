package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	connectorbuilder "github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// extractProfileFields extracts optional fields from the accountInfo profile and populates the createReq.
func extractProfileFields(accountInfo *v2.AccountInfo, createReq *snowflake.CreateUserRequest) {
	profile := accountInfo.GetProfile()
	if profile == nil {
		return
	}

	pMap := profile.AsMap()

	if loginNameStr, ok := pMap["login"].(string); ok && loginNameStr != "" {
		createReq.LoginName = loginNameStr
	}
	if displayNameStr, ok := pMap["display_name"].(string); ok && displayNameStr != "" {
		createReq.DisplayName = displayNameStr
	}
	if firstNameStr, ok := pMap["first_name"].(string); ok && firstNameStr != "" {
		createReq.FirstName = firstNameStr
	}
	if lastNameStr, ok := pMap["last_name"].(string); ok && lastNameStr != "" {
		createReq.LastName = lastNameStr
	}
	if emailStr, ok := pMap["email"].(string); ok && emailStr != "" {
		createReq.Email = emailStr
	}
	if commentStr, ok := pMap["comment"].(string); ok && commentStr != "" {
		createReq.Comment = commentStr
	}
	// Handle disabled as boolean
	if disabledVal, ok := pMap["disabled"].(bool); ok {
		createReq.Disabled = disabledVal
	}
	// Default warehouse, namespace, role, and secondary roles
	if defaultWarehouseStr, ok := pMap["default_warehouse"].(string); ok && defaultWarehouseStr != "" {
		createReq.DefaultWarehouse = defaultWarehouseStr
	}
	if defaultNamespaceStr, ok := pMap["default_namespace"].(string); ok && defaultNamespaceStr != "" {
		createReq.DefaultNamespace = defaultNamespaceStr
	}
	if defaultRoleStr, ok := pMap["default_role"].(string); ok && defaultRoleStr != "" {
		createReq.DefaultRole = defaultRoleStr
	}
	if defaultSecondaryRolesStr, ok := pMap["default_secondary_roles"].(string); ok && defaultSecondaryRolesStr != "" {
		createReq.DefaultSecondaryRoles = defaultSecondaryRolesStr
	}
}

// List returns all the users from the database as resource objects.
// Users include a UserTrait because they are the 'shape' of a standard user.
func (o *userBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, opts rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	bag, cursor, err := parseCursorFromToken(opts.PageToken.Token, &v2.ResourceId{ResourceType: o.resourceType.Id})
	if err != nil {
		return nil, nil, wrapError(err, "failed to get next page cursor")
	}

	users, err := o.client.ListUsers(ctx, cursor, resourcePageSize)
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

// CreateAccountCapabilityDetails returns the capability details for user account provisioning.
func (o *userBuilder) CreateAccountCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	return &v2.CredentialDetailsAccountProvisioning{
		SupportedCredentialOptions: []v2.CapabilityDetailCredentialOption{
			v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_RANDOM_PASSWORD,
			v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_ENCRYPTED_PASSWORD,
		},
		PreferredCredentialOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_RANDOM_PASSWORD,
	}, nil, nil
}

// CreateAccount creates a new Snowflake user using the REST API.
func (o *userBuilder) CreateAccount(
	ctx context.Context,
	accountInfo *v2.AccountInfo,
	credentialOptions *v2.LocalCredentialOptions,
) (connectorbuilder.CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	// Extract user name from accountInfo
	// The user name must be provided in profile.name (required in schema)
	userName := ""
	if profile := accountInfo.GetProfile(); profile != nil {
		if nameStr, ok := rs.GetProfileStringValue(profile, "name"); ok && nameStr != "" {
			userName = nameStr
		}
	}

	if userName == "" {
		return nil, nil, nil, status.Error(codes.InvalidArgument, "baton-snowflake: user name is required (provide via profile.name)")
	}

	// Build create user request
	// name is the only required field for the create user request
	// Quote the username to preserve case sensitivity (Snowflake stores unquoted identifiers in uppercase)
	// Escape any double quotes in the username by doubling them
	quotedUserName := quoteSnowflakeIdentifier(userName)
	createReq := &snowflake.CreateUserRequest{
		Name: quotedUserName,
	}

	// Extract optional fields from profile (login and email are optional - only set if provided in profile)
	extractProfileFields(accountInfo, createReq)

	// Handle password generation
	var plaintextData []*v2.PlaintextData
	if credentialOptions != nil {
		createReq.MustChangePassword = credentialOptions.GetForceChangeAtNextLogin()
		// Generate password if random password is requested
		if credentialOptions.GetRandomPassword() == nil && credentialOptions.GetPlaintextPassword() == nil {
			return nil, nil, nil, errors.New("unsupported credential option")
		}
		plaintextPassword, err := crypto.GeneratePassword(ctx, credentialOptions)
		if err != nil {
			return nil, nil, nil, wrapError(err, "failed to generate password")
		}
		createReq.Password = plaintextPassword
		plaintextData = append(plaintextData, &v2.PlaintextData{
			Name:        "password",
			Description: "Password for the user",
			Bytes:       []byte(plaintextPassword),
		})
	}

	// Create user via REST API
	_, rateLimitDesc, err := o.client.CreateUserREST(ctx, createReq)
	if err != nil {
		l.Error("failed to create user",
			zap.String("user_name", userName),
			zap.Error(err),
		)
		var annos annotations.Annotations
		if rateLimitDesc != nil {
			annos = annotations.New(rateLimitDesc)
		}
		return nil, nil, annos, wrapError(err, "failed to create user")
	}

	user, err := o.fetchUserWithSQLRetry(ctx, userName)
	if err != nil {
		l.Error("failed to fetch user after creation",
			zap.String("user_name", userName),
			zap.Error(err),
		)
		annos := annotations.Annotations{}
		if rateLimitDesc != nil {
			annos.Update(rateLimitDesc)
		}
		return nil, nil, annos, wrapError(err, "failed to fetch user after creation")
	}

	// Build resource for the new user
	resource, err := userResource(ctx, user, o.syncSecrets)
	if err != nil {
		return nil, nil, nil, wrapError(err, "failed to create user resource")
	}

	l.Debug("user created successfully",
		zap.String("user_name", user.Username),
	)

	// Build annotations with rate limit information
	var annos annotations.Annotations
	if rateLimitDesc != nil {
		annos = annotations.New(rateLimitDesc)
	}

	// Return success result with plaintext data (password)
	result := &v2.CreateAccountResponse_SuccessResult{
		Resource: resource,
	}

	return result, plaintextData, annos, nil
}

// fetchUserWithSQLRetry attempts to fetch a user using the SQL API with retry logic for 422 errors.
// Retries up to 5 times with exponential backoff if we get a 422 Unprocessable Entity error.
func (o *userBuilder) fetchUserWithSQLRetry(ctx context.Context, userName string) (*snowflake.User, error) {
	l := ctxzap.Extract(ctx)
	maxRetries := 5
	baseDelay := 500 * time.Millisecond

	for attempt := 0; attempt <= maxRetries; attempt++ {
		user, statusCode, err := o.client.GetUser(ctx, userName)
		if err == nil && statusCode == http.StatusOK {
			l.Debug("user fetched successfully via SQL API",
				zap.String("user_name", userName),
			)
			return user, nil
		}

		// Check if we got a 422 error
		is422 := false
		if statusCode == http.StatusUnprocessableEntity {
			is422 = true
		} else if err != nil {
			errStr := strings.ToLower(err.Error())
			is422 = strings.Contains(errStr, "422") || strings.Contains(errStr, "unprocessable entity")
		}

		// If it's not a 422 error, or we've exhausted retries, return the error
		if !is422 || attempt >= maxRetries {
			if err != nil {
				return nil, err
			}
			if statusCode != http.StatusOK {
				return nil, fmt.Errorf("baton-snowflake: unexpected status code %d when fetching user", statusCode)
			}
			return nil, fmt.Errorf("baton-snowflake: failed to fetch user")
		}

		// Calculate exponential backoff: baseDelay * 2^attempt
		delay := baseDelay
		for i := 0; i < attempt; i++ {
			delay *= 2
		}

		l.Debug("user fetch returned 422, retrying with SQL API",
			zap.String("user_name", userName),
			zap.Int("attempt", attempt+1),
			zap.Int("max_retries", maxRetries),
			zap.Duration("delay", delay),
		)

		// Wait before retrying
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
			// Continue to next iteration
		}
	}

	// This should not be reached, but handle it just in case
	return nil, fmt.Errorf("baton-snowflake: failed to fetch user after %d retries", maxRetries)
}

// Delete deletes a Snowflake user using the REST API.
func (o *userBuilder) Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceID *v2.ResourceId) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	userName := resourceId.Resource
	if userName == "" {
		return nil, fmt.Errorf("baton-snowflake: user name is required")
	}

	// Quote the username to match the case-sensitive identifier created with quotes
	// This ensures we delete the exact case-sensitive identifier
	// Escape any double quotes in the username by doubling them
	quotedUserName := quoteSnowflakeIdentifier(userName)
	options := &snowflake.DeleteUserOptions{
		IfExists: true,
	}
	// Delete user via REST API
	_, err := o.client.DeleteUserREST(ctx, quotedUserName, options)
	if err != nil {
		l.Error("failed to delete user",
			zap.String("user_name", userName),
			zap.Error(err),
		)
		return nil, wrapError(err, "failed to delete user")
	}

	l.Debug("user deleted successfully",
		zap.String("user_name", userName),
	)

	return nil, nil
}

func newUserBuilder(client *snowflake.Client, syncSecrets bool) *userBuilder {
	return &userBuilder{
		resourceType: userResourceType,
		client:       client,
		syncSecrets:  syncSecrets,
	}
}
