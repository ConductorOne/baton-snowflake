package connector

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	issueProgrammaticAccessTokenAction = "issue_programmatic_access_token"
	patActionUserIDArg                 = "user_id"
	patActionRequestIDArg              = "request_id"
	patActionExpiresAtArg              = "expires_at"
	patActionIssuedResourceReturn      = "issued_resource"
	patActionNativeReturnName          = "api_key_v2"
)

var patActionRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

var issueProgrammaticAccessTokenSchema = &v2.BatonActionSchema{
	Name:           issueProgrammaticAccessTokenAction,
	DisplayName:    "Issue programmatic access token",
	Description:    "Issues a Snowflake programmatic access token for an approved user.",
	ResourceTypeId: userResourceType.Id,
	ActionType:     []v2.ActionType{v2.ActionType_ACTION_TYPE_DYNAMIC},
	Arguments: []*config.Field{
		{
			Name: patActionUserIDArg, DisplayName: "User", IsRequired: true,
			Field: &config.Field_ResourceIdField{ResourceIdField: &config.ResourceIdField{
				Rules: &config.ResourceIDRules{AllowedResourceTypeIds: []string{userResourceType.Id}},
			}},
		},
		{
			Name: patActionRequestIDArg, DisplayName: "Request ID", IsRequired: true,
			Field: &config.Field_StringField{StringField: &config.StringField{
				Rules: &config.StringRules{
					MinLen: uint64Ptr(1), MaxLen: uint64Ptr(128), Pattern: stringPtr(`^[A-Za-z0-9_-]+$`),
				},
			}},
		},
		{
			Name: patActionExpiresAtArg, DisplayName: "Expires at",
			Description: "Optional approved RFC3339 expiration instant.",
			Field:       &config.Field_StringField{StringField: &config.StringField{}},
		},
	},
	ReturnTypes: []*config.Field{
		{
			Name: patActionIssuedResourceReturn, DisplayName: "Issued resource", IsRequired: true,
			Field: &config.Field_ResourceField{ResourceField: &config.ResourceField{}},
		},
		{
			Name: patActionNativeReturnName, DisplayName: "API key", IsRequired: true, IsSecret: true,
			Field: &config.Field_StringField{StringField: &config.StringField{}},
		},
	},
}

var _ connectorbuilder.ResourceActionProvider = (*credentialUserBuilder)(nil)

func (o *credentialUserBuilder) ResourceActions(ctx context.Context, registry actions.ActionRegistry) error {
	if err := actions.RegisterWithSecrets(ctx, registry, issueProgrammaticAccessTokenSchema, o.issueProgrammaticAccessTokenAction); err != nil {
		return fmt.Errorf("baton-snowflake: register programmatic access token action: %w", err)
	}
	return nil
}

func (o *credentialUserBuilder) issueProgrammaticAccessTokenAction(
	ctx context.Context,
	args *structpb.Struct,
) (_ *structpb.Struct, _ []*v2.PlaintextData, _ annotations.Annotations, actionErr error) {
	input, err := parseProgrammaticAccessTokenActionArgs(args, time.Now().UTC())
	if err != nil {
		return nil, nil, nil, err
	}
	output, err := o.Issue(ctx, input)
	if err != nil {
		return nil, nil, nil, err
	}
	converted := false
	defer func() {
		if converted {
			return
		}
		// Issue returned a located credential. Preserve cleanup if adapting that
		// known result fails before the SDK encrypts it.
		if rmErr := o.client.RemoveProgrammaticAccessTokenStrict(
			context.WithoutCancel(ctx), input.IdentityID.Resource, "c1-"+input.RequestID,
		); rmErr != nil {
			actionErr = errors.Join(actionErr, fmt.Errorf(
				"baton-snowflake: action result cleanup failed; outcome unknown, verify/reconcile in Snowflake before retry: %w", rmErr,
			))
		}
	}()
	if output.Secret.GetId().GetResourceType() != programmaticAccessTokenResourceType.Id ||
		len(output.PlaintextData) != 1 || output.PlaintextData[0].GetName() != patActionNativeReturnName {
		return nil, nil, nil, fmt.Errorf("baton-snowflake: issued token action returned an unexpected credential shape")
	}
	resourceField, err := actions.NewResourceReturnField(patActionIssuedResourceReturn, output.Secret)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("baton-snowflake: encode issued token resource: %w", err)
	}
	response := &structpb.Struct{Fields: map[string]*structpb.Value{resourceField.Key: resourceField.Value}}
	output.PlaintextData[0].SetSchema("api_key_v2")
	converted = true
	return response, output.PlaintextData, output.Annotations, nil
}

func parseProgrammaticAccessTokenActionArgs(args *structpb.Struct, now time.Time) (*connectorbuilder.CredentialIssueInput, error) {
	if args == nil {
		return nil, fmt.Errorf("baton-snowflake: action arguments are required")
	}
	for name := range args.GetFields() {
		switch name {
		case patActionUserIDArg, patActionRequestIDArg, patActionExpiresAtArg:
		default:
			return nil, fmt.Errorf("baton-snowflake: unsupported programmatic access token argument %q", name)
		}
	}
	identityID, err := actions.RequireResourceIDArg(args, patActionUserIDArg)
	if err != nil || identityID.GetResourceType() != userResourceType.Id || identityID.GetResource() == "" {
		return nil, fmt.Errorf("baton-snowflake: user_id must identify a Snowflake user")
	}
	requestID, err := actions.RequireStringArg(args, patActionRequestIDArg)
	if err != nil || len(requestID) == 0 || len(requestID) > 128 || !patActionRequestIDPattern.MatchString(requestID) {
		return nil, fmt.Errorf("baton-snowflake: request_id must be 1..128 ASCII letters, digits, underscore, or hyphen")
	}
	input := &connectorbuilder.CredentialIssueInput{
		IdentityID: identityID,
		RequestID:  requestID,
		CredentialOptions: v2.CredentialIssueOptions_builder{
			SecretResourceTypeId: programmaticAccessTokenResourceType.Id,
			ApiKey:               v2.CredentialIssueOptions_ApiKey_builder{}.Build(),
		}.Build(),
	}
	if value, present := args.GetFields()[patActionExpiresAtArg]; present {
		if value == nil {
			return nil, fmt.Errorf("baton-snowflake: expires_at must be an RFC3339 timestamp")
		}
		text, ok := value.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return nil, fmt.Errorf("baton-snowflake: expires_at must be an RFC3339 timestamp")
		}
		expiresAt, err := time.Parse(time.RFC3339Nano, text.StringValue)
		if err != nil {
			return nil, fmt.Errorf("baton-snowflake: expires_at must be an RFC3339 timestamp: %w", err)
		}
		remaining := expiresAt.Sub(now)
		if remaining < programmaticAccessTokenMinLifetime || remaining > programmaticAccessTokenMaxLifetime {
			return nil, fmt.Errorf("baton-snowflake: expires_at must be between one and 365 days from now")
		}
		input.ExpiresAt = timestamppb.New(expiresAt.UTC())
	}
	return input, nil
}

func uint64Ptr(value uint64) *uint64 { return &value }
func stringPtr(value string) *string { return &value }
