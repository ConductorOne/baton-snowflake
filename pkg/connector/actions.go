package connector

import (
	"context"
	"fmt"
	"strings"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	actionDisableUser = "disable_user"
	actionEnableUser  = "enable_user"

	argUserIDKey     = "user_id"
	argUserIDDisplay = "User Resource ID"
	retSuccessKey    = "success"
)

// successReturnType is shared across all schemas - define once, reuse everywhere.
var successReturnType = []*config.Field{
	{Name: retSuccessKey, DisplayName: "Success", Field: &config.Field_BoolField{}},
}

var disableUserSchema = &v2.BatonActionSchema{
	Name:        actionDisableUser,
	DisplayName: "Disable User",
	Description: "Deactivates a Snowflake user (sets DISABLED = TRUE). Reversible via enable_user.",
	Arguments: []*config.Field{
		{Name: argUserIDKey, DisplayName: argUserIDDisplay, Field: &config.Field_StringField{}, IsRequired: true},
	},
	ReturnTypes: successReturnType,
	ActionType:  []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT, v2.ActionType_ACTION_TYPE_ACCOUNT_DISABLE},
}

var enableUserSchema = &v2.BatonActionSchema{
	Name:        actionEnableUser,
	DisplayName: "Enable User",
	Description: "Reactivates a Snowflake user (sets DISABLED = FALSE).",
	Arguments: []*config.Field{
		{Name: argUserIDKey, DisplayName: argUserIDDisplay, Field: &config.Field_StringField{}, IsRequired: true},
	},
	ReturnTypes: successReturnType,
	ActionType:  []v2.ActionType{v2.ActionType_ACTION_TYPE_ACCOUNT, v2.ActionType_ACTION_TYPE_ACCOUNT_ENABLE},
}

var _ connectorbuilder.GlobalActionProvider = (*Connector)(nil)

func (c *Connector) GlobalActions(ctx context.Context, registry actions.ActionRegistry) error {
	if err := registry.Register(ctx, disableUserSchema, c.disableUserHandler); err != nil {
		return fmt.Errorf("baton-snowflake: register disable_user: %w", err)
	}
	if err := registry.Register(ctx, enableUserSchema, c.enableUserHandler); err != nil {
		return fmt.Errorf("baton-snowflake: register enable_user: %w", err)
	}
	return nil
}

func successStruct() *structpb.Struct {
	return &structpb.Struct{
		Fields: map[string]*structpb.Value{
			retSuccessKey: {Kind: &structpb.Value_BoolValue{BoolValue: true}},
		},
	}
}

func (c *Connector) disableUserHandler(
	ctx context.Context,
	args *structpb.Struct,
) (*structpb.Struct, annotations.Annotations, error) {
	if args == nil {
		return nil, nil, status.Error(codes.InvalidArgument, "baton-snowflake: missing arguments")
	}
	userID, err := actions.RequireStringArg(args, argUserIDKey)
	if err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "baton-snowflake: user_id: %v", err)
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return nil, nil, status.Error(codes.InvalidArgument, "baton-snowflake: user_id must not be empty")
	}

	if err := c.Client.SetUserDisabled(ctx, userID, true); err != nil {
		return nil, nil, fmt.Errorf("baton-snowflake: disable user %s: %w", userID, err)
	}
	return successStruct(), nil, nil
}

func (c *Connector) enableUserHandler(
	ctx context.Context,
	args *structpb.Struct,
) (*structpb.Struct, annotations.Annotations, error) {
	if args == nil {
		return nil, nil, status.Error(codes.InvalidArgument, "baton-snowflake: missing arguments")
	}
	userID, err := actions.RequireStringArg(args, argUserIDKey)
	if err != nil {
		return nil, nil, status.Errorf(codes.InvalidArgument, "baton-snowflake: user_id: %v", err)
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return nil, nil, status.Error(codes.InvalidArgument, "baton-snowflake: user_id must not be empty")
	}

	if err := c.Client.SetUserDisabled(ctx, userID, false); err != nil {
		return nil, nil, fmt.Errorf("baton-snowflake: enable user %s: %w", userID, err)
	}
	return successStruct(), nil, nil
}
