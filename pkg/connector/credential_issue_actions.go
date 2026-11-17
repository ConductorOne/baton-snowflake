package connector

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	configv1 "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/conductorone/baton-snowflake/local_vendor/baton-multipass-sdk/pkg/multipass"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

// The programmatic_access_token type carries one issuer: a resource-scoped
// ACTION_TYPE_RESOURCE_CREATE action that mints a programmatic access token for
// a user. C1 recognizes it as a credential issuer by its schema alone: the
// action type, the one secret return whose content type is a Multipass type,
// and the resource return that carries the created token. Revoking an issued
// token is the resource delete the type's syncer already provides.
//
// This is the only issuance path; the connector registers no IssueCredential
// handler.

const (
	// ActionIssueProgrammaticAccessToken is the issuer registered on
	// programmatic_access_token.
	ActionIssueProgrammaticAccessToken = "issue_programmatic_access_token"

	// actionArgSubject is the one subject: the user the token belongs to.
	actionArgSubject = "user"

	// Optional admin-fixed arguments, one per clause of ALTER USER ... ADD
	// PROGRAMMATIC ACCESS TOKEN. The token name is never an argument.
	actionArgDaysToExpiry    = "days_to_expiry"
	actionArgRoleRestriction = "role_restriction"
	actionArgMinsToBypass    = "mins_to_bypass_network_policy_requirement"
	actionArgComment         = "comment"

	minDaysToExpiry       = 1
	maxDaysToExpiry       = 365
	minMinsToBypass       = 1
	maxMinsToBypass       = 1440
	maxRoleRestrictionLen = 255
	maxCommentLen         = 256

	// actionReturnSecret is the secret return and the PlaintextData name.
	actionReturnSecret = "api_key"
	// actionReturnResource is the created token resource, carrying its SecretTrait.
	actionReturnResource = "resource"

	// apiKeyV2Provider is the api_key_v2 `provider` value for a Snowflake token.
	apiKeyV2Provider = "snowflake"

	// issuedTokenNameSuffixLen is the random part of an issued token name: 6
	// base32 characters, 30 bits.
	issuedTokenNameSuffixLen = 6

	// discardTimeout bounds the best-effort removal of a token that cannot be
	// returned.
	discardTimeout = 30 * time.Second
)

// programmaticAccessTokenActionSchema is the issuer schema. The user is the one
// required subject; every other argument is an optional setting an admin can
// fix. The token name is always chosen by the connector.
func programmaticAccessTokenActionSchema() *v2.BatonActionSchema {
	secret := multipass.OutputField(actionReturnSecret)
	secret.SetIsRequired(true)
	return v2.BatonActionSchema_builder{
		Name:        ActionIssueProgrammaticAccessToken,
		DisplayName: "Issue Snowflake programmatic access token",
		Description: "Creates a programmatic access token for a Snowflake user and returns it as a Multipass api_key_v2 secret.",
		Arguments: []*configv1.Field{
			configv1.Field_builder{
				Name:        actionArgSubject,
				DisplayName: "User",
				Description: "The Snowflake user the token is issued for.",
				IsRequired:  true,
				ResourceIdField: configv1.ResourceIdField_builder{
					Rules: configv1.ResourceIDRules_builder{
						AllowedResourceTypeIds: []string{userResourceType.Id},
					}.Build(),
				}.Build(),
			}.Build(),
			configv1.Field_builder{
				Name:        actionArgDaysToExpiry,
				DisplayName: "Days to expiry",
				Description: fmt.Sprintf("Optional. Days until the token expires, %d to %d. Defaults to %d.", minDaysToExpiry, maxDaysToExpiry, programmaticAccessTokenDefaultDays),
				IntField: configv1.IntField_builder{
					Rules: configv1.Int64Rules_builder{Gte: proto.Int64(minDaysToExpiry), Lte: proto.Int64(maxDaysToExpiry)}.Build(),
				}.Build(),
			}.Build(),
			configv1.Field_builder{
				Name:        actionArgRoleRestriction,
				DisplayName: "Role restriction",
				Description: "Optional. Snowflake role the token is restricted to; it must be granted to the user. " +
					"Unquoted names are case-insensitive (analyst is ANALYST); a double-quoted name (\"Mixed Case\") is matched exactly. " +
					"Defaults to the user's default role for service users, which must have one, and to no restriction for PERSON users.",
				StringField: configv1.StringField_builder{}.Build(),
			}.Build(),
			configv1.Field_builder{
				Name:        actionArgMinsToBypass,
				DisplayName: "Minutes to bypass network policy requirement",
				Description: fmt.Sprintf("Optional. PERSON users only; refused for service users. Minutes, %d to %d, during which the token may be used without a network policy. "+
					"Omitted by default.", minMinsToBypass, maxMinsToBypass),
				IntField: configv1.IntField_builder{
					Rules: configv1.Int64Rules_builder{Gte: proto.Int64(minMinsToBypass), Lte: proto.Int64(maxMinsToBypass)}.Build(),
				}.Build(),
			}.Build(),
			configv1.Field_builder{
				Name:        actionArgComment,
				DisplayName: "Comment",
				Description: fmt.Sprintf("Optional. Comment stored on the token, at most %d characters, no control characters. Omitted by default.", maxCommentLen),
				StringField: configv1.StringField_builder{}.Build(),
			}.Build(),
		},
		ReturnTypes: []*configv1.Field{
			secret,
			configv1.Field_builder{
				Name:          actionReturnResource,
				DisplayName:   "Secret resource",
				Description:   "The created token, so it can be revoked.",
				IsRequired:    true,
				ResourceField: configv1.ResourceField_builder{}.Build(),
			}.Build(),
		},
		ActionType: []v2.ActionType{v2.ActionType_ACTION_TYPE_RESOURCE_CREATE},
	}.Build()
}

var _ connectorbuilder.ResourceActionProvider = (*programmaticAccessTokenBuilder)(nil)

// ResourceActions registers the issuer on the programmatic_access_token type
// when issue-credentials is set.
func (o *programmaticAccessTokenBuilder) ResourceActions(ctx context.Context, registry actions.ActionRegistry) error {
	if !o.issueCredentials {
		return nil
	}
	if err := actions.RegisterWithSecrets(ctx, registry, programmaticAccessTokenActionSchema(), o.issueProgrammaticAccessToken); err != nil {
		return fmt.Errorf("baton-snowflake: failed to register %s action: %w", ActionIssueProgrammaticAccessToken, err)
	}
	return nil
}

// issueProgrammaticAccessToken creates one token for the user the request
// names, with optional admin-fixed settings. Issuance is not retried: a retry after an ambiguous failure would
// leave a second live token nobody holds.
func (o *programmaticAccessTokenBuilder) issueProgrammaticAccessToken(
	ctx context.Context,
	args *structpb.Struct,
) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
	subject, err := actions.RequireResourceIDArg(args, actionArgSubject)
	if err != nil {
		return nil, nil, nil, status.Errorf(codes.InvalidArgument, "baton-snowflake: %v", err)
	}
	userName := subject.GetResource()
	if subject.GetResourceType() != userResourceType.Id || userName == "" {
		return nil, nil, nil, status.Errorf(codes.InvalidArgument, "baton-snowflake: %s must be a %s resource", actionArgSubject, userResourceType.Id)
	}

	opts, err := parseIssuerOptions(args)
	if err != nil {
		return nil, nil, nil, err
	}

	tokenName, err := newIssuedTokenName(time.Now(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}

	user, _, err := o.client.GetUser(ctx, nil, userName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("baton-snowflake: get user for programmatic access token: %w", err)
	}
	serviceUser := isServiceUserType(user.Type)
	if opts.MinsToBypassNetworkPolicy > 0 && serviceUser {
		return nil, nil, nil, status.Errorf(codes.InvalidArgument,
			"baton-snowflake: %s applies only to PERSON users; user %q is a service user",
			actionArgMinsToBypass, userName)
	}
	roleRestriction := opts.RoleRestriction
	if roleRestriction == "" && serviceUser {
		// DESCRIBE USER renders an unset DEFAULT_ROLE as Snowflake's textual NULL,
		// not as an empty string, so this must normalize before testing for absence.
		roleRestriction = snowflake.NormalizeNullValue(user.DefaultRole)
		if roleRestriction == "" {
			return nil, nil, nil, fmt.Errorf(
				"baton-snowflake: service user %q has no default role; assign a role and set it as "+
					"the user's default role before issuing a programmatic access token",
				userName,
			)
		}
	}
	if roleRestriction != "" {
		granted, err := o.client.RoleGrantedToUser(ctx, userName, roleRestriction)
		switch {
		case err == nil && !granted:
			return nil, nil, nil, fmt.Errorf(
				"baton-snowflake: role %q is not granted to user %q; "+
					"grant it before issuing a programmatic access token",
				roleRestriction, userName,
			)
		case snowflake.IsInsufficientPrivileges(err):
			// SHOW GRANTS TO USER needs privileges that creating the token does not.
			// This check only turns a Snowflake rejection into a better message, so a
			// role that cannot run it must still be allowed to issue.
			ctxzap.Extract(ctx).Warn("baton-snowflake: skipping role check: insufficient privileges",
				zap.String("username", userName))
		case err != nil:
			return nil, nil, nil, fmt.Errorf("baton-snowflake: verify token role restriction: %w", err)
		}
	}
	opts.RoleRestriction = roleRestriction

	// Snowflake derives the expiry from its own clock at ALTER USER time, so sample
	// as late as possible: every round-trip between here and the statement pushes the
	// real expiry further past the one computed below. A whole number of days keeps
	// the local value from ever being later than Snowflake's.
	expiresAt := time.Now().UTC().AddDate(0, 0, opts.DaysToExpiry)

	// From here the ALTER USER statement may have reached Snowflake. Any failure,
	// including an errored or ambiguous statement, may leave a live token that no
	// resource references, and the SDK does not retry issuance. The name is unique
	// to this call and removal is idempotent, so removing by name is always safe.
	issued := false
	defer func() {
		if !issued {
			o.discardToken(ctx, userName, tokenName)
		}
	}()

	secret, err := o.client.CreateProgrammaticAccessToken(ctx, userName, tokenName, opts)

	if err != nil {
		return nil, nil, nil, fmt.Errorf("baton-snowflake: create programmatic access token: %w", err)
	}

	resource, err := newProgrammaticAccessTokenResource(subject, tokenName, expiresAt, nil, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("baton-snowflake: build issuer resource: %w", err)
	}
	field, err := actions.NewResourceReturnField(actionReturnResource, resource)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("baton-snowflake: build issuer resource return: %w", err)
	}
	// Plaintext carries the schema; the SDK does not copy it from the field.
	plaintext, err := multipass.Plaintext(actionReturnSecret, multipass.APIKeyV2{
		KeyValue: secret,
		Provider: apiKeyV2Provider,
		KeyID:    tokenName,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("baton-snowflake: encode api_key_v2: %w", err)
	}

	issued = true
	return actions.NewReturnValues(true, field), []*v2.PlaintextData{plaintext}, nil, nil
}

// parseIssuerOptions reads and validates the optional admin-fixed arguments
// before anything touches Snowflake. RoleRestriction is returned as supplied and
// resolved against the user by the caller.
func parseIssuerOptions(args *structpb.Struct) (snowflake.ProgrammaticAccessTokenOptions, error) {
	opts := snowflake.ProgrammaticAccessTokenOptions{DaysToExpiry: programmaticAccessTokenDefaultDays}
	var err error
	if v, ok, e := optionalIntArg(args, actionArgDaysToExpiry, minDaysToExpiry, maxDaysToExpiry); e != nil {
		return opts, e
	} else if ok {
		opts.DaysToExpiry = v
	}
	if v, ok, e := optionalIntArg(args, actionArgMinsToBypass, minMinsToBypass, maxMinsToBypass); e != nil {
		return opts, e
	} else if ok {
		opts.MinsToBypassNetworkPolicy = v
	}
	if opts.RoleRestriction, err = optionalStringArg(args, actionArgRoleRestriction, maxRoleRestrictionLen); err != nil {
		return opts, err
	}
	if opts.RoleRestriction, err = normalizeRoleIdentifier(opts.RoleRestriction); err != nil {
		return opts, err
	}
	if opts.Comment, err = optionalStringArg(args, actionArgComment, maxCommentLen); err != nil {
		return opts, err
	}
	return opts, nil
}

func argPresent(args *structpb.Struct, key string) (*structpb.Value, bool) {
	v, ok := args.GetFields()[key]
	if !ok || v == nil {
		return nil, false
	}
	if _, isNull := v.GetKind().(*structpb.Value_NullValue); isNull {
		return nil, false
	}
	return v, true
}

func optionalIntArg(args *structpb.Struct, key string, lo, hi int) (int, bool, error) {
	v, ok := argPresent(args, key)
	if !ok {
		return 0, false, nil
	}
	n, isNum := v.GetKind().(*structpb.Value_NumberValue)
	if !isNum || n.NumberValue != math.Trunc(n.NumberValue) {
		return 0, false, status.Errorf(codes.InvalidArgument, "baton-snowflake: %s must be a whole number", key)
	}
	if n.NumberValue < float64(lo) || n.NumberValue > float64(hi) {
		return 0, false, status.Errorf(codes.InvalidArgument, "baton-snowflake: %s must be between %d and %d", key, lo, hi)
	}
	return int(n.NumberValue), true, nil
}

// optionalStringArg returns "" when the argument is absent or blank. The value
// is later rendered as an escaped SQL literal; the checks here keep it short and
// free of control characters.
func optionalStringArg(args *structpb.Struct, key string, maxLen int) (string, error) {
	v, ok := argPresent(args, key)
	if !ok {
		return "", nil
	}
	sv, isStr := v.GetKind().(*structpb.Value_StringValue)
	if !isStr {
		return "", status.Errorf(codes.InvalidArgument, "baton-snowflake: %s must be a string", key)
	}
	s := strings.TrimSpace(sv.StringValue)
	if utf8.RuneCountInString(s) > maxLen {
		return "", status.Errorf(codes.InvalidArgument, "baton-snowflake: %s must be at most %d characters", key, maxLen)
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return "", status.Errorf(codes.InvalidArgument, "baton-snowflake: %s must not contain control characters", key)
		}
	}
	return s, nil
}

// normalizeRoleIdentifier resolves an admin-supplied role name by Snowflake
// identifier rules, because ROLE_RESTRICTION's string literal is matched as an
// exact-case name. A double-quoted value keeps its case, with "" unescaped to ".
// An unquoted value is uppercased and must not contain a double quote. An empty
// input stays empty, meaning "not supplied".
func normalizeRoleIdentifier(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if len(raw) >= 2 && strings.HasPrefix(raw, `"`) && strings.HasSuffix(raw, `"`) {
		inner := raw[1 : len(raw)-1]
		// Inside quotes every " must be doubled.
		if strings.Contains(strings.ReplaceAll(inner, `""`, ""), `"`) {
			return "", status.Errorf(codes.InvalidArgument, "baton-snowflake: %s has an unescaped double quote", actionArgRoleRestriction)
		}
		name := strings.ReplaceAll(inner, `""`, `"`)
		if name == "" {
			return "", status.Errorf(codes.InvalidArgument, "baton-snowflake: %s must not be an empty quoted name", actionArgRoleRestriction)
		}
		return name, nil
	}
	if strings.Contains(raw, `"`) {
		return "", status.Errorf(codes.InvalidArgument, "baton-snowflake: %s contains a double quote outside a fully quoted name", actionArgRoleRestriction)
	}
	return strings.ToUpper(raw), nil
}

// discardToken best-effort removes a token that cannot be returned, since
// nobody would hold it. It detaches from the caller's context, which may be why
// the result could not be returned, and keeps a deadline. Failures are logged
// with names only, never the secret.
func (o *programmaticAccessTokenBuilder) discardToken(ctx context.Context, userName, tokenName string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), discardTimeout)
	defer cancel()
	if err := o.client.RemoveProgrammaticAccessToken(ctx, userName, tokenName); err != nil {
		ctxzap.Extract(ctx).Error("baton-snowflake: failed to remove an undeliverable programmatic access token; remove it by hand",
			zap.String("username", userName), zap.String("token_name", tokenName), zap.Error(err))
	}
}

// newIssuedTokenName names a token the issuer creates: c1-<UTC
// 20060102T150405Z>-<6 random lowercase>, e.g. c1-20261110T201426Z-k3x9qa. The
// clock and randomness are arguments so tests can pin them.
func newIssuedTokenName(now time.Time, random io.Reader) (string, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(random, buf); err != nil {
		return "", fmt.Errorf("baton-snowflake: generate token name: %w", err)
	}
	suffix := strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(buf))[:issuedTokenNameSuffixLen]
	return "c1-" + now.UTC().Format("20060102T150405Z") + "-" + suffix, nil
}
