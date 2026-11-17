package connector

import (
	"bytes"
	"context"
	"crypto/rand"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	configv1 "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/conductorone/baton-snowflake/local_vendor/baton-multipass-sdk/pkg/multipass"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

const issuerTestUser = "service-user"

var issuedTokenNameRE = regexp.MustCompile(`c1-\d{8}T\d{6}Z-[a-z2-7]{6}`)

func issuerTestBuilder(t *testing.T, mock credentialIssueMock) (*programmaticAccessTokenBuilder, *[]string) {
	t.Helper()
	statements := []string{}
	mock.statements = &statements
	server := serveCredentialIssueMock(t, mock)
	t.Cleanup(server.Close)
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	require.NoError(t, err)
	b := newProgrammaticAccessTokenBuilder(client)
	b.issueCredentials = true
	return b, &statements
}

func issuerArgs(t *testing.T, resourceType, resource string) *structpb.Struct {
	t.Helper()
	v, err := structpb.NewValue(map[string]any{"resource_type_id": resourceType, "resource_id": resource})
	require.NoError(t, err)
	return &structpb.Struct{Fields: map[string]*structpb.Value{actionArgSubject: v}}
}

func serviceUserMock() credentialIssueMock {
	return credentialIssueMock{userType: "SERVICE", defaultRole: "service_role", roleGranted: true}
}

func statementsMatching(statements []string, re string) []string {
	var out []string
	for _, s := range statements {
		if regexp.MustCompile(re).MatchString(s) {
			out = append(out, s)
		}
	}
	return out
}

func newIssuerActionManager(t *testing.T, c *Connector) *actions.ActionManager {
	t.Helper()
	ctx := context.Background()
	mgr := actions.NewActionManager(ctx)
	for _, syncer := range c.ResourceSyncers(ctx) {
		provider, ok := syncer.(connectorbuilder.ResourceActionProvider)
		if !ok {
			continue
		}
		registry, err := mgr.GetTypeRegistry(ctx, syncer.ResourceType(ctx).GetId())
		require.NoError(t, err)
		require.NoError(t, provider.ResourceActions(ctx, registry))
	}
	return mgr
}

// Schema shape: what C1 keys on to recognize a credential issuer.
func TestIssuerSchemaShape(t *testing.T) {
	mgr := newIssuerActionManager(t, &Connector{IssueCredentials: true})
	schemas, _, err := mgr.ListActionSchemas(context.Background(), programmaticAccessTokenResourceType.Id)
	require.NoError(t, err)
	require.Len(t, schemas, 1, "one RESOURCE_CREATE per resource type")
	schema := schemas[0]
	require.Equal(t, ActionIssueProgrammaticAccessToken, schema.GetName())
	require.Equal(t, programmaticAccessTokenResourceType.Id, schema.GetResourceTypeId())
	require.Equal(t, []v2.ActionType{v2.ActionType_ACTION_TYPE_RESOURCE_CREATE}, schema.GetActionType())

	var secrets, resources []*configv1.Field
	for _, f := range schema.GetReturnTypes() {
		if f.GetIsSecret() {
			secrets = append(secrets, f)
		}
		if f.HasResourceField() {
			resources = append(resources, f)
		}
	}
	require.Len(t, secrets, 1)
	require.Equal(t, multipass.ContentType, secrets[0].GetContentType())
	require.Len(t, resources, 1)

	// One required subject, then the optional admin-fixed settings; never the name.
	args := schema.GetArguments()
	require.Len(t, args, 5)
	subject := args[0]
	require.Equal(t, actionArgSubject, subject.GetName())
	require.True(t, subject.GetIsRequired())
	require.Equal(t, []string{userResourceType.Id}, subject.GetResourceIdField().GetRules().GetAllowedResourceTypeIds())
	got := map[string]*configv1.Field{}
	for _, a := range args[1:] {
		require.False(t, a.GetIsRequired(), a.GetName())
		require.NotEmpty(t, a.GetDescription(), a.GetName())
		got[a.GetName()] = a
	}
	require.Len(t, got, 4)
	require.True(t, got[actionArgDaysToExpiry].HasIntField())
	require.EqualValues(t, 1, got[actionArgDaysToExpiry].GetIntField().GetRules().GetGte())
	require.EqualValues(t, 365, got[actionArgDaysToExpiry].GetIntField().GetRules().GetLte())
	require.Contains(t, got[actionArgDaysToExpiry].GetDescription(), "15")
	require.True(t, got[actionArgRoleRestriction].HasStringField())
	require.True(t, got[actionArgMinsToBypass].HasIntField())
	require.EqualValues(t, 1440, got[actionArgMinsToBypass].GetIntField().GetRules().GetLte())
	require.Contains(t, got[actionArgMinsToBypass].GetDescription(), "PERSON")
	require.True(t, got[actionArgComment].HasStringField())

	// No other type carries an issuer.
	for _, other := range []string{userResourceType.Id, databaseResourceType.Id} {
		got, _, err := mgr.ListActionSchemas(context.Background(), other)
		require.NoError(t, err)
		require.Empty(t, got, other)
	}
}

func TestIssuerNotRegisteredWithoutFlag(t *testing.T) {
	mgr := newIssuerActionManager(t, &Connector{})
	schemas, _, err := mgr.ListActionSchemas(context.Background(), programmaticAccessTokenResourceType.Id)
	require.NoError(t, err)
	require.Empty(t, schemas)
}

func TestIssuerResult(t *testing.T) {
	b, statements := issuerTestBuilder(t, serviceUserMock())

	response, plaintext, _, err := b.issueProgrammaticAccessToken(context.Background(), issuerArgs(t, userResourceType.Id, issuerTestUser))
	require.NoError(t, err)

	adds := statementsMatching(*statements, `ADD PROGRAMMATIC ACCESS TOKEN`)
	require.Len(t, adds, 1)
	name := issuedTokenNameRE.FindString(adds[0])
	require.NotEmpty(t, name, "token name format: %s", adds[0])
	require.Contains(t, adds[0], `ROLE_RESTRICTION = 'service_role'`)
	require.Contains(t, adds[0], "DAYS_TO_EXPIRY = 15;")

	res := response.GetFields()[actionReturnResource].GetStructValue()
	require.NotNil(t, res)
	id := res.GetFields()["resourceId"].GetStructValue().GetFields()
	require.Equal(t, programmaticAccessTokenResourceType.Id, id["resourceTypeId"].GetStringValue())
	require.Equal(t, programmaticAccessTokenID(issuerTestUser, name), id["resourceId"].GetStringValue(),
		"the id is the one List reports")
	annos := res.GetFields()["annotations"].String()
	require.Contains(t, annos, "SecretTrait")
	require.Regexp(t, `(?i)expires_?at`, annos)

	require.Len(t, plaintext, 1)
	require.Equal(t, actionReturnSecret, plaintext[0].GetName())
	require.Equal(t, multipass.ContentType, plaintext[0].GetSchema())
	require.JSONEq(t, `{"key_value":"redacted","provider":"snowflake","key_id":"`+name+`"}`, string(plaintext[0].GetBytes()))
	require.NotContains(t, response.String(), "redacted", "the structpb response never carries the secret")
}

func TestIssuerResourceMatchesListScheme(t *testing.T) {
	want, err := newProgrammaticAccessTokenResource(
		v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: issuerTestUser}.Build(),
		"tok", time.Now(), nil, nil)
	require.NoError(t, err)
	require.Equal(t, programmaticAccessTokenID(issuerTestUser, "tok"), want.GetId().GetResource())
	_, _, err = parseProgrammaticAccessTokenID(want.GetId().GetResource())
	require.NoError(t, err)
}

func TestIssuerPersonUserHasNoRoleRestriction(t *testing.T) {
	b, statements := issuerTestBuilder(t, credentialIssueMock{userType: "PERSON", defaultRole: "null"})
	_, _, _, err := b.issueProgrammaticAccessToken(context.Background(), issuerArgs(t, userResourceType.Id, "person"))
	require.NoError(t, err)
	adds := statementsMatching(*statements, `ADD PROGRAMMATIC ACCESS TOKEN`)
	require.Len(t, adds, 1)
	require.NotContains(t, adds[0], "ROLE_RESTRICTION")
}

func TestIssuerServiceUserWithoutDefaultRoleRefusedBeforeAnyStatement(t *testing.T) {
	for name, mock := range map[string]credentialIssueMock{
		"null default role":   {userType: "SERVICE", defaultRole: "null"},
		"ungranted default":   {userType: "SERVICE", defaultRole: "service_role", roleGranted: false},
		"legacy service user": {userType: "LEGACY_SERVICE", defaultRole: "null"},
	} {
		t.Run(name, func(t *testing.T) {
			b, statements := issuerTestBuilder(t, mock)
			response, plaintext, _, err := b.issueProgrammaticAccessToken(context.Background(), issuerArgs(t, userResourceType.Id, issuerTestUser))
			require.Error(t, err)
			require.Nil(t, response)
			require.Empty(t, plaintext)
			require.Empty(t, statementsMatching(*statements, `PROGRAMMATIC ACCESS TOKEN`), "no mint and no removal")
		})
	}
}

// A failure after the ALTER USER statement may have been sent removes exactly
// the generated name.
func TestIssuerFailureAfterMintRemovesExactlyThatName(t *testing.T) {
	for name, mock := range map[string]credentialIssueMock{
		"ambiguous 5xx":    {addStatus: http.StatusBadGateway},
		"denied (errored)": {denyPrefix: `ALTER USER "service-user" ADD`},
	} {
		t.Run(name, func(t *testing.T) {
			mock.userType, mock.defaultRole, mock.roleGranted = "SERVICE", "service_role", true
			b, statements := issuerTestBuilder(t, mock)
			response, plaintext, _, err := b.issueProgrammaticAccessToken(context.Background(), issuerArgs(t, userResourceType.Id, issuerTestUser))
			require.Error(t, err)
			require.Nil(t, response)
			require.Empty(t, plaintext)
			require.NotContains(t, err.Error(), "redacted")

			adds := statementsMatching(*statements, `ADD PROGRAMMATIC ACCESS TOKEN`)
			require.Len(t, adds, 1, "no retry")
			tokenName := issuedTokenNameRE.FindString(adds[0])
			removes := statementsMatching(*statements, `REMOVE PROGRAMMATIC ACCESS TOKEN`)
			require.Equal(t, []string{`ALTER USER IF EXISTS "service-user" REMOVE PROGRAMMATIC ACCESS TOKEN IF EXISTS "` + tokenName + `";`}, removes)
		})
	}
}

func TestIssuerRejectsBadArguments(t *testing.T) {
	b, statements := issuerTestBuilder(t, serviceUserMock())
	for name, args := range map[string]*structpb.Struct{
		"missing subject":    {},
		"wrong subject type": issuerArgs(t, databaseResourceType.Id, "db"),
		"empty subject id":   issuerArgs(t, userResourceType.Id, ""),
	} {
		_, _, _, err := b.issueProgrammaticAccessToken(context.Background(), args)
		require.Equal(t, codes.InvalidArgument, status.Code(err), name)
	}
	require.Empty(t, *statements, "every rejection happens before Snowflake is touched")
}

func TestIssuerIgnoresCallerSuppliedName(t *testing.T) {
	b, statements := issuerTestBuilder(t, serviceUserMock())
	args := issuerArgs(t, userResourceType.Id, issuerTestUser)
	args.Fields["name"] = structpb.NewStringValue("attacker-chosen")
	_, _, _, err := b.issueProgrammaticAccessToken(context.Background(), args)
	require.NoError(t, err)
	adds := statementsMatching(*statements, `ADD PROGRAMMATIC ACCESS TOKEN`)
	require.Len(t, adds, 1)
	require.NotContains(t, adds[0], "attacker-chosen")
	require.Regexp(t, issuedTokenNameRE, adds[0])
}

func TestIssuerCreateThenDelete(t *testing.T) {
	ctx := context.Background()
	b, statements := issuerTestBuilder(t, serviceUserMock())
	response, _, _, err := b.issueProgrammaticAccessToken(ctx, issuerArgs(t, userResourceType.Id, issuerTestUser))
	require.NoError(t, err)
	returned := response.GetFields()[actionReturnResource].GetStructValue().GetFields()["resourceId"].GetStructValue().GetFields()["resourceId"].GetStringValue()

	_, err = b.Delete(ctx, v2.ResourceId_builder{ResourceType: programmaticAccessTokenResourceType.Id, Resource: returned}.Build(), nil)
	require.NoError(t, err)
	adds := statementsMatching(*statements, `ADD PROGRAMMATIC ACCESS TOKEN`)
	removes := statementsMatching(*statements, `REMOVE PROGRAMMATIC ACCESS TOKEN`)
	require.Len(t, removes, 1)
	require.Contains(t, removes[0], issuedTokenNameRE.FindString(adds[0]))
}

func TestIssuerSecretTraitExpiryIsDefaultDays(t *testing.T) {
	b, _ := issuerTestBuilder(t, serviceUserMock())
	response, _, _, err := b.issueProgrammaticAccessToken(context.Background(), issuerArgs(t, userResourceType.Id, issuerTestUser))
	require.NoError(t, err)
	res := response.GetFields()[actionReturnResource].GetStructValue()
	require.NotNil(t, res)
	require.Contains(t, res.GetFields()["annotations"].String(), "expiresAt")
}

func TestNewIssuedTokenName(t *testing.T) {
	now := time.Date(2026, 1, 2, 15, 4, 5, 0, time.FixedZone("x", 3600))
	name, err := newIssuedTokenName(now, bytes.NewReader([]byte{0, 0, 0, 0}))
	require.NoError(t, err)
	require.Equal(t, "c1-20260102T140405Z-aaaaaa", name)

	again, err := newIssuedTokenName(now, rand.Reader)
	require.NoError(t, err)
	require.Regexp(t, `^c1-\d{8}T\d{6}Z-[a-z2-7]{6}$`, again)

	_, err = newIssuedTokenName(now, bytes.NewReader(nil))
	require.Error(t, err)
}

func withArgs(t *testing.T, kv map[string]any) *structpb.Struct {
	t.Helper()
	args := issuerArgs(t, userResourceType.Id, issuerTestUser)
	for k, v := range kv {
		pv, err := structpb.NewValue(v)
		require.NoError(t, err)
		args.Fields[k] = pv
	}
	return args
}

func TestIssuerOptionalArgumentsEmitted(t *testing.T) {
	b, statements := issuerTestBuilder(t, credentialIssueMock{userType: "PERSON", defaultRole: "analyst", roleGranted: true})
	_, _, _, err := b.issueProgrammaticAccessToken(context.Background(), withArgs(t, map[string]any{
		actionArgDaysToExpiry:    float64(3),
		actionArgRoleRestriction: "analyst",
		actionArgMinsToBypass:    float64(45),
		actionArgComment:         `it's \ ok`,
	}))
	require.NoError(t, err)
	adds := statementsMatching(*statements, `ADD PROGRAMMATIC ACCESS TOKEN`)
	require.Len(t, adds, 1)
	require.Contains(t, adds[0], ` ROLE_RESTRICTION = 'analyst' DAYS_TO_EXPIRY = 3 MINS_TO_BYPASS_NETWORK_POLICY_REQUIREMENT = 45 COMMENT = 'it''s \\ ok';`)
}

func TestIssuerOmittedArgumentsUseDefaults(t *testing.T) {
	b, statements := issuerTestBuilder(t, credentialIssueMock{userType: "PERSON", defaultRole: "null"})
	response, _, _, err := b.issueProgrammaticAccessToken(context.Background(), issuerArgs(t, userResourceType.Id, issuerTestUser))
	require.NoError(t, err)
	add := statementsMatching(*statements, `ADD PROGRAMMATIC ACCESS TOKEN`)[0]
	require.Contains(t, add, "DAYS_TO_EXPIRY = 15;")
	for _, absent := range []string{"ROLE_RESTRICTION", "MINS_TO_BYPASS", "COMMENT"} {
		require.NotContains(t, add, absent)
	}
	require.NotNil(t, response)
}

func TestIssuerExplicitRoleOverridesDefaultAndIsChecked(t *testing.T) {
	// A service user with an explicit role: the explicit role wins over the default role.
	b, statements := issuerTestBuilder(t, credentialIssueMock{userType: "SERVICE", defaultRole: "other_role", roleGranted: true})
	_, _, _, err := b.issueProgrammaticAccessToken(context.Background(), withArgs(t, map[string]any{actionArgRoleRestriction: "other_role"}))
	require.NoError(t, err)
	require.Contains(t, statementsMatching(*statements, `ADD PROGRAMMATIC`)[0], `ROLE_RESTRICTION = 'other_role'`)

	// A role that is not granted is refused before the mint statement.
	b, statements = issuerTestBuilder(t, credentialIssueMock{userType: "PERSON", defaultRole: "x", roleGranted: false})
	_, _, _, err = b.issueProgrammaticAccessToken(context.Background(), withArgs(t, map[string]any{actionArgRoleRestriction: "x"}))
	require.ErrorContains(t, err, "is not granted")
	require.Empty(t, statementsMatching(*statements, `PROGRAMMATIC ACCESS TOKEN`))
}

func TestIssuerExpiryFollowsDaysToExpiry(t *testing.T) {
	b, _ := issuerTestBuilder(t, credentialIssueMock{userType: "PERSON", defaultRole: "null"})
	response, _, _, err := b.issueProgrammaticAccessToken(context.Background(), withArgs(t, map[string]any{actionArgDaysToExpiry: float64(1)}))
	require.NoError(t, err)
	res := response.GetFields()[actionReturnResource].GetStructValue()
	annos := res.GetFields()["annotations"].String()
	want := time.Now().UTC().AddDate(0, 0, 1)
	require.Contains(t, annos, want.Format("2006-01-02"), "expires_at derives from the days actually used")
}

func TestIssuerInvalidArgumentsRefusedBeforeAnyStatement(t *testing.T) {
	for name, kv := range map[string]map[string]any{
		"days zero":          {actionArgDaysToExpiry: float64(0)},
		"days too large":     {actionArgDaysToExpiry: float64(366)},
		"days fractional":    {actionArgDaysToExpiry: 1.5},
		"days wrong type":    {actionArgDaysToExpiry: "7"},
		"mins zero":          {actionArgMinsToBypass: float64(0)},
		"mins too large":     {actionArgMinsToBypass: float64(1441)},
		"comment too long":   {actionArgComment: strings.Repeat("a", 257)},
		"comment newline":    {actionArgComment: "a\nb"},
		"comment nul":        {actionArgComment: "a\x00b"},
		"comment wrong type": {actionArgComment: float64(1)},
		"role newline":       {actionArgRoleRestriction: "a\nb"},
		"role too long":      {actionArgRoleRestriction: strings.Repeat("r", 256)},
	} {
		t.Run(name, func(t *testing.T) {
			b, statements := issuerTestBuilder(t, credentialIssueMock{userType: "PERSON", defaultRole: "null"})
			_, _, _, err := b.issueProgrammaticAccessToken(context.Background(), withArgs(t, kv))
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.Empty(t, *statements)
		})
	}
}

func TestIssuerBypassRefusedForServiceUsers(t *testing.T) {
	for _, userType := range []string{"SERVICE", "LEGACY_SERVICE"} {
		b, statements := issuerTestBuilder(t, credentialIssueMock{userType: userType, defaultRole: "service_role", roleGranted: true})
		_, _, _, err := b.issueProgrammaticAccessToken(context.Background(), withArgs(t, map[string]any{actionArgMinsToBypass: float64(10)}))
		require.Equal(t, codes.InvalidArgument, status.Code(err), userType)
		require.Empty(t, statementsMatching(*statements, `PROGRAMMATIC ACCESS TOKEN`), "no mint statement")
	}
}
