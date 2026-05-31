package connector

import (
	"context"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

type integrationBuilder struct {
	client *snowflake.Client
}

func (o *integrationBuilder) ResourceType(ctx context.Context) *v2.ResourceType {
	return integrationResourceType
}

// classifyIntegration maps a Snowflake integration to its non-human-identity
// spine value and axis-2 detail string. EXTERNAL OAUTH security integrations are
// app registrations; STORAGE and API integrations assume a cloud IAM role. Every
// other integration kind syncs with an axis-2-only detail (UNSPECIFIED spine) so
// the resource still carries a governed namespaced detail.
func classifyIntegration(integrationType, category string) (v2.NonHumanIdentityTrait_NhiType, string) {
	t := strings.ToUpper(strings.TrimSpace(integrationType))
	c := strings.ToUpper(strings.TrimSpace(category))

	switch {
	case strings.Contains(t, "EXTERNAL_OAUTH"), strings.Contains(t, "EXTERNAL OAUTH"):
		return v2.NonHumanIdentityTrait_NHI_TYPE_APP_REGISTRATION, "snowflake.integration.external_oauth"
	case c == "STORAGE":
		return v2.NonHumanIdentityTrait_NHI_TYPE_ASSUMABLE_ROLE, "snowflake.integration.storage"
	case c == "API":
		return v2.NonHumanIdentityTrait_NHI_TYPE_ASSUMABLE_ROLE, "snowflake.integration.api"
	default:
		return v2.NonHumanIdentityTrait_NHI_TYPE_UNSPECIFIED, "snowflake.integration." + normalizeDetailToken(c)
	}
}

// normalizeDetailToken lowercases an integration category/type and collapses any
// non-alphanumeric runs into single underscores so it is a valid dotted-lowercase
// axis-2 detail token (e.g. "EXTERNAL ACCESS" -> "external_access").
func normalizeDetailToken(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	lastUnderscore := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteRune('_')
			lastUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}

func integrationResource(integration *snowflake.Integration) (*v2.Resource, error) {
	profile := map[string]interface{}{
		profileKeyName:    integration.Name,
		"type":            integration.Type,
		"category":        integration.Category,
		profileKeyComment: integration.Comment,
	}

	nhiType, nhiDetail := classifyIntegration(integration.Type, integration.Category)

	integrationTraits := []rs.AppTraitOption{
		rs.WithAppProfile(profile),
	}

	resource, err := rs.NewAppResource(
		integration.Name,
		integrationResourceType,
		integration.Name,
		integrationTraits,
		rs.WithNHIType(nhiType, nhiDetail),
	)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (o *integrationBuilder) List(ctx context.Context, parentResourceID *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	l := ctxzap.Extract(ctx)

	integrations, err := o.client.ListIntegrations(ctx)
	if err != nil {
		// A role without privileges to view integrations can surface a 422
		// rather than an empty result set; treat that as "nothing visible".
		if isUnprocessableEntityError(err) {
			l.Debug("ListIntegrations: insufficient privileges, skipping", zap.Error(err))
			return nil, nil, nil
		}
		return nil, nil, wrapError(err, "failed to list integrations")
	}

	var resources []*v2.Resource
	for _, integration := range integrations {
		resource, err := integrationResource(&integration) // #nosec G601
		if err != nil {
			return nil, nil, wrapError(err, "failed to create integration resource")
		}
		resources = append(resources, resource)
	}

	return resources, nil, nil
}

func (o *integrationBuilder) Entitlements(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func (o *integrationBuilder) Grants(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func newIntegrationBuilder(client *snowflake.Client) *integrationBuilder {
	return &integrationBuilder{
		client: client,
	}
}
