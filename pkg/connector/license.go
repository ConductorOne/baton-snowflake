package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type licenseBuilder struct {
	resourceType *v2.ResourceType
	client       *snowflake.Client
}

func (l *licenseBuilder) ResourceType(_ context.Context) *v2.ResourceType {
	return licenseResourceType
}

func (l *licenseBuilder) List(
	ctx context.Context,
	_ *v2.ResourceId,
	_ rs.SyncOpAttrs,
) ([]*v2.Resource, *rs.SyncOpResults, error) {
	logger := ctxzap.Extract(ctx)

	accounts, statusCode, err := l.client.ListOrganizationAccounts(ctx)
	if err != nil {
		if snowflake.IsUnprocessableEntity(statusCode, err) {
			logger.Warn("insufficient privileges to list organization accounts for license data; skipping license sync",
				zap.Error(err),
			)
			return nil, nil, nil
		}
		return nil, nil, wrapError(err, "failed to list organization accounts")
	}

	userCount, err := l.client.CountUsers(ctx)
	if err != nil {
		logger.Warn("failed to count users for license data; proceeding without user count",
			zap.Error(err),
		)
	}

	var resources []*v2.Resource
	for _, account := range accounts {
		if account.Edition == "" {
			continue
		}

		licenseName := fmt.Sprintf("Snowflake %s", account.Edition)
		resourceID := fmt.Sprintf("%s:%s", account.AccountName, account.Edition)

		traitOpts := []rs.LicenseProfileTraitOption{
			rs.WithLicenseName(licenseName),
		}
		if userCount > 0 {
			traitOpts = append(traitOpts, rs.WithLicenseSeats(0, userCount))
		}

		licenseTrait, err := rs.NewLicenseProfileTrait(traitOpts...)
		if err != nil {
			return nil, nil, wrapError(err, "failed to create license trait")
		}

		resource, err := rs.NewResource(
			licenseName,
			licenseResourceType,
			resourceID,
			rs.WithAnnotation(licenseTrait),
		)
		if err != nil {
			return nil, nil, wrapError(err, "failed to create license resource")
		}

		resources = append(resources, resource)
	}

	return resources, &rs.SyncOpResults{}, nil
}

func (l *licenseBuilder) Entitlements(
	_ context.Context,
	_ *v2.Resource,
	_ rs.SyncOpAttrs,
) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, &rs.SyncOpResults{}, nil
}

func (l *licenseBuilder) Grants(
	_ context.Context,
	_ *v2.Resource,
	_ rs.SyncOpAttrs,
) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return nil, &rs.SyncOpResults{}, nil
}

func newLicenseBuilder(client *snowflake.Client) *licenseBuilder {
	return &licenseBuilder{
		resourceType: licenseResourceType,
		client:       client,
	}
}
