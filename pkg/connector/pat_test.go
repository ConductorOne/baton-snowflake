package connector

import (
	"context"
	"testing"
	"time"

	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPatResource_BasicFields(t *testing.T) {
	ctx := context.Background()
	createdOn := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	expiresAt := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	pat := &snowflake.ProgrammaticAccessToken{
		Name:      "my_pat",
		UserName:  "ALICE",
		Status:    "ACTIVE",
		CreatedOn: createdOn,
		ExpiresAt: expiresAt,
	}

	parentID, err := rs.NewResourceID(userResourceType, "ALICE")
	require.NoError(t, err)

	resource, err := patResource(ctx, pat, parentID)
	require.NoError(t, err)
	require.NotNil(t, resource)

	assert.Equal(t, "my_pat", resource.DisplayName)
	assert.Equal(t, "ALICE/my_pat", resource.Id.Resource)
	assert.Equal(t, programmaticAccessTokenResourceType.Id, resource.Id.ResourceType)
}

func TestPatResource_ZeroTimesOmitted(t *testing.T) {
	ctx := context.Background()

	pat := &snowflake.ProgrammaticAccessToken{
		Name:     "no_ts_pat",
		UserName: "BOB",
		Status:   "ACTIVE",
		// CreatedOn and ExpiresAt are zero — should not be emitted
	}

	parentID, err := rs.NewResourceID(userResourceType, "BOB")
	require.NoError(t, err)

	resource, err := patResource(ctx, pat, parentID)
	require.NoError(t, err)
	require.NotNil(t, resource)
	assert.Equal(t, "no_ts_pat", resource.DisplayName)
}

func TestPatBuilder_ParentTypeMismatch(t *testing.T) {
	ctx := context.Background()
	builder := newPATBuilder(nil)

	wrongParent, err := rs.NewResourceID(databaseResourceType, "MY_DB")
	require.NoError(t, err)

	_, _, err = builder.List(ctx, wrongParent, rs.SyncOpAttrs{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid parent resource type")
}

func TestPatBuilder_NilParent(t *testing.T) {
	ctx := context.Background()
	builder := newPATBuilder(nil)

	resources, results, err := builder.List(ctx, nil, rs.SyncOpAttrs{})
	require.NoError(t, err)
	assert.Nil(t, resources)
	assert.Nil(t, results)
}

func TestPATResourceType(t *testing.T) {
	assert.Equal(t, "programmatic_access_token", programmaticAccessTokenResourceType.Id)
	assert.Equal(t, "Programmatic Access Token", programmaticAccessTokenResourceType.DisplayName)
}
