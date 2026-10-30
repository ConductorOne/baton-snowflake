package connector

import (
	"context"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRsaResource(t *testing.T) {
	setTime := time.Date(2025, 4, 9, 10, 0, 0, 0, time.UTC)
	usedTime := time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC)
	userID := v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "ALICE"}.Build()
	keyPair := func(fp string, lastUsed *time.Time) snowflake.Credential {
		return snowflake.Credential{UserName: "ALICE", Type: snowflake.CredentialTypeKeyPair, PublicKeyFp: fp, LastUsedOn: lastUsed}
	}

	tests := []struct {
		name         string
		user         snowflake.UserRsa
		idx          RsaIndex
		credentials  []snowflake.Credential
		wantResource bool
		wantCreated  *time.Time
		wantLastUsed *time.Time
	}{
		{
			name:         "slot 1 set time is created at, not last used",
			user:         snowflake.UserRsa{Username: "ALICE", RsaPublicKeyLastSetTime: &setTime},
			idx:          RsaIndex1,
			wantResource: true,
			wantCreated:  &setTime,
		},
		{
			name:         "slot 2 set time is created at, not last used",
			user:         snowflake.UserRsa{Username: "ALICE", RsaPublicKeyLastSetTime2: &setTime},
			idx:          RsaIndex2,
			wantResource: true,
			wantCreated:  &setTime,
		},
		{
			name: "no set time and no fingerprint emits nothing",
			user: snowflake.UserRsa{Username: "ALICE"},
			idx:  RsaIndex1,
		},
		{
			name:         "fingerprint without set time still emits the key",
			user:         snowflake.UserRsa{Username: "ALICE", RsaPublicKeyFp: "SHA256:abc="},
			idx:          RsaIndex1,
			wantResource: true,
		},
		{
			name:         "last use comes from credentials, not the rotation date",
			user:         snowflake.UserRsa{Username: "ALICE", RsaPublicKeyLastSetTime: &setTime, RsaPublicKeyFp: "SHA256:abc="},
			idx:          RsaIndex1,
			credentials:  []snowflake.Credential{keyPair("abc=", &usedTime)},
			wantResource: true,
			wantCreated:  &setTime,
			wantLastUsed: &usedTime,
		},
		{
			name:         "fingerprint matches with prefix on both sides",
			user:         snowflake.UserRsa{Username: "ALICE", RsaPublicKeyLastSetTime2: &setTime, RsaPublicKeyFp2: "SHA256:abc="},
			idx:          RsaIndex2,
			credentials:  []snowflake.Credential{keyPair("sha256:abc=", &usedTime)},
			wantResource: true,
			wantCreated:  &setTime,
			wantLastUsed: &usedTime,
		},
		{
			name:         "fingerprint comparison is case-sensitive",
			user:         snowflake.UserRsa{Username: "ALICE", RsaPublicKeyLastSetTime: &setTime, RsaPublicKeyFp: "SHA256:abc="},
			idx:          RsaIndex1,
			credentials:  []snowflake.Credential{keyPair("ABC=", &usedTime)},
			wantResource: true,
			wantCreated:  &setTime,
		},
		{
			name:         "matching row with null last use leaves it unset",
			user:         snowflake.UserRsa{Username: "ALICE", RsaPublicKeyLastSetTime: &setTime, RsaPublicKeyFp: "SHA256:abc="},
			idx:          RsaIndex1,
			credentials:  []snowflake.Credential{keyPair("abc=", nil)},
			wantResource: true,
			wantCreated:  &setTime,
		},
		{
			name:         "PAT rows never match a key slot",
			user:         snowflake.UserRsa{Username: "ALICE", RsaPublicKeyLastSetTime: &setTime, RsaPublicKeyFp: "SHA256:abc="},
			idx:          RsaIndex1,
			credentials:  []snowflake.Credential{{UserName: "ALICE", Type: snowflake.CredentialTypePAT, PublicKeyFp: "abc=", LastUsedOn: &usedTime}},
			wantResource: true,
			wantCreated:  &setTime,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resource, err := rsaResource(context.Background(), &tt.user, tt.idx, userID, tt.credentials)
			require.NoError(t, err)
			if !tt.wantResource {
				assert.Nil(t, resource)
				return
			}
			require.NotNil(t, resource)
			assert.Equal(t, "ALICE-"+tt.idx.String(), resource.GetId().GetResource())

			trait := secretTrait(t, resource)
			assertTimestamp(t, tt.wantCreated, resource.GetCreatedAt().AsTime(), resource.HasCreatedAt())
			assertTimestamp(t, tt.wantLastUsed, trait.GetLastUsedAt().AsTime(), trait.HasLastUsedAt())
		})
	}
}

func TestRsaResourceInvalidIndex(t *testing.T) {
	userID := v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "ALICE"}.Build()
	_, err := rsaResource(context.Background(), &snowflake.UserRsa{Username: "ALICE"}, RsaIndex(3), userID, nil)
	require.Error(t, err)
}

func TestRsaListEmitsKeysWhenCredentialsAreDenied(t *testing.T) {
	server := serveSecretsMock(t, secretsMock{denyCredentials: true})
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	require.NoError(t, err)

	userID := v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "ALICE"}.Build()
	resources, _, err := newRsaBuilder(client).List(context.Background(), userID, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Len(t, resources, 2)
	for _, resource := range resources {
		assert.True(t, resource.HasCreatedAt())
		assert.False(t, secretTrait(t, resource).HasLastUsedAt())
	}
}

func TestRsaListSetsLastUsedFromCredentials(t *testing.T) {
	server := serveSecretsMock(t, secretsMock{})
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	require.NoError(t, err)

	userID := v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "ALICE"}.Build()
	resources, _, err := newRsaBuilder(client).List(context.Background(), userID, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Len(t, resources, 2)

	trait := secretTrait(t, resources[0])
	require.True(t, trait.HasLastUsedAt())
	assert.Equal(t, time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), trait.GetLastUsedAt().AsTime())

	trait = secretTrait(t, resources[1])
	assert.False(t, trait.HasLastUsedAt())
}

func assertTimestamp(t *testing.T, want *time.Time, got time.Time, present bool) {
	t.Helper()
	if want == nil {
		assert.False(t, present)
		return
	}
	require.True(t, present)
	assert.True(t, want.Equal(got), "got %s, want %s", got, want)
}
