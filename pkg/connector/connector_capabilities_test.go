package connector

import (
	"context"
	"slices"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/require"
)

type capabilityProvider interface {
	GetCapabilities(context.Context) (*v2.ConnectorCapabilities, error)
}

func connectorCapabilities(t *testing.T, syncSecrets bool) map[string]*v2.ResourceTypeCapability {
	t.Helper()
	server, err := connectorbuilder.NewConnector(context.Background(), &Connector{
		Client:      &snowflake.Client{},
		SyncSecrets: syncSecrets,
	})
	require.NoError(t, err)
	provider, ok := server.(capabilityProvider)
	require.True(t, ok)
	capabilities, err := provider.GetCapabilities(context.Background())
	require.NoError(t, err)

	byID := make(map[string]*v2.ResourceTypeCapability, len(capabilities.GetResourceTypeCapabilities()))
	for _, capability := range capabilities.GetResourceTypeCapabilities() {
		byID[capability.GetResourceType().GetId()] = capability
	}
	return byID
}

func TestNamedKeyPairCapabilitiesAreOptInAndLifecycleComplete(t *testing.T) {
	capabilities := connectorCapabilities(t, true)
	namedKeyPair, ok := capabilities[namedKeyPairResourceType.Id]
	require.True(t, ok)
	require.True(t, namedKeyPair.GetOptInRequired())
	require.True(t, slices.Contains(namedKeyPair.GetCapabilities(), v2.Capability_CAPABILITY_SYNC))
	require.True(t, slices.Contains(namedKeyPair.GetCapabilities(), v2.Capability_CAPABILITY_RESOURCE_DELETE))
	require.False(t, slices.Contains(namedKeyPair.GetCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ISSUE))
	annos := annotations.Annotations(namedKeyPair.GetResourceType().GetAnnotations())
	require.True(t, annos.Contains(&v2.SkipEntitlementsAndGrants{}))

	user := capabilities[userResourceType.Id]
	require.NotNil(t, user)
	require.True(t, slices.Contains(user.GetCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ISSUE))
	require.NotNil(t, user.GetCredentialIssue())
	require.Len(t, user.GetCredentialIssue().GetOptions(), 1)
	require.Equal(t, namedKeyPairResourceType.Id, user.GetCredentialIssue().GetOptions()[0].GetSecretResourceTypeId())
	require.Equal(t, v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE, user.GetCredentialIssue().GetOptions()[0].GetResourceMode())
}

func TestNamedKeyPairCapabilitiesAreAbsentWithoutSecretSync(t *testing.T) {
	capabilities := connectorCapabilities(t, false)
	_, ok := capabilities[namedKeyPairResourceType.Id]
	require.False(t, ok)

	user := capabilities[userResourceType.Id]
	require.NotNil(t, user)
	require.False(t, slices.Contains(user.GetCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ISSUE))
	require.Nil(t, user.GetCredentialIssue())
}
