package connector

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCredentialIssuingUserBuilderIssueNamedKeyPair(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	identityID, err := rs.NewResourceID(userResourceType, "svc_automation")
	require.NoError(t, err)

	var registeredUser, registeredName, registeredPublicKey string
	var registeredDays int
	builder := &credentialIssuingUserBuilder{
		userBuilder: &userBuilder{},
		getUser: func(context.Context, string) (*snowflake.User, error) {
			return &snowflake.User{Username: "svc_automation", Type: "SERVICE"}, nil
		},
		addKeyPair: func(_ context.Context, user, name, publicKey string, days int) error {
			registeredUser = user
			registeredName = name
			registeredPublicKey = publicKey
			registeredDays = days
			return nil
		},
		newKeyName: func() string { return "c1_test_key" },
		now:        func() time.Time { return now },
	}
	bits := uint32(2048)
	keypair := v2.CredentialIssueOptions_Keypair_builder{
		Profile: v2.KeyGenerationProfile_builder{Kty: "RSA", RsaModulusBits: &bits}.Build(),
	}.Build()
	options := v2.CredentialIssueOptions_builder{Keypair: keypair}.Build()

	output, err := builder.Issue(ctx, &connectorbuilder.CredentialIssueInput{
		IdentityID:        identityID,
		CredentialOptions: options,
		ExpiresAt:         timestamppb.New(now.Add(90 * 24 * time.Hour)),
	})
	require.NoError(t, err)
	secret, plaintext := output.Secret, output.PlaintextData
	require.Equal(t, "svc_automation", registeredUser)
	require.Equal(t, "c1_test_key", registeredName)
	require.Equal(t, 90, registeredDays)
	require.Equal(t, "svc_automation:c1_test_key", secret.GetId().GetResource())
	require.Len(t, plaintext, 1)
	require.Equal(t, "private_key.pem", plaintext[0].GetName())

	publicDER, err := base64.StdEncoding.DecodeString(registeredPublicKey)
	require.NoError(t, err)
	publicValue, err := x509.ParsePKIXPublicKey(publicDER)
	require.NoError(t, err)
	publicKey, ok := publicValue.(*rsa.PublicKey)
	require.True(t, ok)
	require.Equal(t, 2048, publicKey.N.BitLen())

	privateBlock, _ := pem.Decode(plaintext[0].GetBytes())
	require.NotNil(t, privateBlock)
	privateValue, err := x509.ParsePKCS8PrivateKey(privateBlock.Bytes)
	require.NoError(t, err)
	privateKey, ok := privateValue.(*rsa.PrivateKey)
	require.True(t, ok)
	require.Equal(t, publicKey.N, privateKey.N)
}

func TestCredentialIssuingUserBuilderRejectsHumanUser(t *testing.T) {
	identityID, err := rs.NewResourceID(userResourceType, "alice")
	require.NoError(t, err)
	builder := &credentialIssuingUserBuilder{
		userBuilder: &userBuilder{},
		getUser: func(context.Context, string) (*snowflake.User, error) {
			return &snowflake.User{Username: "alice", Type: "PERSON"}, nil
		},
	}
	options := v2.CredentialIssueOptions_builder{Keypair: v2.CredentialIssueOptions_Keypair_builder{}.Build()}.Build()

	_, err = builder.Issue(context.Background(), &connectorbuilder.CredentialIssueInput{IdentityID: identityID, CredentialOptions: options})
	require.ErrorContains(t, err, "only be issued for service users")
}

func TestCredentialIssuingUserBuilderRejectsPastExpiry(t *testing.T) {
	identityID, err := rs.NewResourceID(userResourceType, "svc_automation")
	require.NoError(t, err)
	builder := &credentialIssuingUserBuilder{
		userBuilder: &userBuilder{},
		getUser: func(context.Context, string) (*snowflake.User, error) {
			return &snowflake.User{Username: "svc_automation", Type: "SERVICE"}, nil
		},
		now: time.Now,
	}
	bits := uint32(2048)
	keypair := v2.CredentialIssueOptions_Keypair_builder{
		Profile: v2.KeyGenerationProfile_builder{Kty: "RSA", RsaModulusBits: &bits}.Build(),
	}.Build()
	options := v2.CredentialIssueOptions_builder{Keypair: keypair}.Build()

	_, err = builder.Issue(context.Background(), &connectorbuilder.CredentialIssueInput{
		IdentityID:        identityID,
		CredentialOptions: options,
		ExpiresAt:         timestamppb.New(time.Now().Add(-time.Hour)),
	})
	require.ErrorContains(t, err, "must be in the future")
}

func TestNamedKeyPairBuilderDelete(t *testing.T) {
	var gotUser, gotKey string
	builder := &namedKeyPairBuilder{removeKeyPair: func(_ context.Context, user, key string) error {
		gotUser, gotKey = user, key
		return nil
	}}
	resourceID, err := rs.NewResourceID(namedKeyPairResourceType, "svc:ops:c1_key")
	require.NoError(t, err)
	parentID, err := rs.NewResourceID(userResourceType, "svc:ops")
	require.NoError(t, err)

	_, err = builder.Delete(context.Background(), resourceID, parentID)
	require.NoError(t, err)
	require.Equal(t, "svc:ops", gotUser)
	require.Equal(t, "c1_key", gotKey)
}

func TestNamedKeyPairBuilderDeleteRejectsMismatchedParent(t *testing.T) {
	builder := &namedKeyPairBuilder{removeKeyPair: func(context.Context, string, string) error {
		t.Fatal("remove must not be called")
		return nil
	}}
	resourceID, err := rs.NewResourceID(namedKeyPairResourceType, "alice:c1_key")
	require.NoError(t, err)
	parentID, err := rs.NewResourceID(userResourceType, "bob")
	require.NoError(t, err)

	_, err = builder.Delete(context.Background(), resourceID, parentID)
	require.ErrorContains(t, err, "does not belong")
}
