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
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
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
	keypair := &v2.LocalCredentialOptions_Keypair{}
	keypair.SetAlgorithm("RSA")
	keypair.SetBits(2048)
	keypair.SetTtl(durationpb.New(90 * 24 * time.Hour))
	options := &v2.LocalCredentialOptions{}
	options.SetKeypair(keypair)

	secret, plaintext, _, err := builder.Issue(ctx, identityID, options)
	require.NoError(t, err)
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
	require.Equal(t, publicKey.N, privateKey.PublicKey.N)
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
	options := &v2.LocalCredentialOptions{}
	options.SetKeypair(&v2.LocalCredentialOptions_Keypair{})

	_, _, _, err = builder.Issue(context.Background(), identityID, options)
	require.ErrorContains(t, err, "only be issued for service users")
}

func TestCredentialIssuingUserBuilderRejectsFractionalDayTTL(t *testing.T) {
	identityID, err := rs.NewResourceID(userResourceType, "svc_automation")
	require.NoError(t, err)
	builder := &credentialIssuingUserBuilder{
		userBuilder: &userBuilder{},
		getUser: func(context.Context, string) (*snowflake.User, error) {
			return &snowflake.User{Username: "svc_automation", Type: "SERVICE"}, nil
		},
	}
	keypair := &v2.LocalCredentialOptions_Keypair{}
	keypair.SetTtl(durationpb.New(36 * time.Hour))
	options := &v2.LocalCredentialOptions{}
	options.SetKeypair(keypair)

	_, _, _, err = builder.Issue(context.Background(), identityID, options)
	require.ErrorContains(t, err, "positive whole number of days")
}
