package connector

import (
	"context"
	"os"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	snowflake "github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/assert"
)

var (
	accountUrl           = os.Getenv("BATON_ACCOUNT_URL")
	accountIdentifier    = os.Getenv("BATON_ACCOUNT_IDENTIFIER")
	userIdentifier       = os.Getenv("BATON_USER_IDENTIFIER")
	publicKeyFingerPrint = os.Getenv("BATON_PUBLIC_KEY_FINGERPRINT")
	privateKeyPath       = os.Getenv("BATON_PRIVATE_KEY_PATH")
	ctx                  = context.Background()
)

func TestUserBuilderList(t *testing.T) {
	cli, err := getCientForTesting(ctx)
	assert.Nil(t, err)

	u := &userBuilder{
		resourceType: userResourceType,
		client:       cli,
	}
	rv, _, _, err := u.List(ctx, &v2.ResourceId{}, &pagination.Token{})
	assert.Nil(t, err)
	assert.NotNil(t, rv)
}

func getCientForTesting(ctx context.Context) (*snowflake.Client, error) {
	var jwtConfig = snowflake.JWTConfig{
		AccountIdentifier:    accountIdentifier,
		UserIdentifier:       userIdentifier,
		PublicKeyFingerPrint: publicKeyFingerPrint,
	}
	privateKeyValue, err := snowflake.ReadPrivateKey(privateKeyPath)
	if err != nil {
		return nil, err
	}

	jwtConfig.PrivateKeyValue = privateKeyValue
	token, err := jwtConfig.GenerateBearerToken()
	if err != nil {
		return nil, err
	}

	httpClient, err := uhttp.NewBearerAuth(token).GetClient(ctx)
	if err != nil {
		return nil, err
	}

	client, err := snowflake.New(accountUrl, jwtConfig, httpClient)
	if err != nil {
		return client, err
	}

	return client, nil
}
