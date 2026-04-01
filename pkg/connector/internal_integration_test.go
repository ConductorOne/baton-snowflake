package connector

import (
	"context"
	"os"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	snowflake "github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
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
	if accountUrl == "" && accountIdentifier == "" &&
		userIdentifier == "" && publicKeyFingerPrint == "" &&
		privateKeyPath == "" {
		t.Skip()
	}
	cli, err := getCientForTesting(ctx)
	assert.Nil(t, err)

	u := &userBuilder{
		resourceType: userResourceType,
		client:       cli,
	}
	rv, results, err := u.List(ctx, &v2.ResourceId{}, rs.SyncOpAttrs{PageToken: pagination.Token{}})
	assert.Nil(t, err)
	assert.NotNil(t, rv)
	assert.NotNil(t, results)
}

func getCientForTesting(ctx context.Context) (*snowflake.Client, error) {
	privateKeyValue, err := snowflake.ReadPrivateKey(privateKeyPath)
	if err != nil {
		return nil, err
	}

	jwtConfig := snowflake.JWTConfig{
		AccountIdentifier: accountIdentifier,
		UserIdentifier:    userIdentifier,
		PrivateKeyValue:   privateKeyValue,
	}

	noAuth := uhttp.NoAuth{}
	baseHttpClient, err := noAuth.GetClient(ctx)
	if err != nil {
		return nil, err
	}

	ts := snowflake.NewJWTTokenSource(&jwtConfig)
	ctx = context.WithValue(ctx, oauth2.HTTPClient, baseHttpClient)
	httpClient := oauth2.NewClient(ctx, ts)

	return snowflake.New(accountUrl, jwtConfig, httpClient)
}
