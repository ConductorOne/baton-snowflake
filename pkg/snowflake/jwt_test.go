package snowflake

import (
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func newTestJWTConfig(t *testing.T) *JWTConfig {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return &JWTConfig{
		AccountIdentifier: "test-account",
		UserIdentifier:    "test-user",
		PrivateKeyValue:   privateKey,
	}
}

func TestJWTTokenSource_Token_ReturnsValidToken(t *testing.T) {
	source := &JWTTokenSource{config: newTestJWTConfig(t)}

	token, err := source.Token()
	require.NoError(t, err)

	assert.NotEmpty(t, token.AccessToken)
	assert.Equal(t, "Bearer", token.TokenType)
	assert.True(t, token.Expiry.After(time.Now()))
}

func TestJWTTokenSource_Token_RefreshesExpiredToken(t *testing.T) {
	inner := &JWTTokenSource{config: newTestJWTConfig(t)}

	// Seed ReuseTokenSource with an already-expired token.
	expired := &oauth2.Token{
		AccessToken: "expired-token",
		Expiry:      time.Now().Add(-time.Minute),
	}
	ts := oauth2.ReuseTokenSource(expired, inner)

	token, err := ts.Token()
	require.NoError(t, err)

	assert.NotEqual(t, "expired-token", token.AccessToken, "expected a new token to be generated")
	assert.True(t, token.Valid())
}

func TestJWTTokenSource_Token_ReusesCachedToken(t *testing.T) {
	ts := NewJWTTokenSource(newTestJWTConfig(t))

	first, err := ts.Token()
	require.NoError(t, err)

	second, err := ts.Token()
	require.NoError(t, err)

	assert.Equal(t, first.AccessToken, second.AccessToken, "expected the cached token to be reused")
}
