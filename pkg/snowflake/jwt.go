package snowflake

import (
	"crypto/rsa"
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt"
)

type JWTConfig struct {
	AccountIdentifier    string
	UserIdentifier       string
	PublicKeyFingerPrint string
	PrivateKeyValue      *rsa.PrivateKey
}

func (c *JWTConfig) GetIssuer() string {
	return fmt.Sprintf("%s.%s.SHA256:%s", c.AccountIdentifier, c.UserIdentifier, c.PublicKeyFingerPrint)
}

func (c *JWTConfig) GetSubject() string {
	return fmt.Sprintf("%s.%s", c.AccountIdentifier, c.UserIdentifier)
}

func (c *JWTConfig) GenerateBearerToken() (string, error) {
	issuedAt := time.Now()
	expiresAt := issuedAt.Add(time.Minute * 60)
	claims := jwt.MapClaims{
		"iss": c.GetIssuer(),
		"sub": c.GetSubject(),
		"iat": issuedAt.Unix(),
		"exp": expiresAt.Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)

	tokenString, err := token.SignedString(c.PrivateKeyValue)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %v", err)
	}

	return tokenString, nil
}

func ReadPrivateKey(path string) (*rsa.PrivateKey, error) {
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(key)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}
