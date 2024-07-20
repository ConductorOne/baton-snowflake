package snowflake

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"strings"
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
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	return tokenString, nil
}

func (c *JWTConfig) GenerateBearerTokenRsaKey(keyPath string) (string, error) {
	// Set up Snowflake account and username in uppercase
	account := c.AccountIdentifier
	user := c.UserIdentifier
	qualifiedUsername := fmt.Sprintf("%s.%s", strings.ToUpper(account), strings.ToUpper(user))

	// Use the fingerprint obtained from the 'DESCRIBE USER' command in Snowflake
	fingerprintFromDescribeUser := c.PublicKeyFingerPrint

	// Get current time in UTC and set JWT lifetime to 59 minutes
	now := time.Now().UTC()
	lifetime := time.Minute * 59

	// Construct JWT payload with issuer, subject, issue time, and expiration time
	payload := jwt.MapClaims{
		"iss": fmt.Sprintf("%s.%s", qualifiedUsername, fingerprintFromDescribeUser),
		"sub": qualifiedUsername,
		"iat": now.Unix(),
		"exp": now.Add(lifetime).Unix(),
	}

	// Specify the encoding algorithm for JWT
	encodingAlgorithm := jwt.SigningMethodRS256

	// Read private key from file and encode payload into JWT
	pemData, err := os.ReadFile(keyPath)
	if err != nil {
		return "", err
	}

	block, _ := pem.Decode(pemData)
	if block == nil {
		return "", fmt.Errorf("failed to parse PEM block containing the private key")
	}

	privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return "", err
	}

	token := jwt.NewWithClaims(encodingAlgorithm, payload)
	signedToken, err := token.SignedString(privateKey)
	if err != nil {
		return "", err
	}

	return signedToken, nil
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
