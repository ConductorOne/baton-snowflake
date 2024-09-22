package snowflake

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt"
)

type JWTConfig struct {
	AccountIdentifier string
	UserIdentifier    string
	PrivateKeyValue   any
	publicKeyFP       string
}

func (c *JWTConfig) getIssuer() string {
	// Set up Snowflake account and username in uppercase
	return fmt.Sprintf("%s.%s.SHA256:%s", strings.ToUpper(c.AccountIdentifier), strings.ToUpper(c.UserIdentifier), c.publicKeyFP)
}

func (c *JWTConfig) getSubject() string {
	return fmt.Sprintf("%s.%s", strings.ToUpper(c.AccountIdentifier), strings.ToUpper(c.UserIdentifier))
}

func (c *JWTConfig) GenerateBearerToken() (string, error) {
	if c.publicKeyFP == "" {
		fp, err := publicKeyFingerprint(c.PrivateKeyValue)
		if err != nil {
			return "", fmt.Errorf("failed to generate public key fingerprint: %w", err)
		}
		c.publicKeyFP = fp
	}

	issuedAt := time.Now()
	expiresAt := issuedAt.Add(time.Minute * 60)
	claims := jwt.MapClaims{
		"iss": c.getIssuer(),
		"sub": c.getSubject(),
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

func ReadPrivateKey(path string) (any, error) {
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParsePrivateKey(key)
}

func ParsePrivateKey(key []byte) (any, error) {
	// Decode the PEM block
	block, _ := pem.Decode(key)
	if block == nil {
		return "", errors.New("failed to decode PEM block containing the key")
	}

	var privateKey interface{}
	var err error
	// Parse the private key based on its type
	switch block.Type {
	case "RSA PRIVATE KEY":
		privateKey, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return "", fmt.Errorf("failed to parse PKCS1 private key: %v", err)
		}
	case "EC PRIVATE KEY":
		privateKey, err = x509.ParseECPrivateKey(block.Bytes)
		if err != nil {
			return "", fmt.Errorf("failed to parse EC private key: %v", err)
		}
	case "PRIVATE KEY":
		privateKey, err = x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return "", fmt.Errorf("failed to parse PKCS8 private key: %v", err)
		}
	default:
		return "", fmt.Errorf("unsupported key type: %s", block.Type)
	}

	return privateKey, nil
}

func publicKeyFingerprint(privateKey interface{}) (string, error) {
	var pubKey interface{}

	// Extract the public key
	switch key := privateKey.(type) {
	case *rsa.PrivateKey:
		pubKey = key.Public()
	case *ecdsa.PrivateKey:
		pubKey = key.Public()
	default:
		return "", fmt.Errorf("unsupported private key type: %T", privateKey)
	}

	// Marshal the public key to DER-encoded PKIX format
	pubKeyBytes, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		return "", fmt.Errorf("failed to marshal public key: %v", err)
	}

	// Compute the SHA-256 hash of the public key
	hash := sha256.Sum256(pubKeyBytes)

	// Base64-encode the hash
	encodedHash := base64.StdEncoding.EncodeToString(hash[:])

	return encodedHash, nil
}
