package snowflake

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"golang.org/x/oauth2/jwt"
)

type JWTConfig struct {
	AccountIdentifier    string
	UserIdentifier       string
	PublicKeyFingerPrint string
	PrivateKeyValue      []byte
}

func (c *JWTConfig) ParseFromGOB(input []byte) error {
	buf := bytes.NewBuffer(input)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(c)

	return err
}

func (c *JWTConfig) SerializeToGOB() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(c)

	return buf.Bytes(), err
}

func (c *JWTConfig) GetIssuer() string {
	return fmt.Sprintf("%s.%s.SHA256:%s", c.AccountIdentifier, c.UserIdentifier, c.PublicKeyFingerPrint)
}

func (c *JWTConfig) GetSubject() string {
	return fmt.Sprintf("%s.%s", c.AccountIdentifier, c.UserIdentifier)
}

func CreateJWTConfigFn() uhttp.CreateJWTConfig {
	return func(creadentials []byte, scopes ...string) (*jwt.Config, error) {
		cfg := &JWTConfig{}
		err := cfg.ParseFromGOB(creadentials)
		if err != nil {
			return nil, err
		}

		return &jwt.Config{
			Email:      cfg.GetIssuer(),
			PrivateKey: cfg.PrivateKeyValue,
			Subject:    cfg.GetSubject(),
			Expires:    time.Hour,
		}, nil
	}
}
