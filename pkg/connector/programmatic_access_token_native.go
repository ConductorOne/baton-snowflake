package connector

import (
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/secretcontracts"
)

// encodeNativeProgrammaticAccessToken prepares the api_key_v2 payload through
// the SDK contract helper, which validates it and returns the content type with
// the bytes. The API_KEY issuance arm selects this representation, while sharing
// the existing PAT inventory and revocation path with the raw TOKEN arm.
// Snowflake permits PAT renames, so the two forms cannot be split into resource
// types by name.
func encodeNativeProgrammaticAccessToken(token string) (secretcontracts.Payload, error) {
	if token == "" {
		return secretcontracts.Payload{}, fmt.Errorf("baton-snowflake: empty programmatic access token")
	}
	payload, err := secretcontracts.APIKeyV2{KeyValue: token, Provider: "snowflake", HeaderName: "Authorization"}.Encode()
	if err != nil {
		return secretcontracts.Payload{}, fmt.Errorf("baton-snowflake: encode programmatic access token: %w", err)
	}
	return payload, nil
}
