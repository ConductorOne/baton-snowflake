package connector

import (
	"encoding/json"
	"fmt"
)

// encodeNativeProgrammaticAccessToken prepares the api_key_v2 JsonV1 payload.
// It does not choose an issuance option or resource type: Snowflake permits PAT
// renames, and SHOW USER PROGRAMMATIC ACCESS TOKENS has no durable token ID with
// which to distinguish native credentials from the existing raw-token resources.
func encodeNativeProgrammaticAccessToken(token string) ([]byte, error) {
	if token == "" {
		return nil, fmt.Errorf("baton-snowflake: empty programmatic access token")
	}
	return json.Marshal(struct {
		KeyValue   string `json:"key_value"`
		Provider   string `json:"provider"`
		HeaderName string `json:"header_name"`
	}{
		KeyValue:   token,
		Provider:   "snowflake",
		HeaderName: "Authorization",
	})
}
