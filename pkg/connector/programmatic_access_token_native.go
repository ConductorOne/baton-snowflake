package connector

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// encodeNativeProgrammaticAccessToken prepares the api_key_v2 JsonV1 payload.
// The API_KEY issuance arm selects this representation, while sharing the
// existing PAT inventory and revocation path with the raw TOKEN arm. Snowflake
// permits PAT renames, so the two forms cannot be split into resource types by name.
func encodeNativeProgrammaticAccessToken(token string) ([]byte, error) {
	if token == "" {
		return nil, fmt.Errorf("baton-snowflake: empty programmatic access token")
	}
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(struct {
		KeyValue   string `json:"key_value"`
		Provider   string `json:"provider"`
		HeaderName string `json:"header_name"`
	}{
		KeyValue:   token,
		Provider:   "snowflake",
		HeaderName: "Authorization",
	})
	if err != nil {
		return nil, fmt.Errorf("baton-snowflake: encode programmatic access token: %w", err)
	}
	// Encoder appends one framing newline; Multipass JsonV1 does not.
	return bytes.TrimSuffix(output.Bytes(), []byte("\n")), nil
}
