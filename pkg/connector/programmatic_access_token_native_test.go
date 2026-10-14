package connector

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestEncodeNativeProgrammaticAccessToken(t *testing.T) {
	const input = "Snowflake.PAT-<>&\"\\\n\x01"
	encoded, err := encodeNativeProgrammaticAccessToken(input)
	if err != nil {
		t.Fatalf("encodeNativeProgrammaticAccessToken() error = %v", err)
	}
	const wantBytes = "{\"key_value\":\"Snowflake.PAT-<>&\\\"\\\\\\n\\u0001\",\"provider\":\"snowflake\",\"header_name\":\"Authorization\"}"
	if string(encoded) != wantBytes {
		t.Fatalf("native payload bytes = %q, want %q", encoded, wantBytes)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("native payload is not JSON: %v", err)
	}
	want := map[string]any{
		"key_value":   input,
		"provider":    "snowflake",
		"header_name": "Authorization",
	}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("native payload = %#v, want %#v", fields, want)
	}
}

func TestEncodeNativeProgrammaticAccessTokenRejectsEmptyToken(t *testing.T) {
	if _, err := encodeNativeProgrammaticAccessToken(""); err == nil {
		t.Fatal("empty token was encoded")
	}
}
