package connector

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/secretcontracts"
)

func TestEncodeNativeProgrammaticAccessToken(t *testing.T) {
	const input = "Snowflake.PAT-<>&\"\\\n\x01"
	payload, err := encodeNativeProgrammaticAccessToken(input)
	if err != nil {
		t.Fatalf("encodeNativeProgrammaticAccessToken() error = %v", err)
	}
	if payload.ContentType != "api_key_v2" {
		t.Fatalf("content type = %q, want the literal api_key_v2", payload.ContentType)
	}
	if secretcontracts.TypeAPIKeyV2 != "api_key_v2" {
		t.Fatalf("exported type ID = %q", secretcontracts.TypeAPIKeyV2)
	}
	var fields map[string]any
	if err := json.Unmarshal(payload.Bytes, &fields); err != nil {
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
	payload, err := encodeNativeProgrammaticAccessToken("")
	if err == nil {
		t.Fatal("empty token was encoded")
	}
	if payload.ContentType != "" || payload.Bytes != nil {
		t.Fatalf("empty token returned a payload: %#v", payload)
	}
}
