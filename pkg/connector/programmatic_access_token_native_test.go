package connector

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestEncodeNativeProgrammaticAccessToken(t *testing.T) {
	const token = "Snowflake.PAT-\"\\\n"
	encoded, err := encodeNativeProgrammaticAccessToken(token)
	if err != nil {
		t.Fatalf("encodeNativeProgrammaticAccessToken() error = %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("native payload is not JSON: %v", err)
	}
	want := map[string]any{
		"key_value":   token,
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
