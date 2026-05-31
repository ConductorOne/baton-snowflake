package connector

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestClassifyIntegration(t *testing.T) {
	tests := []struct {
		name            string
		integrationType string
		category        string
		wantType        v2.NonHumanIdentityTrait_NhiType
		wantDetail      string
	}{
		{
			name:            "external oauth security integration -> app registration",
			integrationType: "EXTERNAL_OAUTH - AZURE",
			category:        "SECURITY",
			wantType:        v2.NonHumanIdentityTrait_NHI_TYPE_APP_REGISTRATION,
			wantDetail:      "snowflake.integration.external_oauth",
		},
		{
			name:            "storage integration -> assumable role",
			integrationType: "EXTERNAL_STAGE",
			category:        "STORAGE",
			wantType:        v2.NonHumanIdentityTrait_NHI_TYPE_ASSUMABLE_ROLE,
			wantDetail:      "snowflake.integration.storage",
		},
		{
			name:            "api integration -> assumable role",
			integrationType: "API",
			category:        "API",
			wantType:        v2.NonHumanIdentityTrait_NHI_TYPE_ASSUMABLE_ROLE,
			wantDetail:      "snowflake.integration.api",
		},
		{
			name:            "non-external oauth security integration -> unspecified spine, governed detail",
			integrationType: "OAUTH - SNOWFLAKE",
			category:        "SECURITY",
			wantType:        v2.NonHumanIdentityTrait_NHI_TYPE_UNSPECIFIED,
			wantDetail:      "snowflake.integration.security",
		},
		{
			name:            "external access integration -> unspecified spine, governed detail",
			integrationType: "EXTERNAL_ACCESS",
			category:        "EXTERNAL ACCESS",
			wantType:        v2.NonHumanIdentityTrait_NHI_TYPE_UNSPECIFIED,
			wantDetail:      "snowflake.integration.external_access",
		},
		{
			name:            "notification integration -> unspecified spine, governed detail",
			integrationType: "QUEUE",
			category:        "NOTIFICATION",
			wantType:        v2.NonHumanIdentityTrait_NHI_TYPE_UNSPECIFIED,
			wantDetail:      "snowflake.integration.notification",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotDetail := classifyIntegration(tt.integrationType, tt.category)
			if gotType != tt.wantType {
				t.Errorf("classifyIntegration() type = %v, want %v", gotType, tt.wantType)
			}
			if gotDetail != tt.wantDetail {
				t.Errorf("classifyIntegration() detail = %q, want %q", gotDetail, tt.wantDetail)
			}
		})
	}
}

func TestNormalizeDetailToken(t *testing.T) {
	tests := map[string]string{
		"SECURITY":        "security",
		"EXTERNAL ACCESS": "external_access",
		"  API  ":         "api",
		"EXTERNAL_OAUTH":  "external_oauth",
		"":                "",
	}
	for in, want := range tests {
		if got := normalizeDetailToken(in); got != want {
			t.Errorf("normalizeDetailToken(%q) = %q, want %q", in, got, want)
		}
	}
}
