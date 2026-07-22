package connector

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

func TestClassifyUserNHI(t *testing.T) {
	tests := []struct {
		name       string
		userType   string
		wantType   v2.NonHumanIdentityTrait_NhiType
		wantDetail string
		wantIsNHI  bool
	}{
		{
			name:       "service user -> app registration",
			userType:   "SERVICE",
			wantType:   v2.NonHumanIdentityTrait_NHI_TYPE_APP_REGISTRATION,
			wantDetail: "snowflake.user.service",
			wantIsNHI:  true,
		},
		{
			name:       "legacy service user -> app registration",
			userType:   "LEGACY_SERVICE",
			wantType:   v2.NonHumanIdentityTrait_NHI_TYPE_APP_REGISTRATION,
			wantDetail: "snowflake.user.legacy_service",
			wantIsNHI:  true,
		},
		{
			name:       "lowercase and padded service type still classified",
			userType:   "  service  ",
			wantType:   v2.NonHumanIdentityTrait_NHI_TYPE_APP_REGISTRATION,
			wantDetail: "snowflake.user.service",
			wantIsNHI:  true,
		},
		{
			name:      "person user -> no NHI trait",
			userType:  "PERSON",
			wantIsNHI: false,
		},
		{
			name:      "empty user type -> no NHI trait",
			userType:  "",
			wantIsNHI: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotDetail, gotIsNHI := classifyUserNHI(tt.userType)
			if gotIsNHI != tt.wantIsNHI {
				t.Fatalf("classifyUserNHI() isNHI = %v, want %v", gotIsNHI, tt.wantIsNHI)
			}
			if !tt.wantIsNHI {
				return
			}
			if gotType != tt.wantType {
				t.Errorf("classifyUserNHI() type = %v, want %v", gotType, tt.wantType)
			}
			if gotDetail != tt.wantDetail {
				t.Errorf("classifyUserNHI() detail = %q, want %q", gotDetail, tt.wantDetail)
			}
		})
	}
}

func TestUserResourceNHIAnnotation(t *testing.T) {
	ctx := context.Background()

	serviceUser := &snowflake.User{Username: "svc", Type: "SERVICE"}
	res, err := userResource(ctx, serviceUser, false)
	if err != nil {
		t.Fatalf("userResource() error = %v", err)
	}
	nhi, err := rs.GetNonHumanIdentityTrait(res)
	if err != nil {
		t.Fatalf("expected NHI trait on service user, got error = %v", err)
	}
	if nhi.GetNhiType() != v2.NonHumanIdentityTrait_NHI_TYPE_APP_REGISTRATION {
		t.Errorf("service user NHI type = %v, want APP_REGISTRATION", nhi.GetNhiType())
	}
	if nhi.GetNhiDetail() != "snowflake.user.service" {
		t.Errorf("service user NHI detail = %q, want %q", nhi.GetNhiDetail(), "snowflake.user.service")
	}

	personUser := &snowflake.User{Username: "alice", Type: "PERSON"}
	res, err = userResource(ctx, personUser, false)
	if err != nil {
		t.Fatalf("userResource() error = %v", err)
	}
	if _, err := rs.GetNonHumanIdentityTrait(res); err == nil {
		t.Errorf("expected no NHI trait on person user, but found one")
	}
}
