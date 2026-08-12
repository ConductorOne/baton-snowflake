package connector

import (
	"testing"

	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

func TestMissingLoginPrivilegeErr(t *testing.T) {
	tests := []struct {
		name    string
		logins  []string
		wantErr bool
	}{
		{name: "login_name populated", logins: []string{"alice"}, wantErr: false},
		{name: "login_name blank", logins: []string{""}, wantErr: true},
		{name: "login_name whitespace-only", logins: []string{"   "}, wantErr: true},
		{name: "one of many populated", logins: []string{"", "bob", ""}, wantErr: false},
		{name: "all blank", logins: []string{"", "  ", ""}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			users := make([]snowflake.User, len(tt.logins))
			for i, login := range tt.logins {
				users[i] = snowflake.User{Login: login}
			}
			err := missingLoginPrivilegeErr(users)
			if tt.wantErr && err == nil {
				t.Fatal("missingLoginPrivilegeErr() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("missingLoginPrivilegeErr() = %v, want nil", err)
			}
		})
	}
}
