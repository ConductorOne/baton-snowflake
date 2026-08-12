package connector

import (
	"testing"

	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

func TestMissingLoginPrivilegeErr(t *testing.T) {
	tests := []struct {
		name    string
		login   string
		wantErr bool
	}{
		{name: "login_name populated", login: "alice", wantErr: false},
		{name: "login_name blank", login: "", wantErr: true},
		{name: "login_name whitespace-only", login: "   ", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := missingLoginPrivilegeErr(snowflake.User{Login: tt.login})
			if tt.wantErr && err == nil {
				t.Fatal("missingLoginPrivilegeErr() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("missingLoginPrivilegeErr() = %v, want nil", err)
			}
		})
	}
}
