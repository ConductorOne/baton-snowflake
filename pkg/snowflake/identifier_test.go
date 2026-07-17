package snowflake

import "testing"

func TestUnquoteSnowflakeIdentifier(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "already unquoted uppercase system role is unaffected",
			in:   "SYSADMIN",
			want: "SYSADMIN",
		},
		{
			name: "already unquoted simple name is unaffected",
			in:   "alice",
			want: "alice",
		},
		{
			name: "quoted mixed-case name with spaces is unquoted",
			in:   `"Data Engineer"`,
			want: "Data Engineer",
		},
		{
			name: "quoted name with embedded escaped double quote",
			in:   `"He said ""hi"""`,
			want: `He said "hi"`,
		},
		{
			name: "quoted name with unicode",
			in:   `"Ingénieur Données"`,
			want: "Ingénieur Données",
		},
		{
			name: "empty string is unaffected",
			in:   "",
			want: "",
		},
		{
			name: "single quote character is not treated as a quoted pair",
			in:   `"`,
			want: `"`,
		},
		{
			name: "case is never altered",
			in:   `"MixedCase"`,
			want: "MixedCase",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := unquoteSnowflakeIdentifier(tt.in)
			if got != tt.want {
				t.Errorf("unquoteSnowflakeIdentifier(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
