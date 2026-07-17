package snowflake

import "strings"

// unquoteSnowflakeIdentifier normalizes an identifier as rendered by Snowflake's
// SHOW GRANTS * commands. Snowflake renders identifiers that require quoting (mixed
// case, spaces, special characters) wrapped in double quotes, with any embedded
// double quote doubled (Snowflake source name `He said "hi"` -> `"He said ""hi"""`).
// SHOW ROLES/SHOW USERS/SHOW DATABASES list output, by contrast, always returns the
// bare canonical name and must never be passed through this function - doing so
// would change already-stable resource IDs.
//
// If s is not wrapped in double quotes, it is returned unchanged (defensive: some
// grantee names, e.g. default uppercase system roles, are already bare).
func unquoteSnowflakeIdentifier(s string) string {
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return s
	}
	inner := s[1 : len(s)-1]
	return strings.ReplaceAll(inner, `""`, `"`)
}
