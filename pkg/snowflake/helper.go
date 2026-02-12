package snowflake

import (
	"net/http"
	"strings"
)

// IsUnprocessableEntity reports whether the Snowflake API returned HTTP 422 (Unprocessable Entity).
// Snowflake returns 422 for certain operations on system/predefined objects (e.g. SHOW GRANTS OF ROLE for ACCOUNTADMIN,
// SHOW ROLES LIKE for some roles). Callers can treat this as "no data" or "not resolvable" instead of a hard error.
func IsUnprocessableEntity(resp *http.Response, err error) bool {
	if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity {
		return true
	}
	if err != nil && (strings.Contains(err.Error(), "422") || strings.Contains(err.Error(), "Unprocessable Entity")) {
		return true
	}
	return false
}
