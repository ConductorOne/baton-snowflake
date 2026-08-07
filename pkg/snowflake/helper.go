package snowflake

import (
	"errors"
	"net/http"
	"strings"
)

// ErrInsufficientPrivileges marks a Snowflake HTTP 422 that means "the connector role cannot see
// this object" rather than a genuine failure. Snowflake answers SQL access-control denials
// (error code 003001) with 422 instead of 403, and does so for objects a lesser role simply is
// not entitled to observe - SHOW GRANTS OF ROLE on a system role, SHOW SCHEMAS on a database the
// role has no USAGE on, and so on.
//
// Client methods join this sentinel into the error they return so callers can recognise the
// condition with errors.Is even after the error crosses package boundaries. Errors that carry it
// are safe to treat as "nothing visible here"; every other error is a real failure.
var ErrInsufficientPrivileges = errors.New("baton-snowflake: insufficient privileges")

// sqlAccessControlErrorCode is Snowflake's error code for "SQL access control error: Insufficient
// privileges". It is what separates a benign 422 from a fatal one: Snowflake also answers 422 for
// SQL compilation errors, which mean the connector sent a malformed statement. Those must keep
// failing the sync loudly instead of being skipped as invisible data.
const sqlAccessControlErrorCode = "003001"

// isAccessControlDenial reports whether a response is the 422 that means "this role may not
// observe this object", rather than any other 422.
func isAccessControlDenial(resp *http.Response, apiErr *SnowflakeError) bool {
	return resp != nil &&
		resp.StatusCode == http.StatusUnprocessableEntity &&
		apiErr != nil &&
		apiErr.Code == sqlAccessControlErrorCode
}

// IsUnprocessableEntity reports whether the Snowflake API returned HTTP 422 (Unprocessable Entity).
// Snowflake returns 422 for certain operations on system/predefined objects (e.g. SHOW GRANTS OF ROLE for ACCOUNTADMIN,
// SHOW ROLES LIKE for some roles). Callers can treat this as "no data" or "not resolvable" instead of a hard error.
//
// Use this overload only where the client method hands back the raw HTTP status code; when all you
// have is the error, call IsInsufficientPrivileges or IsUnprocessableEntityError directly.
func IsUnprocessableEntity(statusCode int, err error) bool {
	if statusCode == http.StatusUnprocessableEntity {
		return true
	}
	return IsInsufficientPrivileges(err) || IsUnprocessableEntityError(err)
}

// IsInsufficientPrivileges reports whether err is a Snowflake access-control denial, i.e. one the
// caller may skip over.
//
// It matches on the sentinel alone, never on the HTTP status: the raw 422 is not sufficient
// evidence, because Snowflake also answers 422 for SQL compilation errors that must stay fatal.
// Only a client method that inspected the error code in the response body joins the sentinel.
func IsInsufficientPrivileges(err error) bool {
	return err != nil && errors.Is(err, ErrInsufficientPrivileges)
}

// IsUnprocessableEntityError reports whether err's message still contains the literal HTTP status
// line "422 Unprocessable Entity". Prefer IsInsufficientPrivileges: after dedupeAPIError, most
// Snowflake client methods keep only the WithErrorResponse detail ("Request failed with status
// 422: …"), so this string match is a weak fallback for call sites that have not yet joined the
// sentinel.
func IsUnprocessableEntityError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "422 Unprocessable Entity")
}
