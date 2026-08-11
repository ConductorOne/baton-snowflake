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

// IsInsufficientPrivileges reports whether err is a Snowflake access-control denial that the
// connector may skip (HTTP 422 with Snowflake code 003001, joined as ErrInsufficientPrivileges).
//
// This is the privilege-skip predicate. We do NOT skip every 422: compilation and other
// non-access-control 422s stay fatal. Prefer this over IsUnprocessableEntity wherever the client
// method has classified the response body.
func IsInsufficientPrivileges(err error) bool {
	return err != nil && errors.Is(err, ErrInsufficientPrivileges)
}

// IsUnprocessableEntity reports whether the call failed with HTTP 422, regardless of Snowflake's
// error code. It is a status-only helper for call sites that only have a raw statusCode (or a
// legacy string-matched error) and treat "unprocessable" as "not resolvable" — e.g. shared/system
// database quirks on GetDatabase.
//
// It is NOT the privilege-skip used by CXH-2193 paths. Those must call IsInsufficientPrivileges
// so a SQL-compilation 422 cannot be swallowed as invisible data.
func IsUnprocessableEntity(statusCode int, err error) bool {
	if statusCode == http.StatusUnprocessableEntity {
		return true
	}
	return IsInsufficientPrivileges(err) || IsUnprocessableEntityError(err)
}

// IsUnprocessableEntityError reports whether err's message still contains the literal HTTP status
// line "422 Unprocessable Entity". Prefer IsInsufficientPrivileges: after dedupeAPIError, most
// Snowflake client methods keep only the WithErrorResponse detail ("Request failed with status
// 422: …"), so this string match is a weak fallback for call sites that have not yet joined the
// sentinel.
func IsUnprocessableEntityError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "422 Unprocessable Entity")
}
