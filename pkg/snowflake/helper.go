package snowflake

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"google.golang.org/grpc/codes"
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

// NormalizeNullValue maps Snowflake's textual rendering of a NULL cell to an empty string.
// The SQL API returns every column as text, so an unset property such as DEFAULT_ROLE arrives
// as the literal "null" rather than as "". Callers testing a string column for absence must go
// through this, or the absence test silently never matches.
func NormalizeNullValue(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == rowNull {
		return ""
	}
	return trimmed
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

// ErrSharedDatabaseUnavailable marks a Snowflake HTTP 422 SQL compilation error meaning the
// database's underlying data share has been revoked or pulled by the publisher. Snowflake still
// lists such a database via SHOW DATABASES / GetDatabase, but any statement scoped into it (SHOW
// SCHEMAS, SHOW SECRETS, SHOW TABLES, ...) fails with this specific, stable message. Unlike
// ErrInsufficientPrivileges this is not an access-control denial - QueryFailureStatus carries a
// SQL-compilation code, not 003001 - but it is equally unactionable by the connector's role: the
// object stays gone until the publisher restores the share.
//
// Client methods join this sentinel the same way they join ErrInsufficientPrivileges, so callers
// can skip the object instead of failing the sync.
var ErrSharedDatabaseUnavailable = errors.New("baton-snowflake: shared database unavailable")

// sharedDatabaseUnavailableMessage is the stable substring of Snowflake's canned SQL compilation
// error for a database whose backing share has been revoked or pulled by the publisher. There is
// no dedicated QueryFailureStatus code for this condition the way there is for access-control
// denials (003001), so it is identified by its message text instead. Anchoring on the full canned
// phrase - not just "no longer available for use" - keeps the predicate from swallowing an
// unrelated 422 that happens to share that trailing wording; see
// TestClient_NonAccessControl422StaysFatal, which pins that "Object does not exist" stays fatal.
const sharedDatabaseUnavailableMessage = "Shared database is no longer available for use"

// isSharedDatabaseUnavailable reports whether a response is the 422 Snowflake returns for a
// revoked/unavailable shared database, rather than a genuine SQL-compilation bug.
func isSharedDatabaseUnavailable(resp *http.Response, apiErr *SnowflakeError) bool {
	return resp != nil &&
		resp.StatusCode == http.StatusUnprocessableEntity &&
		apiErr != nil &&
		strings.Contains(apiErr.Message(), sharedDatabaseUnavailableMessage)
}

// IsSharedDatabaseUnavailable reports whether err is a Snowflake shared-database-unavailable
// denial that the connector may skip (HTTP 422 whose body matches Snowflake's canned "no longer
// available for use" message, joined as ErrSharedDatabaseUnavailable).
func IsSharedDatabaseUnavailable(err error) bool {
	// An ACCOUNT_USAGE failure is never skippable, and classifyAccountUsageError joins this
	// sentinel alongside ErrAccountUsageUnavailable for the shared-database shape. Joining
	// alone is not enough: a call site that checks this predicate first would skip the
	// database and continue. The fatal sentinel has to win here, not just be present.
	if IsAccountUsageUnavailable(err) {
		return false
	}
	return err != nil && errors.Is(err, ErrSharedDatabaseUnavailable)
}

// IsUnprocessableEntity reports whether the call failed with HTTP 422, regardless of Snowflake's
// error code. It is a status-only helper for call sites that only have a raw statusCode (or a
// legacy string-matched error) and treat "unprocessable" as "not resolvable" — e.g. shared/system
// database quirks on GetDatabase.
//
// It is NOT the privilege-skip used by CXH-2193 paths. Those must call IsInsufficientPrivileges
// so a SQL-compilation 422 cannot be swallowed as invisible data.
func IsUnprocessableEntity(statusCode int, err error) bool {
	// Same ordering problem as IsSharedDatabaseUnavailable, and worse here because this
	// predicate keys on the raw status: classifyAccountUsageError preserves the underlying
	// 422, so an account-wide ACCOUNT_USAGE failure would read as a per-object "not
	// resolvable" and be swallowed. pkg/connector/tables.go then marks the database shared,
	// which collapses every table under it to owner-only entitlements and zero grants - a
	// silent deletion in place of a loud failure.
	if IsAccountUsageUnavailable(err) {
		return false
	}
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

// ErrAccountUsageUnavailable marks a failure to read a SNOWFLAKE.ACCOUNT_USAGE view under
// the ACCOUNT_USAGE discovery path. Unlike ErrInsufficientPrivileges this is never a
// skippable "nothing visible here": the ACCOUNT_USAGE views are account-wide, so a role
// that cannot read one cannot read any object of that kind, and continuing would sync an
// empty resource type and delete everything previously synced under it.
//
// Snowflake reports a missing ACCOUNT_USAGE grant as a SQL compilation error ("Object
// '<view>' does not exist or not authorized") rather than as an access-control denial, so
// this sentinel is what turns that opaque message into a named, actionable one.
var ErrAccountUsageUnavailable = errors.New("baton-snowflake: SNOWFLAKE.ACCOUNT_USAGE is not readable")

// accountUsageViewerGrants names the database roles that make the ACCOUNT_USAGE views
// readable, so the diagnostic tells the operator what to run instead of only what failed.
const accountUsageViewerGrants = "GRANT DATABASE ROLE SNOWFLAKE.SECURITY_VIEWER TO ROLE <connector role> " +
	"(USERS, ROLES, GRANTS_TO_USERS, GRANTS_TO_ROLES) and " +
	"GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER TO ROLE <connector role> " +
	"(DATABASES, SCHEMATA, TABLES)"

// IsAccountUsageUnavailable reports whether err is a failed ACCOUNT_USAGE view read.
func IsAccountUsageUnavailable(err error) bool {
	return err != nil && errors.Is(err, ErrAccountUsageUnavailable)
}

// statusCodeOf returns resp's status code, or 0 for a nil response (a transport failure
// that never produced one).
func statusCodeOf(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

// classifyAccountUsageError turns a failed ACCOUNT_USAGE read into a named, actionable
// error. Both of Snowflake's refusal shapes are covered: the access-control denial (422 /
// 003001) and the SQL compilation error it uses when the view is simply not granted, which
// is indistinguishable from a typo in the view name without this context.
//
// It also names the warehouse requirement. These are SELECTs, not metadata commands, so a
// service account with no usable warehouse fails here with an unrelated-looking error.
func classifyAccountUsageError(view string, resp *http.Response, apiErr *SnowflakeError, err error) error {
	if err == nil {
		return nil
	}
	// Only a 422 carries a Snowflake refusal this function can interpret. Anything else - a
	// 429, a 5xx, or a transport failure that never produced a response - is a transient or
	// infrastructural error, and labelling it PermissionDenied would both mislead the operator
	// ("grant the viewer roles" for a 503) and strip the rate-limit gRPC details that
	// dedupeAPIError carries over for the SDK's retry logic.
	if resp == nil || resp.StatusCode != http.StatusUnprocessableEntity {
		return dedupeAPIError(err)
	}
	if isSharedDatabaseUnavailable(resp, apiErr) {
		// Both sentinels are joined: ErrSharedDatabaseUnavailable is a skip-this-object
		// signal, which is the wrong shape for an account-wide view read, so
		// ErrAccountUsageUnavailable rides along to keep the fatal predicate true no matter
		// which one a call site checks first.
		return uhttp.WrapErrors(
			codes.NotFound,
			fmt.Sprintf("baton-snowflake: shared database unavailable while reading %s", view),
			ErrAccountUsageUnavailable, ErrSharedDatabaseUnavailable, err,
		)
	}
	// An "invalid identifier" compilation error is a different failure with a different
	// fix: the view is readable but does not expose a column this connector selects,
	// because the account is on an ACCOUNT_USAGE schema version that predates it. Pointing
	// the operator at the viewer grants there would send them after the wrong cause.
	if isInvalidIdentifier(resp, apiErr) {
		return uhttp.WrapErrors(
			codes.FailedPrecondition,
			fmt.Sprintf(
				"baton-snowflake: %s is readable but rejected a column this connector selects, so "+
					"this account's ACCOUNT_USAGE schema does not expose it. Use "+
					"--discovery-mode=show instead, and report the Snowflake error below so the "+
					"column list can be adjusted",
				view,
			),
			ErrAccountUsageUnavailable, err,
		)
	}
	return uhttp.WrapErrors(
		codes.PermissionDenied,
		fmt.Sprintf(
			"baton-snowflake: failed to read %s under --discovery-mode=account_usage. Grant the "+
				"viewer database roles (%s), and confirm the service account has USAGE on a "+
				"warehouse that can resume - ACCOUNT_USAGE reads are SELECTs and need compute, "+
				"unlike the SHOW commands the default discovery mode uses",
			view, accountUsageViewerGrants,
		),
		ErrAccountUsageUnavailable, err,
	)
}

// invalidIdentifierErrorCode is Snowflake's error code for "SQL compilation error: invalid
// identifier", which is what a SELECT of a column the view does not have produces.
const invalidIdentifierErrorCode = "000904"

// isInvalidIdentifier reports whether a response is the 422 Snowflake returns for a column
// that does not exist on the object being selected from.
func isInvalidIdentifier(resp *http.Response, apiErr *SnowflakeError) bool {
	return resp != nil &&
		resp.StatusCode == http.StatusUnprocessableEntity &&
		apiErr != nil &&
		(apiErr.Code == invalidIdentifierErrorCode ||
			strings.Contains(apiErr.Message(), "invalid identifier"))
}

// ErrStatementNotComplete marks a SQL API response for a statement that has not finished
// executing.
//
// POST /api/v2/statements answers 202 - and GET /statements/<handle> keeps answering 202 -
// when a statement exceeds the synchronous execution window, returning a handle with no rows
// and no rowType. 202 is a 2xx, so uhttp does not treat it as an error, WithJSONResponse
// decodes a body with no data, and every row parser turns that into an empty slice with a
// nil error. A sync reads that as "this resource type is empty" and C1 reads it as a
// deletion of everything under it, so an unfinished statement has to fail loudly instead.
//
// SHOW discovery barely reaches this: SHOW commands are metadata-only and return in
// milliseconds. ACCOUNT_USAGE reads are warehouse-backed SELECTs over views that can be very
// large, on a warehouse that may have to resume first, which is exactly the shape that
// crosses the window.
//
// This is the loud-failure floor, not full async support. Polling the handle to completion is
// the real fix and is deliberately not attempted here: it needs a timeout and retry policy
// that is a product decision rather than a bug fix.
var ErrStatementNotComplete = errors.New("baton-snowflake: statement has not finished executing")

// IsStatementNotComplete reports whether err is an unfinished-statement response.
func IsStatementNotComplete(err error) bool {
	return err != nil && errors.Is(err, ErrStatementNotComplete)
}

// errIfStatementIncomplete converts a 202 into ErrStatementNotComplete. It is called on the
// success path of a statements request, where err is nil precisely because 202 is a 2xx.
func errIfStatementIncomplete(resp *http.Response, what string) error {
	if resp == nil || resp.StatusCode != http.StatusAccepted {
		return nil
	}
	return uhttp.WrapErrors(
		codes.Unavailable,
		fmt.Sprintf(
			"baton-snowflake: %s did not finish inside the SQL API's synchronous window "+
				"(HTTP 202). Retry the sync; if it recurs, the warehouse is too small for the "+
				"volume this statement reads, or --discovery-mode=show avoids the warehouse "+
				"entirely for this resource type",
			what,
		),
		ErrStatementNotComplete,
	)
}

// skippableDenial reports whether a 422/003001 on an inventory read may be treated as
// "nothing visible here" and skipped.
//
// Under SHOW discovery it may: the denial is genuinely per-object, because a role can hold
// USAGE on one database and not another. Under ACCOUNT_USAGE it never may - the views are
// account-wide, so a role that cannot read one cannot read any object of that kind, and
// skipping would sync an empty resource type and delete everything previously synced under
// it. Callers on a read path that has no ACCOUNT_USAGE equivalent (secrets, RSA keys,
// integrations, tokens) stay on isAccessControlDenial directly: those really are
// per-object in both modes.
func (c *Client) skippableDenial(resp *http.Response, apiErr *SnowflakeError) bool {
	return !c.usesAccountUsage() && isAccessControlDenial(resp, apiErr)
}

// skippableSharedDatabase is the same discovery-mode gate for the shared-database-unavailable
// shape, for the same reason.
func (c *Client) skippableSharedDatabase(resp *http.Response, apiErr *SnowflakeError) bool {
	return !c.usesAccountUsage() && isSharedDatabaseUnavailable(resp, apiErr)
}

// classifyReadError classifies a failed inventory read according to the active discovery
// mode. Under ACCOUNT_USAGE a failure is never a skippable "nothing visible here" - the
// views are account-wide, so a role that cannot read one cannot read any object of that
// kind, and swallowing it would sync an empty resource type and delete everything
// previously synced under it. Under SHOW discovery the behavior is unchanged.
func (c *Client) classifyReadError(view string, resp *http.Response, apiErr *SnowflakeError, err error) error {
	if err == nil {
		return nil
	}
	if c.usesAccountUsage() {
		return classifyAccountUsageError(view, resp, apiErr, err)
	}
	return dedupeAPIError(err)
}
