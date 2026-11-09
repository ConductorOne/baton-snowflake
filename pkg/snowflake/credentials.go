package snowflake

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
)

const (
	CredentialTypeKeyPair = "KEYPAIR"
	CredentialTypePAT     = "PAT"

	// Rows without a last use carry nothing to report. Ordered by user so each user's rows are contiguous
	// and can be flushed to the session store per partition.
	credentialsQuery = "SELECT USER_NAME, TYPE, NAME, ADDITIONAL_DETAILS:PUBLIC_KEY_FP::STRING AS PUBLIC_KEY_FP, LAST_USED_ON " +
		"FROM SNOWFLAKE.ACCOUNT_USAGE.CREDENTIALS WHERE TYPE IN ('KEYPAIR', 'PAT') AND LAST_USED_ON IS NOT NULL ORDER BY USER_NAME;"

	// A role without access to the SNOWFLAKE database gets "does not exist or not authorized" instead of 003001.
	sqlObjectNotAuthorizedErrorCode = "002003"

	// Unlike the SHOW/DESCRIBE statements the rest of the sync runs, this SELECT needs a warehouse.
	// No active warehouse in the session.
	noActiveWarehouseErrorCode = "000606"
	// Warehouse suspended and not resumable (AUTO_RESUME = FALSE, or suspended by a resource monitor).
	warehouseSuspendedErrorCode = "000608"
	// SQL API: no warehouse resolvable for the request (no DEFAULT_WAREHOUSE, or no USAGE on it).
	noWarehouseSpecifiedErrorCode = "391920"

	credentialsStatusKey         = "status"
	credentialsStatusLoaded      = "loaded"
	credentialsStatusUnavailable = "unavailable"
)

// errNoWarehouse marks a credentials query that could not run for lack of a usable warehouse.
// Last-used dates are optional, so this degrades like a denial instead of failing the sync.
var errNoWarehouse = errors.New("no usable warehouse")

// missingSessionWarning logs once per process, since without a session store every call would hit this.
var missingSessionWarning sync.Once

type Credential struct {
	UserName    string     `json:"user_name"`
	Type        string     `json:"type"`
	Name        string     `json:"name"`
	PublicKeyFp string     `json:"public_key_fp,omitempty"`
	LastUsedOn  *time.Time `json:"last_used_on,omitempty"`
}

// UserCredentials returns the user's KEYPAIR and PAT rows with a LAST_USED_ON, loading
// SNOWFLAKE.ACCOUNT_USAGE.CREDENTIALS once per session (POST /api/v2/statements).
// Requires the SNOWFLAKE.SECURITY_VIEWER database role and USAGE on the default warehouse;
// without either, or without a session store, it returns no rows and a nil error.
func (c *Client) UserCredentials(ctx context.Context, ss sessions.SessionStore, userName string) ([]Credential, error) {
	// Without a cache the whole view would be queried per user, so skip the optional dates instead.
	if ss == nil {
		missingSessionWarning.Do(func() {
			ctxzap.Extract(ctx).Warn("baton-snowflake: secret last-used dates unavailable; no session store to cache them")
		})
		return nil, nil
	}

	status, found, err := session.GetJSON[string](ctx, ss, credentialsStatusKey, credentialsStatusNamespace)
	if err != nil {
		return nil, fmt.Errorf("read credentials cache status: %w", err)
	}
	if !found {
		status, err = c.loadCredentials(ctx, ss)
		if err != nil {
			return nil, err
		}
	}
	if status == credentialsStatusUnavailable {
		return nil, nil
	}

	userCredentials, _, err := session.GetJSON[[]Credential](ctx, ss, userName, credentialsNamespace)
	if err != nil {
		return nil, fmt.Errorf("read cached credentials: %w", err)
	}
	return userCredentials, nil
}

func (c *Client) loadCredentials(ctx context.Context, ss sessions.SessionStore) (string, error) {
	err := c.listCredentials(ctx, func(batch map[string][]Credential) error {
		if err := session.SetManyJSON(ctx, ss, batch, credentialsNamespace); err != nil {
			return fmt.Errorf("cache credentials: %w", err)
		}
		return nil
	})
	status := credentialsStatusLoaded
	switch {
	case IsInsufficientPrivileges(err):
		// The status is cached for the session, so this logs once per sync.
		ctxzap.Extract(ctx).Warn("baton-snowflake: secret last-used dates unavailable; " +
			"grant SNOWFLAKE.SECURITY_VIEWER to the connector role to populate them")
		status = credentialsStatusUnavailable
	case errors.Is(err, errNoWarehouse):
		ctxzap.Extract(ctx).Warn("baton-snowflake: secret last-used dates unavailable; "+
			"the connector user needs a DEFAULT_WAREHOUSE that the connector role has USAGE on "+
			"and that is running or set to AUTO_RESUME", zap.Error(err))
		status = credentialsStatusUnavailable
	case err != nil:
		return "", err
	}
	if err := session.SetJSON(ctx, ss, credentialsStatusKey, status, credentialsStatusNamespace); err != nil {
		return "", fmt.Errorf("cache credentials status: %w", err)
	}
	return status, nil
}

// listCredentials streams the view to emit one partition at a time, grouped by user.
// A user's rows are held back until the next user starts, since they may continue in the following partition.
//
// Accepted risk: the result is fetched with a single follow-up GET, without polling. If the statement is still
// running after it, the result is read as empty and the sync has no last-used dates, without a warning.
// The view is small, so this needs an unusually slow warehouse, and the dates are optional.
func (c *Client) listCredentials(ctx context.Context, emit func(map[string][]Credential) error) error {
	req, err := c.PostStatementRequest(ctx, []string{credentialsQuery})
	if err != nil {
		return err
	}

	var response StatementsApiResponseBase
	var apiErr SnowflakeError
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp)
	if err != nil {
		return classifyCredentialsError(resp, &apiErr, err)
	}

	if c.needsStatementResultFetch(ctx, resp, &response, "ListCredentials") {
		req, err = c.GetStatementResponse(ctx, response.StatementHandle)
		if err != nil {
			return err
		}
		resp2, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
		defer closeResponseBody(resp2)
		if err != nil {
			return classifyCredentialsError(resp2, &apiErr, err)
		}
	}

	metadata := response.ResultSetMetadata
	numPartitions := len(metadata.PartitionInfo)
	if numPartitions > 1 && response.StatementHandle == "" {
		return fmt.Errorf("ListCredentials: result spans %d partitions but has no statement handle", numPartitions)
	}

	var pendingUser string
	var pending []Credential
	totalRows := 0
	process := func(rows [][]string) error {
		batch := make(map[string][]Credential)
		for _, row := range rows {
			credential, err := parseCredentialRow(&metadata, row)
			if err != nil {
				return err
			}
			if credential.UserName != pendingUser && len(pending) > 0 {
				batch[pendingUser] = append(batch[pendingUser], pending...)
				pending = nil
			}
			pendingUser = credential.UserName
			pending = append(pending, credential)
		}
		totalRows += len(rows)
		if len(batch) == 0 {
			return nil
		}
		return emit(batch)
	}

	if err := process(response.Data); err != nil {
		return err
	}
	for partitionID := 1; partitionID < numPartitions; partitionID++ {
		rows, err := c.fetchPartitionRows(ctx, response.StatementHandle, partitionID)
		if err != nil {
			return fmt.Errorf("failed to fetch credentials partition %d: %w", partitionID, err)
		}
		if err := process(rows); err != nil {
			return err
		}
	}

	if metadata.NumRows > 0 && totalRows != metadata.NumRows {
		return fmt.Errorf("ListCredentials: got %d rows but Snowflake declared %d (statementHandle=%s)",
			totalRows, metadata.NumRows, response.StatementHandle)
	}
	if len(pending) > 0 {
		return emit(map[string][]Credential{pendingUser: pending})
	}
	return nil
}

func parseCredentialRow(metadata *ResultSetMetadata, row []string) (Credential, error) {
	var credential Credential
	var err error
	if credential.UserName, err = metadata.GetStringValueFromRow(row, "USER_NAME"); err != nil {
		return Credential{}, fmt.Errorf("read credential user name: %w", err)
	}
	if credential.Type, err = metadata.GetStringValueFromRow(row, "TYPE"); err != nil {
		return Credential{}, fmt.Errorf("read credential type: %w", err)
	}
	if credential.Name, err = metadata.GetStringValueFromRow(row, "NAME"); err != nil {
		return Credential{}, fmt.Errorf("read credential name: %w", err)
	}
	fp, err := metadata.GetStringValueFromRow(row, "PUBLIC_KEY_FP")
	if err != nil {
		return Credential{}, fmt.Errorf("read credential public key fingerprint: %w", err)
	}
	credential.PublicKeyFp = NormalizeNullValue(fp)
	lastUsedOn, err := metadata.GetTimeValueFromRow(row, "LAST_USED_ON")
	if err != nil {
		return Credential{}, fmt.Errorf("read credential last used on: %w", err)
	}
	if !lastUsedOn.IsZero() {
		credential.LastUsedOn = &lastUsedOn
	}
	return credential, nil
}

func classifyCredentialsError(resp *http.Response, apiErr *SnowflakeError, err error) error {
	if isAccessControlDenial(resp, apiErr) ||
		(resp != nil && resp.StatusCode == http.StatusUnprocessableEntity && apiErr.Code == sqlObjectNotAuthorizedErrorCode) {
		return uhttp.WrapErrors(
			codes.PermissionDenied,
			"insufficient privileges to read ACCOUNT_USAGE.CREDENTIALS",
			ErrInsufficientPrivileges, err,
		)
	}
	if isWarehouseUnavailable(resp, apiErr) {
		return uhttp.WrapErrors(
			codes.FailedPrecondition,
			"no usable warehouse to read ACCOUNT_USAGE.CREDENTIALS",
			errNoWarehouse, err,
		)
	}
	return dedupeAPIError(err)
}

// isWarehouseUnavailable reports whether a response is the 422 that means the statement had no usable warehouse.
func isWarehouseUnavailable(resp *http.Response, apiErr *SnowflakeError) bool {
	if resp == nil || resp.StatusCode != http.StatusUnprocessableEntity || apiErr == nil {
		return false
	}
	switch apiErr.Code {
	case noActiveWarehouseErrorCode, warehouseSuspendedErrorCode, noWarehouseSpecifiedErrorCode:
		return true
	}
	return false
}
