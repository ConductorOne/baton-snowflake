package snowflake

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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
	// Snowflake answers 000606 when the role has no usable default warehouse, or it is suspended.
	noActiveWarehouseErrorCode = "000606"

	credentialsStatusKey         = "status"
	credentialsStatusLoaded      = "loaded"
	credentialsStatusUnavailable = "unavailable"
)

// errNoWarehouse marks a credentials query that could not run for lack of a usable warehouse.
// Last-used dates are optional, so this degrades like a denial instead of failing the sync.
var errNoWarehouse = errors.New("baton-snowflake: no usable warehouse")

type Credential struct {
	UserName    string     `json:"user_name"`
	Type        string     `json:"type"`
	Name        string     `json:"name"`
	PublicKeyFp string     `json:"public_key_fp,omitempty"`
	LastUsedOn  *time.Time `json:"last_used_on,omitempty"`
}

// UserCredentials returns the user's key pair and PAT rows from ACCOUNT_USAGE.CREDENTIALS, loading the view once per session.
// available is false when the connector role may not read the view.
func (c *Client) UserCredentials(ctx context.Context, ss sessions.SessionStore, userName string) ([]Credential, bool, error) {
	if ss == nil {
		var userCredentials []Credential
		err := c.listCredentials(ctx, func(batch map[string][]Credential) error {
			userCredentials = append(userCredentials, batch[userName]...)
			return nil
		})
		if IsInsufficientPrivileges(err) || errors.Is(err, errNoWarehouse) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		return userCredentials, true, nil
	}

	status, found, err := session.GetJSON[string](ctx, ss, credentialsStatusKey, credentialsStatusNamespace)
	if err != nil {
		return nil, false, fmt.Errorf("baton-snowflake: read credentials cache status: %w", err)
	}
	if !found {
		status, err = c.loadCredentials(ctx, ss)
		if err != nil {
			return nil, false, err
		}
	}
	if status == credentialsStatusUnavailable {
		return nil, false, nil
	}

	userCredentials, _, err := session.GetJSON[[]Credential](ctx, ss, userName, credentialsNamespace)
	if err != nil {
		return nil, false, fmt.Errorf("baton-snowflake: read cached credentials: %w", err)
	}
	return userCredentials, true, nil
}

func (c *Client) loadCredentials(ctx context.Context, ss sessions.SessionStore) (string, error) {
	err := c.listCredentials(ctx, func(batch map[string][]Credential) error {
		if err := session.SetManyJSON(ctx, ss, batch, credentialsNamespace); err != nil {
			return fmt.Errorf("baton-snowflake: cache credentials: %w", err)
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
			"set a DEFAULT_WAREHOUSE the connector role has USAGE on to populate them", zap.Error(err))
		status = credentialsStatusUnavailable
	case err != nil:
		return "", err
	}
	if err := session.SetJSON(ctx, ss, credentialsStatusKey, status, credentialsStatusNamespace); err != nil {
		return "", fmt.Errorf("baton-snowflake: cache credentials status: %w", err)
	}
	return status, nil
}

// listCredentials streams the view to emit one partition at a time, grouped by user.
// A user's rows are held back until the next user starts, since they may continue in the following partition.
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
		return fmt.Errorf("baton-snowflake: ListCredentials: result spans %d partitions but has no statement handle", numPartitions)
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
			return fmt.Errorf("baton-snowflake: failed to fetch credentials partition %d: %w", partitionID, err)
		}
		if err := process(rows); err != nil {
			return err
		}
	}

	if metadata.NumRows > 0 && totalRows != metadata.NumRows {
		return fmt.Errorf("baton-snowflake: ListCredentials: got %d rows but Snowflake declared %d (statementHandle=%s)",
			totalRows, metadata.NumRows, response.StatementHandle)
	}
	if len(pending) > 0 {
		return emit(map[string][]Credential{pendingUser: pending})
	}
	return nil
}

func (c *Client) fetchPartitionRows(ctx context.Context, handle string, partitionID int) ([][]string, error) {
	req, err := c.GetStatementPartition(ctx, handle, partitionID)
	if err != nil {
		return nil, err
	}
	var partition StatementsApiResponseBase
	var apiErr SnowflakeError
	resp, err := c.Do(req, uhttp.WithJSONResponse(&partition), uhttp.WithErrorResponse(&apiErr))
	closeResponseBody(resp)
	if err != nil {
		return nil, dedupeAPIError(err)
	}
	return partition.Data, nil
}

func parseCredentialRow(metadata *ResultSetMetadata, row []string) (Credential, error) {
	var credential Credential
	var err error
	if credential.UserName, err = metadata.GetStringValueFromRow(row, "USER_NAME"); err != nil {
		return Credential{}, fmt.Errorf("baton-snowflake: read credential user name: %w", err)
	}
	if credential.Type, err = metadata.GetStringValueFromRow(row, "TYPE"); err != nil {
		return Credential{}, fmt.Errorf("baton-snowflake: read credential type: %w", err)
	}
	if credential.Name, err = metadata.GetStringValueFromRow(row, "NAME"); err != nil {
		return Credential{}, fmt.Errorf("baton-snowflake: read credential name: %w", err)
	}
	fp, err := metadata.GetStringValueFromRow(row, "PUBLIC_KEY_FP")
	if err != nil {
		return Credential{}, fmt.Errorf("baton-snowflake: read credential public key fingerprint: %w", err)
	}
	credential.PublicKeyFp = NormalizeNullValue(fp)
	lastUsedOn, err := metadata.GetTimeValueFromRow(row, "LAST_USED_ON")
	if err != nil {
		return Credential{}, fmt.Errorf("baton-snowflake: read credential last used on: %w", err)
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
			"baton-snowflake: insufficient privileges to read ACCOUNT_USAGE.CREDENTIALS",
			ErrInsufficientPrivileges, err,
		)
	}
	if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity && apiErr.Code == noActiveWarehouseErrorCode {
		return uhttp.WrapErrors(
			codes.FailedPrecondition,
			"baton-snowflake: no usable warehouse to read ACCOUNT_USAGE.CREDENTIALS",
			errNoWarehouse, err,
		)
	}
	return dedupeAPIError(err)
}
