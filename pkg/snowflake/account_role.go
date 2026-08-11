package snowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
)

var accountRoleStructFieldToColumnMap = map[string]string{
	structFieldName: columnName,
}

// accountRoleGranteeStructFieldToColumnMap maps AccountRoleGrantee fields to SHOW GRANTS OF
// ROLE's columns: created_on, role, granted_to, grantee_name, granted_by (only the ones this
// connector consumes are mapped - see ParseRow).
var accountRoleGranteeStructFieldToColumnMap = map[string]string{
	"RoleName":             columnRole,
	"GranteeType":          columnGrantedTo,
	structFieldGranteeName: columnGranteeName,
}

type (
	AccountRole struct {
		Name string
	}
	ListAccountRolesRawResponse struct {
		StatementsApiResponseBase
	}
	AccountRoleGrantee struct {
		RoleName    string
		GranteeName string
		GranteeType string
	}
	ListAccountRoleGranteesRawResponse struct {
		StatementsApiResponseBase
	}
	GrantAccountRoleResponse struct {
		StatementsApiResponseBase
	}
)

func (ar *AccountRole) GetColumnName(fieldName string) string {
	return accountRoleStructFieldToColumnMap[fieldName]
}

func (g *AccountRoleGrantee) GetColumnName(fieldName string) string {
	return accountRoleGranteeStructFieldToColumnMap[fieldName]
}

func (r *ListAccountRolesRawResponse) GetAccountRoles() ([]AccountRole, error) {
	var accountRoles []AccountRole
	for _, row := range r.Data {
		accountRole := &AccountRole{}
		if err := r.ResultSetMetadata.ParseRow(accountRole, row); err != nil {
			return nil, err
		}

		accountRoles = append(accountRoles, *accountRole)
	}

	return accountRoles, nil
}

// GetAccountRoleGrantees parses SHOW GRANTS OF ROLE rows by column name via
// ResultSetMetadata.ParseRow rather than fixed positional indexes, so a Snowflake behavior
// change that reorders or adds columns to this command's output doesn't silently corrupt
// RoleName/GranteeType/GranteeName. See accountRoleGranteesCursor for how rowType metadata -
// only present on the partition-0 response - is carried forward for later partitions.
func (r *ListAccountRoleGranteesRawResponse) GetAccountRoleGrantees() ([]AccountRoleGrantee, error) {
	var accountRoleGrantees []AccountRoleGrantee
	for _, row := range r.Data {
		grantee := &AccountRoleGrantee{}
		if err := r.ResultSetMetadata.ParseRow(grantee, row); err != nil {
			return nil, err
		}
		grantee.GranteeName = unquoteSnowflakeIdentifier(grantee.GranteeName)

		accountRoleGrantees = append(accountRoleGrantees, *grantee)
	}
	return accountRoleGrantees, nil
}

func (c *Client) ListAccountRoles(ctx context.Context, cursor string, limit int) ([]AccountRole, error) {
	var queries []string

	if cursor != "" {
		queries = append(queries, fmt.Sprintf("SHOW ROLES LIMIT %d FROM '%s';", limit, escapeStringLiteral(cursor)))
	} else {
		queries = append(queries, fmt.Sprintf("SHOW ROLES LIMIT %d;", limit))
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListAccountRolesRawResponse
	var apiErr SnowflakeError
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp1)
	if err != nil {
		return nil, dedupeAPIError(err)
	}

	l := ctxzap.Extract(ctx)
	l.Debug("ListAccountRoles", zap.String("response.code", response.Code), zap.String("response.message", response.Message))

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp2)
	if err != nil {
		return nil, dedupeAPIError(err)
	}

	accountRoles, err := response.GetAccountRoles()
	if err != nil {
		return nil, err
	}

	return accountRoles, nil
}

// accountRoleGranteesCursor is the opaque page cursor for ListAccountRoleGrantees. SHOW GRANTS
// OF ROLE rows are parsed by column name via ResultSetMetadata.ParseRow, and Snowflake's SQL API
// only returns that column layout (rowType) on the partition-0 response - partitions 1..N return
// bare data with no metadata. The cursor therefore carries the rowType layout captured from
// partition 0 forward so later partitions can still be parsed. Mirrors tableGrantsCursor in
// table.go, which solves the identical problem for SHOW GRANTS ON TABLE/VIEW.
type accountRoleGranteesCursor struct {
	Handle          string    `json:"handle"`
	PartitionID     int       `json:"partitionId"`
	TotalPartitions int       `json:"totalPartitions"`
	RowTypes        []RowType `json:"rowTypes"`
}

func encodeAccountRoleGranteesCursor(cur accountRoleGranteesCursor) (string, error) {
	b, err := json.Marshal(cur)
	if err != nil {
		return "", fmt.Errorf("snowflake: failed to encode grantee page cursor: %w", err)
	}
	return string(b), nil
}

func decodeAccountRoleGranteesCursor(cursor string) (accountRoleGranteesCursor, error) {
	var cur accountRoleGranteesCursor
	if err := json.Unmarshal([]byte(cursor), &cur); err != nil {
		return accountRoleGranteesCursor{}, fmt.Errorf("snowflake: invalid grantee page cursor: %w", err)
	}
	return cur, nil
}

// ListAccountRoleGrantees returns one page of grantees for the given role.
// cursor is empty on the first call; subsequent calls pass the opaque cursor returned by the previous call.
// The returned cursor is empty when all pages have been consumed.
func (c *Client) ListAccountRoleGrantees(ctx context.Context, roleName string, cursor string) ([]AccountRoleGrantee, string, error) {
	var response ListAccountRoleGranteesRawResponse
	var apiErr SnowflakeError

	if cursor == "" {
		queries := []string{fmt.Sprintf("SHOW GRANTS OF ROLE \"%s\";", escapeDoubleQuotedIdentifier(roleName))}

		req, err := c.PostStatementRequest(ctx, queries)
		if err != nil {
			return nil, "", err
		}

		resp1, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
		defer closeResponseBody(resp1)
		if err != nil {
			return nil, "", dedupeAPIError(err)
		}

		handle := response.StatementHandle

		req, err = c.GetStatementResponse(ctx, handle)
		if err != nil {
			return nil, "", err
		}
		resp2, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
		defer closeResponseBody(resp2)
		if err != nil {
			return nil, "", dedupeAPIError(err)
		}

		numPartitions := len(response.ResultSetMetadata.PartitionInfo)
		l := ctxzap.Extract(ctx)
		l.Debug("ListAccountRoleGrantees", zap.String("role", roleName), zap.Int("numPartitions", numPartitions), zap.Int("numRows", response.ResultSetMetadata.NumRows))

		grantees, err := response.GetAccountRoleGrantees()
		if err != nil {
			return nil, "", err
		}

		var nextCursor string
		if numPartitions > 1 {
			nextCursor, err = encodeAccountRoleGranteesCursor(accountRoleGranteesCursor{
				Handle:          handle,
				PartitionID:     1,
				TotalPartitions: numPartitions,
				RowTypes:        response.ResultSetMetadata.RowTypes,
			})
			if err != nil {
				return nil, "", err
			}
		}

		return grantees, nextCursor, nil
	}

	// Subsequent calls: fetch the encoded partition directly.
	cur, err := decodeAccountRoleGranteesCursor(cursor)
	if err != nil {
		return nil, "", err
	}

	req, err := c.GetStatementPartition(ctx, cur.Handle, cur.PartitionID)
	if err != nil {
		return nil, "", err
	}
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp)
	if err != nil {
		return nil, "", dedupeAPIError(err)
	}

	// Partition-only responses carry no rowType metadata - restore it from the cursor so
	// ParseRow can still resolve column names by position.
	response.ResultSetMetadata.RowTypes = cur.RowTypes

	grantees, err := response.GetAccountRoleGrantees()
	if err != nil {
		return nil, "", err
	}

	var nextCursor string
	if cur.PartitionID+1 < cur.TotalPartitions {
		nextCursor, err = encodeAccountRoleGranteesCursor(accountRoleGranteesCursor{
			Handle:          cur.Handle,
			PartitionID:     cur.PartitionID + 1,
			TotalPartitions: cur.TotalPartitions,
			RowTypes:        cur.RowTypes,
		})
		if err != nil {
			return nil, "", err
		}
	}

	return grantees, nextCursor, nil
}

func (c *Client) CacheAccountRoles(ctx context.Context, ss sessions.SessionStore, roles []AccountRole) error {
	if ss == nil || len(roles) == 0 {
		return nil
	}
	m := make(map[string]*AccountRole, len(roles))
	for i := range roles {
		role := roles[i]
		m[role.Name] = &role
	}
	if err := session.SetManyJSON(ctx, ss, m, accountRoleNamespace); err != nil {
		return fmt.Errorf("snowflake: cache account roles: %w", err)
	}
	return nil
}

func (c *Client) GetAccountRole(ctx context.Context, ss sessions.SessionStore, roleName string) (*AccountRole, int, error) {
	if ss != nil {
		if cached, found, err := session.GetJSON[*AccountRole](ctx, ss, roleName, accountRoleNamespace); err == nil && found {
			return cached, http.StatusOK, nil
		}
	}

	// SHOW ROLES' LIKE filter has no ESCAPE clause (unlike the general SQL LIKE predicate) -
	// only the single quote needs escaping to keep the string literal well-formed. _ and %
	// remain active wildcards; there is no Snowflake syntax to suppress that for SHOW commands.
	queries := []string{
		fmt.Sprintf("SHOW ROLES LIKE '%s' LIMIT %d;", escapeLikeStringLiteral(roleName), wildcardLookupLimit),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, 0, err
	}

	var response ListAccountRolesRawResponse
	var apiErr SnowflakeError
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp)
	if err != nil {
		statusCode := 0
		if resp != nil {
			statusCode = resp.StatusCode
		}
		// SHOW ROLES LIKE on a system role the connector cannot observe returns 422/003001.
		if isAccessControlDenial(resp, &apiErr) {
			return nil, statusCode, uhttp.WrapErrors(
				codes.PermissionDenied,
				fmt.Sprintf("baton-snowflake: insufficient privileges to describe role %s", roleName),
				ErrInsufficientPrivileges, err,
			)
		}
		return nil, statusCode, dedupeAPIError(err)
	}

	accountRoles, err := response.GetAccountRoles()
	if err != nil {
		return nil, resp.StatusCode, err
	}

	// Wildcard collisions can outrank the real role, so scan all rows rather than assuming
	// accountRoles[0] is the match.
	var role *AccountRole
	for _, ar := range accountRoles {
		if ar.Name == roleName {
			role = &ar
			break
		}
	}

	if ss != nil && role != nil {
		_ = session.SetJSON(ctx, ss, roleName, role, accountRoleNamespace)
	}

	return role, resp.StatusCode, nil
}

func (c *Client) GrantAccountRole(ctx context.Context, roleName, userName string) error {
	queries := []string{
		fmt.Sprintf("GRANT ROLE \"%s\" TO USER \"%s\";", escapeDoubleQuotedIdentifier(roleName), escapeDoubleQuotedIdentifier(userName)),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return err
	}

	var apiErr SnowflakeError
	resp, err := c.Do(req, uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp)
	if err != nil {
		return dedupeAPIError(err)
	}

	return nil
}

func (c *Client) RevokeAccountRole(ctx context.Context, roleName, userName string) error {
	queries := []string{
		fmt.Sprintf("REVOKE ROLE \"%s\" FROM USER \"%s\";", escapeDoubleQuotedIdentifier(roleName), escapeDoubleQuotedIdentifier(userName)),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return err
	}

	var apiErr SnowflakeError
	resp, err := c.Do(req, uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp)
	if err != nil {
		return dedupeAPIError(err)
	}

	return nil
}
