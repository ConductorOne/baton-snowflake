package snowflake

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

var accountRoleStructFieldToColumnMap = map[string]string{
	structFieldName: columnName,
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
		Data [][]string `json:"data"`
	}
	GrantAccountRoleResponse struct {
		StatementsApiResponseBase
	}
)

func (ar *AccountRole) GetColumnName(fieldName string) string {
	return accountRoleStructFieldToColumnMap[fieldName]
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

func (r *ListAccountRoleGranteesRawResponse) GetAccountRoleGrantees() []AccountRoleGrantee {
	var accountRoleGrantees []AccountRoleGrantee
	for _, accountRoleGrantee := range r.Data {
		accountRoleGrantees = append(accountRoleGrantees, AccountRoleGrantee{
			RoleName:    accountRoleGrantee[1],
			GranteeName: accountRoleGrantee[3],
			GranteeType: accountRoleGrantee[2],
		})
	}
	return accountRoleGrantees
}

func (c *Client) ListAccountRoles(ctx context.Context, cursor string, limit int) ([]AccountRole, error) {
	var queries []string

	if cursor != "" {
		queries = append(queries, fmt.Sprintf("SHOW ROLES LIMIT %d FROM '%s';", limit, cursor))
	} else {
		queries = append(queries, fmt.Sprintf("SHOW ROLES LIMIT %d;", limit))
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListAccountRolesRawResponse
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp1)
	if err != nil {
		return nil, err
	}

	l := ctxzap.Extract(ctx)
	l.Debug("ListAccountRoles", zap.String("response.code", response.Code), zap.String("response.message", response.Message))

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp2)
	if err != nil {
		return nil, err
	}

	accountRoles, err := response.GetAccountRoles()
	if err != nil {
		return nil, err
	}

	return accountRoles, nil
}

// ListAccountRoleGrantees returns one page of grantees for the given role.
// cursor is empty on the first call; subsequent calls pass the opaque cursor returned by the previous call.
// The returned cursor is empty when all pages have been consumed.
// Cursor format (internal): "{statementHandle}:{partitionID}:{totalPartitions}".
func (c *Client) ListAccountRoleGrantees(ctx context.Context, roleName string, cursor string) ([]AccountRoleGrantee, string, error) {
	var response ListAccountRoleGranteesRawResponse

	if cursor == "" {
		queries := []string{fmt.Sprintf("SHOW GRANTS OF ROLE \"%s\";", roleName)}

		req, err := c.PostStatementRequest(ctx, queries)
		if err != nil {
			return nil, "", err
		}

		resp1, err := c.Do(req, uhttp.WithJSONResponse(&response))
		defer closeResponseBody(resp1)
		if err != nil {
			return nil, "", err
		}

		handle := response.StatementHandle

		req, err = c.GetStatementResponse(ctx, handle)
		if err != nil {
			return nil, "", err
		}
		resp2, err := c.Do(req, uhttp.WithJSONResponse(&response))
		defer closeResponseBody(resp2)
		if err != nil {
			return nil, "", err
		}

		numPartitions := len(response.ResultSetMetadata.PartitionInfo)
		l := ctxzap.Extract(ctx)
		l.Debug("ListAccountRoleGrantees", zap.String("role", roleName), zap.Int("numPartitions", numPartitions), zap.Int("numRows", response.ResultSetMetadata.NumRows))

		var nextCursor string
		if numPartitions > 1 {
			nextCursor = fmt.Sprintf("%s:1:%d", handle, numPartitions)
		}

		return response.GetAccountRoleGrantees(), nextCursor, nil
	}

	// Subsequent calls: fetch the encoded partition directly.
	parts := strings.SplitN(cursor, ":", 3)
	if len(parts) != 3 {
		return nil, "", fmt.Errorf("snowflake: invalid grantee page cursor")
	}
	handle := parts[0]
	partitionID, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, "", fmt.Errorf("snowflake: invalid partition ID in cursor: %w", err)
	}
	totalPartitions, err := strconv.Atoi(parts[2])
	if err != nil {
		return nil, "", fmt.Errorf("snowflake: invalid partition count in cursor: %w", err)
	}
	req, err := c.GetStatementPartition(ctx, handle, partitionID)
	if err != nil {
		return nil, "", err
	}
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp)
	if err != nil {
		return nil, "", err
	}

	var nextCursor string
	if partitionID+1 < totalPartitions {
		nextCursor = fmt.Sprintf("%s:%d:%d", handle, partitionID+1, totalPartitions)
	}

	return response.GetAccountRoleGrantees(), nextCursor, nil
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

	queries := []string{
		fmt.Sprintf("SHOW ROLES LIKE '%s' LIMIT 1;", roleName),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, 0, err
	}

	var response ListAccountRolesRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp)
	if err != nil {
		statusCode := 0
		if resp != nil {
			statusCode = resp.StatusCode
		}
		return nil, statusCode, err
	}

	accountRoles, err := response.GetAccountRoles()
	if err != nil {
		return nil, resp.StatusCode, err
	}

	var role *AccountRole
	if len(accountRoles) > 0 {
		role = &accountRoles[0]
	}

	if ss != nil {
		_ = session.SetJSON(ctx, ss, roleName, role, accountRoleNamespace)
	}

	return role, resp.StatusCode, nil
}

func (c *Client) GrantAccountRole(ctx context.Context, roleName, userName string) error {
	queries := []string{
		fmt.Sprintf("GRANT ROLE \"%s\" TO USER \"%s\";", roleName, userName),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return err
	}

	resp, err := c.Do(req)
	defer closeResponseBody(resp)
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) RevokeAccountRole(ctx context.Context, roleName, userName string) error {
	queries := []string{
		fmt.Sprintf("REVOKE ROLE \"%s\" FROM USER \"%s\";", roleName, userName),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return err
	}

	resp, err := c.Do(req)
	defer closeResponseBody(resp)
	if err != nil {
		return err
	}

	return nil
}
