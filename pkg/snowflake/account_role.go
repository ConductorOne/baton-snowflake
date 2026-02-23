package snowflake

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

var accountRoleStructFieldToColumnMap = map[string]string{
	"Name": "name",
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

func (c *Client) ListAccountRoleGrantees(ctx context.Context, roleName string) ([]AccountRoleGrantee, error) {
	queries := []string{
		fmt.Sprintf("SHOW GRANTS OF ROLE \"%s\";", roleName),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListAccountRoleGranteesRawResponse
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp1)
	if err != nil {
		return nil, err
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp2)
	if err != nil {
		return nil, err
	}

	accountRoleGrantees := response.GetAccountRoleGrantees()

	return accountRoleGrantees, nil
}

func (c *Client) GetAccountRole(ctx context.Context, roleName string) (*AccountRole, int, error) {
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

	if len(accountRoles) == 0 {
		return nil, resp.StatusCode, nil
	}

	return &accountRoles[0], resp.StatusCode, nil
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
