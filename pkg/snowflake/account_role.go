package snowflake

import (
	"context"
	"fmt"
	"net/http"

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

func (c *Client) ListAccountRoles(ctx context.Context, cursor string, limit int) ([]AccountRole, *http.Response, error) {
	var queries []string

	if cursor != "" {
		queries = append(queries, fmt.Sprintf("SHOW ROLES LIMIT %d FROM '%s';", limit, cursor))
	} else {
		queries = append(queries, fmt.Sprintf("SHOW ROLES LIMIT %d;", limit))
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, nil, err
	}

	var response ListAccountRolesRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, nil, err
	}

	l := ctxzap.Extract(ctx)
	l.Debug("ListAccountRoles", zap.String("response.code", response.Code), zap.String("response.message", response.Message))

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, resp, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, resp, err
	}

	accountRoles, err := response.GetAccountRoles()
	if err != nil {
		return nil, resp, err
	}

	return accountRoles, resp, nil
}

func (c *Client) ListAccountRoleGrantees(ctx context.Context, roleName string) ([]AccountRoleGrantee, *http.Response, error) {
	queries := []string{
		fmt.Sprintf("SHOW GRANTS OF ROLE \"%s\";", roleName),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, nil, err
	}

	var response ListAccountRoleGranteesRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, nil, err
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, resp, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, resp, err
	}

	return response.GetAccountRoleGrantees(), resp, nil
}

func (c *Client) GetAccountRole(ctx context.Context, roleName string) (*AccountRole, *http.Response, error) {
	queries := []string{
		fmt.Sprintf("SHOW ROLES LIKE '%s' LIMIT 1;", roleName),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, nil, err
	}

	var response ListAccountRolesRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, nil, err
	}

	accountRoles, err := response.GetAccountRoles()
	if err != nil {
		return nil, resp, err
	}

	if len(accountRoles) == 0 {
		return nil, resp, nil
	}

	return &accountRoles[0], resp, nil
}

func (c *Client) GrantAccountRole(ctx context.Context, roleName, userName string) (*http.Response, error) {
	queries := []string{
		fmt.Sprintf("GRANT ROLE \"%s\" TO USER \"%s\";", roleName, userName),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	return c.Do(req)
}

func (c *Client) RevokeAccountRole(ctx context.Context, roleName, userName string) (*http.Response, error) {
	queries := []string{
		fmt.Sprintf("REVOKE ROLE \"%s\" FROM USER \"%s\";", roleName, userName),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	return c.Do(req)
}
