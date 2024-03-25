package snowflake

import (
	"context"
	"fmt"
	"net/http"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

type (
	AccountRole struct {
		Name string
	}
	ListAccountRolesRawResponse struct {
		StatementsApiResponseBase
		Data [][]string `json:"data"`
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
)

func (r *ListAccountRolesRawResponse) GetAccountRoles() []AccountRole {
	var accountRoles []AccountRole
	for _, accountRole := range r.Data {
		accountRoles = append(accountRoles, AccountRole{
			Name: accountRole[1],
		})
	}
	return accountRoles
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

func (c *Client) ListAccountRoles(ctx context.Context, offset, limit int) ([]AccountRole, *http.Response, error) {
	queries := []string{
		"SHOW ROLES;",
		c.paginateLastQuery(offset, limit),
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

	req, err = c.GetStatementResponse(ctx, response.StatementHandles[1])
	if err != nil {
		return nil, resp, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, resp, err
	}

	return response.GetAccountRoles(), resp, nil
}

func (c *Client) ListRoleGrantees(ctx context.Context, roleName string, offset, limit int) ([]AccountRoleGrantee, *http.Response, error) {
	queries := []string{
		fmt.Sprintf("SHOW GRANTS OF ROLE %s;", roleName),
		c.paginateLastQuery(offset, limit),
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

	req, err = c.GetStatementResponse(ctx, response.StatementHandles[1])
	if err != nil {
		return nil, resp, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, resp, err
	}

	return response.GetAccountRoleGrantees(), resp, nil
}
