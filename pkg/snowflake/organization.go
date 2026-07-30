package snowflake

import (
	"context"
	"strconv"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

// organizationAccountStructFieldToColumnMap maps OrganizationAccount fields to the
// SHOW ORGANIZATION ACCOUNTS columns the connector consumes. Only columns that are
// always present are listed: ParseRow requires every mapped column to exist in the
// response, and several SHOW ORGANIZATION ACCOUNTS columns are conditional. In
// particular region_group is absent for organizations that do not span multiple
// region groups, which previously aborted the whole sync (CXH-2093). Mapping only
// the columns actually used keeps parsing resilient to that conditional layout.
var organizationAccountStructFieldToColumnMap = map[string]string{
	"Edition":        "edition",
	"AccountLocator": "account_locator",
}

type (
	OrganizationAccount struct {
		Edition        string
		AccountLocator string
	}
	ListOrganizationAccountsRawResponse struct {
		StatementsApiResponseBase
	}
)

func (o *OrganizationAccount) GetColumnName(fieldName string) string {
	return organizationAccountStructFieldToColumnMap[fieldName]
}

func (r *ListOrganizationAccountsRawResponse) GetOrganizationAccounts() ([]OrganizationAccount, error) {
	var accounts []OrganizationAccount
	for _, row := range r.Data {
		account := &OrganizationAccount{}
		if err := r.ResultSetMetadata.ParseRow(account, row); err != nil {
			return nil, err
		}
		accounts = append(accounts, *account)
	}
	return accounts, nil
}

func (c *Client) ListOrganizationAccounts(ctx context.Context) ([]OrganizationAccount, int, error) {
	queries := []string{"SHOW ORGANIZATION ACCOUNTS;"}

	req, err := c.PostStatementRequestWithRole(ctx, queries, GlobalOrgAdminRole)
	if err != nil {
		return nil, 0, err
	}

	var response ListOrganizationAccountsRawResponse
	var apiErr SnowflakeError
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp1)
	if err != nil {
		statusCode := 0
		if resp1 != nil {
			statusCode = resp1.StatusCode
		}
		return nil, statusCode, dedupeAPIError(err)
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, 0, err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp2)
	if err != nil {
		statusCode := 0
		if resp2 != nil {
			statusCode = resp2.StatusCode
		}
		return nil, statusCode, dedupeAPIError(err)
	}

	accounts, err := response.GetOrganizationAccounts()
	if err != nil {
		return nil, resp2.StatusCode, err
	}

	return accounts, resp2.StatusCode, nil
}

func (c *Client) CountUsers(ctx context.Context) (int64, error) {
	queries := []string{"SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.USERS WHERE DELETED_ON IS NULL;"}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return 0, err
	}

	var response StatementsApiResponseBase
	var apiErr SnowflakeError
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp1)
	if err != nil {
		return 0, dedupeAPIError(err)
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return 0, err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp2)
	if err != nil {
		return 0, dedupeAPIError(err)
	}

	if len(response.Data) == 0 || len(response.Data[0]) == 0 {
		return 0, nil
	}

	count, err := strconv.ParseInt(response.Data[0][0], 10, 64)
	if err != nil {
		return 0, err
	}

	return count, nil
}
