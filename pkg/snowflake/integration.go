package snowflake

import (
	"context"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
)

var integrationStructFieldToColumnMap = map[string]string{
	structFieldName:     columnName,
	structFieldType:     columnType,
	structFieldCategory: columnCategory,
	structFieldComment:  columnComment,
}

type (
	// Integration is an account-level Snowflake integration as returned by
	// SHOW INTEGRATIONS. Category is one of SECURITY, STORAGE, API, NOTIFICATION,
	// EXTERNAL ACCESS, CATALOG; Type carries the finer kind (e.g. EXTERNAL_OAUTH).
	Integration struct {
		Name     string
		Type     string
		Category string
		Comment  string
	}
	ListIntegrationsRawResponse struct {
		StatementsApiResponseBase
	}
)

func (i *Integration) GetColumnName(fieldName string) string {
	return integrationStructFieldToColumnMap[fieldName]
}

func (r *ListIntegrationsRawResponse) GetIntegrations() ([]Integration, error) {
	var integrations []Integration
	for _, row := range r.Data {
		integration := &Integration{}
		if err := r.ResultSetMetadata.ParseRow(integration, row); err != nil {
			return nil, err
		}

		integrations = append(integrations, *integration)
	}
	return integrations, nil
}

// ListIntegrations enumerates account-level integrations via SHOW INTEGRATIONS.
// SHOW INTEGRATIONS returns only integrations the current role has been granted
// at least one privilege on (USAGE/OWNERSHIP); a role holding MANAGE GRANTS
// (e.g. ACCOUNTADMIN/SECURITYADMIN) sees every integration in the account. Under
// a restricted role the result set is simply smaller rather than an error.
func (c *Client) ListIntegrations(ctx context.Context) ([]Integration, error) {
	queries := []string{"SHOW INTEGRATIONS;"}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListIntegrationsRawResponse
	var apiErr SnowflakeError
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp1)
	if err != nil {
		if isAccessControlDenial(resp1, &apiErr) {
			return nil, uhttp.WrapErrors(
				codes.PermissionDenied,
				"baton-snowflake: insufficient privileges to list integrations",
				ErrInsufficientPrivileges, err,
			)
		}
		return nil, dedupeAPIError(err)
	}

	l := ctxzap.Extract(ctx)
	l.Debug("ListIntegrations", zap.String("response.code", response.Code), zap.String("response.message", response.Message))

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp2)
	if err != nil {
		if isAccessControlDenial(resp2, &apiErr) {
			return nil, uhttp.WrapErrors(
				codes.PermissionDenied,
				"baton-snowflake: insufficient privileges to list integrations (statement result)",
				ErrInsufficientPrivileges, err,
			)
		}
		return nil, dedupeAPIError(err)
	}

	integrations, err := response.GetIntegrations()
	if err != nil {
		return nil, err
	}

	return integrations, nil
}
