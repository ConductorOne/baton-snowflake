package snowflake

import (
	"context"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

const snowflakeDateFormat = "2006-01-02 15:04:05.999"

func (c *Client) ListSecrets(ctx context.Context, database string) ([]Secret, error) {
	l := ctxzap.Extract(ctx)

	queries := []string{
		fmt.Sprintf("SHOW SECRETS IN DATABASE \"%s\";", escapeDoubleQuotedIdentifier(database)),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListSecretsRawResponse
	var apiErr SnowflakeError
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp)
	if err != nil {
		// Snowflake's SQL API answers statement failures with HTTP 422 + QueryFailureStatus.
		// Access-control denials (code 003001) and an unavailable shared database (the
		// publisher revoked the underlying share) both mean "nothing visible here"; every
		// other 422 (e.g. a genuine SQL compilation bug) must stay fatal. uhttp already
		// decoded the body into apiErr.
		if isAccessControlDenial(resp, &apiErr) {
			l.Debug("Insufficient privileges to show secrets in database", zap.String("database", database))
			return nil, nil
		}
		if isSharedDatabaseUnavailable(resp, &apiErr) {
			l.Debug("Shared database is no longer available, skipping secrets", zap.String("database", database))
			return nil, nil
		}
		return nil, dedupeAPIError(err)
	}

	secrets, err := response.ListSecrets()
	if err != nil {
		return nil, err
	}

	return secrets, nil
}

func (c *Client) UserRsa(ctx context.Context, username string) (*UserRsa, error) {
	queries := []string{
		fmt.Sprintf("DESCRIBE USER \"%s\";", escapeDoubleQuotedIdentifier(username)),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response RsaGetUserRawResponse
	var apiErr SnowflakeError
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp)
	if err != nil {
		// DESCRIBE USER requires MONITOR on the target user. Without it Snowflake answers 422
		// with QueryFailureStatus code 003001 - same access-control shape as SHOW SCHEMAS.
		if isAccessControlDenial(resp, &apiErr) {
			return nil, uhttp.WrapErrors(
				codes.PermissionDenied,
				fmt.Sprintf("baton-snowflake: insufficient privileges to describe user %s", username),
				ErrInsufficientPrivileges, err,
			)
		}
		return nil, dedupeAPIError(err)
	}

	secrets, err := response.GetUserRsa(ctx)
	if err != nil {
		return nil, err
	}

	return secrets, nil
}

func findUserDescriptionPropertyValue(properties []UserDescriptionProperty, name string) string {
	for _, property := range properties {
		if property.Property == name {
			return property.Value
		}
	}

	return ""
}

func (r *RsaGetUserRawResponse) GetUserRsa(ctx context.Context) (*UserRsa, error) {
	rsa := &UserRsa{}

	var userDescriptions []UserDescriptionProperty
	for _, row := range r.Data {
		description := &UserDescriptionProperty{}
		if err := r.ResultSetMetadata.ParseRow(description, row); err != nil {
			return nil, err
		}

		userDescriptions = append(userDescriptions, *description)
	}

	rsa.Username = findUserDescriptionPropertyValue(userDescriptions, "NAME")

	rsa1 := findUserDescriptionPropertyValue(userDescriptions, "RSA_PUBLIC_KEY_LAST_SET_TIME")
	if rsa1 != "" && rsa1 != rowNull {
		rsa1Time, err := time.Parse(snowflakeDateFormat, rsa1)
		if err != nil {
			return nil, err
		}
		rsa.RsaPublicKeyLastSetTime = &rsa1Time
	}

	rsa2 := findUserDescriptionPropertyValue(userDescriptions, "RSA_PUBLIC_KEY_2_LAST_SET_TIME")
	if rsa2 != "" && rsa2 != rowNull {
		rsa2Time, err := time.Parse(snowflakeDateFormat, rsa2)
		if err != nil {
			return nil, err
		}

		rsa.RsaPublicKeyLastSetTime2 = &rsa2Time
	}

	return rsa, nil
}
