package snowflake

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"google.golang.org/grpc/codes"
)

type ProgrammaticAccessToken struct {
	Name      string
	ExpiresAt time.Time
}

func (c *Client) RoleGrantedToUser(ctx context.Context, userName, roleName string) (bool, error) {
	result, err := c.executeStatement(ctx, fmt.Sprintf("SHOW GRANTS TO USER %s;", quoteIdentifier(userName)))
	if err != nil {
		return false, err
	}
	for _, row := range result.Data {
		grantedOn, err := result.ResultSetMetadata.GetStringValueFromRow(row, "granted_on")
		if err != nil {
			return false, fmt.Errorf("snowflake: read user role grant type: %w", err)
		}
		name, err := result.ResultSetMetadata.GetStringValueFromRow(row, "name")
		if err != nil {
			return false, fmt.Errorf("snowflake: read user role grant name: %w", err)
		}
		if strings.EqualFold(grantedOn, "ROLE") && name == roleName {
			return true, nil
		}
	}
	return false, nil
}

func (c *Client) ListProgrammaticAccessTokens(ctx context.Context, userName string) ([]ProgrammaticAccessToken, error) {
	result, err := c.executeStatement(ctx, fmt.Sprintf("SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER %s;", quoteIdentifier(userName)))
	if err != nil {
		return nil, err
	}
	tokens := make([]ProgrammaticAccessToken, 0, len(result.Data))
	for _, row := range result.Data {
		name, err := result.ResultSetMetadata.GetStringValueFromRow(row, "name")
		if err != nil {
			return nil, fmt.Errorf("snowflake: read programmatic access token name: %w", err)
		}
		expiresAt, err := result.ResultSetMetadata.GetTimeValueFromRow(row, "expires_at")
		if err != nil {
			return nil, fmt.Errorf("snowflake: read programmatic access token expiry: %w", err)
		}
		tokens = append(tokens, ProgrammaticAccessToken{Name: name, ExpiresAt: expiresAt})
	}
	return tokens, nil
}

// CreateProgrammaticAccessToken creates a token and returns the secret supplied
// by Snowflake in the one response where it is available. It never logs it.
func (c *Client) CreateProgrammaticAccessToken(ctx context.Context, userName, tokenName, roleRestriction string, daysToExpiry int) (string, error) {
	if daysToExpiry < 1 {
		return "", fmt.Errorf("snowflake: days to expiry must be at least one")
	}
	roleClause := ""
	if roleRestriction != "" {
		roleClause = fmt.Sprintf(" ROLE_RESTRICTION = %s", quoteIdentifier(roleRestriction))
	}
	statement := fmt.Sprintf(
		"ALTER USER %s ADD PROGRAMMATIC ACCESS TOKEN %s%s DAYS_TO_EXPIRY = %d;",
		quoteIdentifier(userName), quoteIdentifier(tokenName), roleClause, daysToExpiry,
	)
	result, err := c.executeStatement(ctx, statement)
	if err != nil {
		return "", err
	}
	if len(result.Data) != 1 {
		return "", fmt.Errorf("snowflake: programmatic access token response did not return exactly one row")
	}
	// By column name, not position: a reordered or inserted column would otherwise
	// return some other field as the credential instead of failing.
	secret, err := result.ResultSetMetadata.GetStringValueFromRow(result.Data[0], "token_secret")
	if err != nil {
		return "", fmt.Errorf("snowflake: read programmatic access token secret: %w", err)
	}
	if secret == "" {
		return "", fmt.Errorf("snowflake: programmatic access token response did not include token_secret")
	}
	return secret, nil
}

func (c *Client) RemoveProgrammaticAccessToken(ctx context.Context, userName, tokenName string) error {
	statement := fmt.Sprintf(
		"ALTER USER IF EXISTS %s REMOVE PROGRAMMATIC ACCESS TOKEN IF EXISTS %s;",
		quoteIdentifier(userName), quoteIdentifier(tokenName),
	)
	_, err := c.executeStatement(ctx, statement)
	return err
}

func (c *Client) executeStatement(ctx context.Context, statement string) (*StatementsApiResponseBase, error) {
	req, err := c.PostStatementRequest(ctx, []string{statement})
	if err != nil {
		return nil, err
	}
	var result StatementsApiResponseBase
	var apiErr SnowflakeError
	resp, err := c.Do(req, uhttp.WithJSONResponse(&result), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp)
	if err != nil {
		return nil, classifyStatementError(resp, &apiErr, err)
	}
	if result.StatementHandle == "" {
		return &result, nil
	}
	req, err = c.GetStatementResponse(ctx, result.StatementHandle)
	if err != nil {
		return nil, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&result), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp)
	if err != nil {
		// Same classification as the POST leg: Snowflake reports an access-control
		// denial on whichever leg surfaces the statement's outcome, and a statement
		// that went async reports it here.
		return nil, classifyStatementError(resp, &apiErr, err)
	}
	return &result, nil
}

// classifyStatementError joins ErrInsufficientPrivileges when Snowflake refused the statement
// because the connector's role may not observe the object, so callers can skip it with
// IsInsufficientPrivileges instead of failing the whole sync. Every other error is unchanged.
func classifyStatementError(resp *http.Response, apiErr *SnowflakeError, err error) error {
	if isAccessControlDenial(resp, apiErr) {
		return uhttp.WrapErrors(
			codes.PermissionDenied,
			"baton-snowflake: insufficient privileges to run statement",
			ErrInsufficientPrivileges, err,
		)
	}
	return dedupeAPIError(err)
}

func quoteIdentifier(identifier string) string {
	return "\"" + escapeDoubleQuotedIdentifier(identifier) + "\""
}
