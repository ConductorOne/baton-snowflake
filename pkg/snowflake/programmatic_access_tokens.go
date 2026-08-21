package snowflake

import (
	"context"
	"fmt"
	"time"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

type ProgrammaticAccessToken struct {
	Name      string
	ExpiresAt time.Time
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
func (c *Client) CreateProgrammaticAccessToken(ctx context.Context, userName, tokenName string, daysToExpiry int) (string, error) {
	if daysToExpiry < 1 {
		return "", fmt.Errorf("snowflake: days to expiry must be at least one")
	}
	statement := fmt.Sprintf(
		"ALTER USER %s ADD PROGRAMMATIC ACCESS TOKEN %s DAYS_TO_EXPIRY = %d;",
		quoteIdentifier(userName), quoteIdentifier(tokenName), daysToExpiry,
	)
	result, err := c.executeStatement(ctx, statement)
	if err != nil {
		return "", err
	}
	if len(result.Data) != 1 || len(result.Data[0]) < 2 || result.Data[0][1] == "" {
		return "", fmt.Errorf("snowflake: programmatic access token response did not include token_secret")
	}
	return result.Data[0][1], nil
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
		return nil, dedupeAPIError(err)
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
		return nil, dedupeAPIError(err)
	}
	return &result, nil
}

func quoteIdentifier(identifier string) string {
	return "\"" + escapeDoubleQuotedIdentifier(identifier) + "\""
}
