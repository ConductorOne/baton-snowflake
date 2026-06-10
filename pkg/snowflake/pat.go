package snowflake

import (
	"context"
	"fmt"
	"time"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// patStructFieldToColumnMap maps ProgrammaticAccessToken field names to the
// column names returned by SHOW USER PROGRAMMATIC ACCESS TOKENS.
// Source: https://docs.snowflake.com/en/sql-reference/sql/show-user-programmatic-access-tokens
var patStructFieldToColumnMap = map[string]string{
	"Name":            "name",
	"UserName":        "user_name",
	"RoleRestriction": "role_restriction",
	"ExpiresAt":       "expires_at",
	"Status":          "status",
	"Comment":         "comment",
	"CreatedOn":       "created_on",
	"CreatedBy":       "created_by",
}

// ProgrammaticAccessToken holds the metadata returned by
// SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER.
// The token secret value is never returned by Snowflake.
// Source: https://docs.snowflake.com/en/sql-reference/sql/show-user-programmatic-access-tokens
type ProgrammaticAccessToken struct {
	Name            string
	UserName        string
	RoleRestriction string
	ExpiresAt       time.Time
	Status          string
	Comment         string
	CreatedOn       time.Time
	CreatedBy       string
}

// GetColumnName implements Parsable.
func (p *ProgrammaticAccessToken) GetColumnName(fieldName string) string {
	return patStructFieldToColumnMap[fieldName]
}

// ListPATsRawResponse wraps the Snowflake Statements API response for
// SHOW USER PROGRAMMATIC ACCESS TOKENS.
type ListPATsRawResponse struct {
	StatementsApiResponseBase
}

func (r *ListPATsRawResponse) ListPATs() ([]ProgrammaticAccessToken, error) {
	var pats []ProgrammaticAccessToken
	for _, row := range r.Data {
		pat := &ProgrammaticAccessToken{}
		if err := r.ResultSetMetadata.ParseRow(pat, row); err != nil {
			return nil, err
		}
		pats = append(pats, *pat)
	}
	return pats, nil
}

// ListProgrammaticAccessTokens issues SHOW USER PROGRAMMATIC ACCESS TOKENS
// FOR USER and returns all PATs for the given Snowflake user.
//
// Required privilege: MODIFY on the user object (USERADMIN / SECURITYADMIN
// satisfy this transitively).
// Source: https://docs.snowflake.com/en/sql-reference/sql/show-user-programmatic-access-tokens
//
// The token secret value is never returned — only metadata is enumerable.
func (c *Client) ListProgrammaticAccessTokens(ctx context.Context, username string) ([]ProgrammaticAccessToken, error) {
	l := ctxzap.Extract(ctx)

	escapedUsername := escapeDoubleQuotedIdentifier(username)
	queries := []string{
		fmt.Sprintf(`SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER "%s";`, escapedUsername),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListPATsRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp)
	if err != nil {
		statusCode := 0
		if resp != nil {
			statusCode = resp.StatusCode
		}
		if IsUnprocessableEntity(statusCode, err) {
			// MODIFY privilege not held for this user — skip silently.
			l.Debug("insufficient privileges for PAT enumeration; skipping user",
				zap.String("username", username), zap.Error(err))
			return nil, nil
		}
		return nil, err
	}

	return response.ListPATs()
}
