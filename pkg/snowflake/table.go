package snowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

var tableStructFieldToColumnMap = map[string]string{
	"CreatedOn":    "created_on",
	"Name":         "name",
	"SchemaName":   "schema_name",
	"DatabaseName": "database_name",
	"Kind":         "kind",
	"Comment":      "comment",
	"Owner":        "owner",
}

type (
	Table struct {
		CreatedOn    time.Time
		Name         string
		SchemaName   string
		DatabaseName string
		Kind         string
		Comment      string
		Owner        string
	}

	ListTablesRawResponse struct {
		StatementsApiResponseBase
	}
)

func (t *Table) GetColumnName(fieldName string) string {
	return tableStructFieldToColumnMap[fieldName]
}

func (r *ListTablesRawResponse) ListTables() ([]Table, error) {
	var tables []Table
	for _, row := range r.Data {
		table := &Table{}
		if err := r.ResultSetMetadata.ParseRow(table, row); err != nil {
			return nil, err
		}

		tables = append(tables, *table)
	}
	return tables, nil
}

const tableListCursorSep = "\x00"

func (c *Client) ListTablesInAccount(ctx context.Context, cursor string, limit int) ([]Table, string, *http.Response, error) {
	l := ctxzap.Extract(ctx)

	var q string
	if cursor != "" {
		// FROM expects a name_string; use fully qualified to avoid duplicates across account
		parts := strings.SplitN(cursor, tableListCursorSep, 3)
		if len(parts) >= 3 {
			fromName := escapeSingleQuote(parts[0] + "." + parts[1] + "." + parts[2])
			q = fmt.Sprintf("SHOW TABLES IN ACCOUNT LIMIT %d FROM '%s';", limit, fromName)
		} else {
			q = fmt.Sprintf("SHOW TABLES IN ACCOUNT LIMIT %d;", limit)
		}
	} else {
		q = fmt.Sprintf("SHOW TABLES IN ACCOUNT LIMIT %d;", limit)
	}
	queries := []string{q}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, "", nil, err
	}

	var response ListTablesRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity {
			l.Debug("Insufficient privileges for SHOW TABLES IN ACCOUNT")
			wrappedErr := fmt.Errorf("baton-snowflake: insufficient privileges for SHOW TABLES IN ACCOUNT: %w", err)
			return nil, "", nil, status.Error(codes.PermissionDenied, wrappedErr.Error())
		}
		return nil, "", nil, err
	}
	if resp != nil {
		defer resp.Body.Close()
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, "", resp, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity {
			l.Debug("Insufficient privileges for SHOW TABLES IN ACCOUNT (statement result)")
			wrappedErr := fmt.Errorf("baton-snowflake: insufficient privileges for SHOW TABLES IN ACCOUNT (statement result): %w", err)
			return nil, "", nil, status.Error(codes.PermissionDenied, wrappedErr.Error())
		}
		return nil, "", resp, err
	}
	if resp != nil {
		defer resp.Body.Close()
	}

	tables, err := response.ListTables()
	if err != nil {
		return nil, "", resp, err
	}

	var nextCursor string
	if len(tables) >= limit {
		last := tables[len(tables)-1]
		nextCursor = last.DatabaseName + tableListCursorSep + last.SchemaName + tableListCursorSep + last.Name
	}
	return tables, nextCursor, resp, nil
}

// escapeSingleQuote doubles single quotes for use inside SQL string literals.
func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// escapeLikePattern escapes a string for safe use in a Snowflake LIKE pattern (exact match).
// Escapes: \ (escape char), ', %, _.
func escapeLikePattern(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "'", "''")
	s = strings.ReplaceAll(s, "%", `\%`)
	s = strings.ReplaceAll(s, "_", `\_`)
	return s
}

// escapeDoubleQuotedIdentifier escapes a string for use inside Snowflake double-quoted identifiers.
// Double quotes inside the identifier must be escaped by doubling them ("").
func escapeDoubleQuotedIdentifier(s string) string {
	return strings.ReplaceAll(s, `"`, `""`)
}

func (c *Client) GetTable(ctx context.Context, database, schema, tableName string) (*Table, *http.Response, error) {
	likePattern := escapeLikePattern(tableName)
	queries := []string{
		fmt.Sprintf("SHOW TABLES LIKE '%s' ESCAPE '\\' IN SCHEMA \"%s\".\"%s\" LIMIT 1;", likePattern, escapeDoubleQuotedIdentifier(database), escapeDoubleQuotedIdentifier(schema)),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, nil, err
	}

	var response ListTablesRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity {
			return nil, resp, nil
		}
		return nil, nil, err
	}
	if resp != nil {
		defer resp.Body.Close()
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, resp, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, resp, err
	}
	if resp != nil {
		defer resp.Body.Close()
	}

	tables, err := response.ListTables()
	if err != nil {
		return nil, resp, err
	}

	// Filter by exact match (database, schema, and name)
	for _, table := range tables {
		if table.DatabaseName == database && table.SchemaName == schema && table.Name == tableName {
			return &table, resp, nil
		}
	}

	return nil, resp, fmt.Errorf("table %s.%s.%s not found", database, schema, tableName)
}

var tableGrantStructFieldToColumnMap = map[string]string{
	"CreatedOn":   "created_on",
	"Privilege":   "privilege",
	"GrantedOn":   "granted_on",
	"Name":        "name",
	"GrantedTo":   "granted_to",
	"GranteeName": "grantee_name",
	"GrantOption": "grant_option",
	"GrantedBy":   "granted_by",
}

type (
	TableGrant struct {
		CreatedOn   time.Time
		Privilege   string
		GrantedOn   string
		Name        string
		GrantedTo   string
		GranteeName string
		GrantOption string
		GrantedBy   string
	}

	ListTableGrantsRawResponse struct {
		StatementsApiResponseBase
	}
)

func (tg *TableGrant) GetColumnName(fieldName string) string {
	return tableGrantStructFieldToColumnMap[fieldName]
}

func (r *ListTableGrantsRawResponse) GetTableGrants() ([]TableGrant, error) {
	var grants []TableGrant
	for _, row := range r.Data {
		grant := &TableGrant{}
		if err := r.ResultSetMetadata.ParseRow(grant, row); err != nil {
			return nil, err
		}

		grants = append(grants, *grant)
	}
	return grants, nil
}

// ListTableGrants uses objectKind to run SHOW GRANTS ON TABLE or ON VIEW (Snowflake requires the correct type).
func (c *Client) ListTableGrants(ctx context.Context, database, schema, tableName, objectKind string) ([]TableGrant, error) {
	l := ctxzap.Extract(ctx)
	objectType := "TABLE"
	if strings.EqualFold(objectKind, "VIEW") {
		objectType = "VIEW"
	}
	queries := []string{
		fmt.Sprintf("SHOW GRANTS ON %s \"%s\".\"%s\".\"%s\";", objectType, escapeDoubleQuotedIdentifier(database), escapeDoubleQuotedIdentifier(schema), escapeDoubleQuotedIdentifier(tableName)),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListTableGrantsRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity {
			var errMsg struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}

			decodeErr := json.NewDecoder(resp.Body).Decode(&errMsg)
			if decodeErr != nil {
				return nil, fmt.Errorf("received 422 but failed to decode response body: %w (request error: %s)", decodeErr, err.Error())
			}

			// code: 003001
			// message: SQL access control error:\nInsufficient privileges
			tableRef := fmt.Sprintf("%s.%s.%s", database, schema, tableName)
			if errMsg.Code == "003001" {
				l.Debug("Insufficient privileges to show grants on table", zap.String("table", tableRef))
			} else {
				l.Error(errMsg.Message, zap.String("table", tableRef))
			}

			return nil, status.Errorf(codes.PermissionDenied, "baton-snowflake: insufficient privileges to show grants on table %s: %s", tableRef, errMsg.Message)
		}

		return nil, err
	}
	if resp != nil {
		defer resp.Body.Close()
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity {
			l.Debug("Insufficient privileges to show grants on table (statement result)", zap.String("table", fmt.Sprintf("%s.%s.%s", database, schema, tableName)))
			wrappedErr := fmt.Errorf("baton-snowflake: insufficient privileges to show grants on table %s.%s.%s (statement result): %w", database, schema, tableName, err)
			return nil, status.Error(codes.PermissionDenied, wrappedErr.Error())
		}
		return nil, err
	}
	if resp != nil {
		defer resp.Body.Close()
	}

	grants, err := response.GetTableGrants()
	if err != nil {
		return nil, err
	}

	return grants, nil
}
