package snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

// ErrObjectNotFound is returned when Snowflake reports a target object does not
// exist or cannot be seen, and the cause is not an RBAC privilege error (code
// 003001). Callers should treat this as a soft-skip condition.
var ErrObjectNotFound = errors.New("baton-snowflake: object does not exist or not authorized")

var schemaStructFieldToColumnMap = map[string]string{
	structFieldName:         columnName,
	structFieldDatabaseName: columnDatabaseName,
}

type (
	Schema struct {
		Name         string
		DatabaseName string
	}

	ListSchemasRawResponse struct {
		StatementsApiResponseBase
	}
)

func (s *Schema) GetColumnName(fieldName string) string {
	return schemaStructFieldToColumnMap[fieldName]
}

func (r *ListSchemasRawResponse) ListSchemas() ([]Schema, error) {
	var schemas []Schema
	for _, row := range r.Data {
		schema := &Schema{}
		if err := r.ResultSetMetadata.ParseRow(schema, row); err != nil {
			return nil, err
		}
		schemas = append(schemas, *schema)
	}
	return schemas, nil
}

func (c *Client) ListSchemasInDatabase(ctx context.Context, databaseName string) ([]Schema, error) {
	l := ctxzap.Extract(ctx)

	escapedDB := escapeDoubleQuotedIdentifier(databaseName)
	queries := []string{
		fmt.Sprintf("SHOW SCHEMAS IN DATABASE \"%s\";", escapedDB),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListSchemasRawResponse
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp1)
	if err != nil {
		// All 422s returned as PermissionDenied. Callers in tables.go propagate this
		// error via wrapError — there is no soft-skip at the List level, so returning
		// ErrObjectNotFound here would have no observable effect on sync behavior.
		if resp1 != nil && resp1.StatusCode == http.StatusUnprocessableEntity {
			l.Debug("Insufficient privileges for SHOW SCHEMAS IN DATABASE", zap.String("database", databaseName))
			wrappedErr := fmt.Errorf("baton-snowflake: insufficient privileges for SHOW SCHEMAS IN DATABASE %s: %w", databaseName, err)
			return nil, status.Error(codes.PermissionDenied, wrappedErr.Error())
		}
		return nil, err
	}

	// Inline async poll: unlike fetchStatementResultIfAsync, this path also wraps 422 as
	// PermissionDenied. That extra error handling is why this function does not call the
	// shared helper.
	if resp1.StatusCode == http.StatusAccepted {
		req, err = c.GetStatementResponse(ctx, response.StatementHandle)
		if err != nil {
			return nil, err
		}
		resp2, err := c.Do(req, uhttp.WithJSONResponse(&response))
		defer closeResponseBody(resp2)
		if err != nil {
			// All 422s returned as PermissionDenied. Callers in tables.go propagate this
			// error via wrapError — there is no soft-skip at the List level, so returning
			// ErrObjectNotFound here would have no observable effect on sync behavior.
			if resp2 != nil && resp2.StatusCode == http.StatusUnprocessableEntity {
				l.Debug("Insufficient privileges for SHOW SCHEMAS IN DATABASE (statement result)", zap.String("database", databaseName))
				wrappedErr := fmt.Errorf("baton-snowflake: insufficient privileges for SHOW SCHEMAS IN DATABASE %s (statement result): %w", databaseName, err)
				return nil, status.Error(codes.PermissionDenied, wrappedErr.Error())
			}
			return nil, err
		}
	}

	return response.ListSchemas()
}

var tableStructFieldToColumnMap = map[string]string{
	structFieldCreatedOn:    columnCreatedOn,
	structFieldName:         columnName,
	"SchemaName":            "schema_name",
	structFieldDatabaseName: columnDatabaseName,
	"Kind":                  "kind",
	structFieldComment:      columnComment,
	structFieldOwner:        columnOwner,
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

func (c *Client) ListTablesInSchema(ctx context.Context, databaseName, schemaName string, cursor string, limit int) ([]Table, string, error) {
	l := ctxzap.Extract(ctx)

	escapedDB := escapeDoubleQuotedIdentifier(databaseName)
	escapedSchema := escapeDoubleQuotedIdentifier(schemaName)
	var q string
	if cursor != "" {
		q = fmt.Sprintf("SHOW TABLES IN SCHEMA \"%s\".\"%s\" LIMIT %d FROM '%s';", escapedDB, escapedSchema, limit, escapeSingleQuote(cursor))
	} else {
		q = fmt.Sprintf("SHOW TABLES IN SCHEMA \"%s\".\"%s\" LIMIT %d;", escapedDB, escapedSchema, limit)
	}
	queries := []string{q}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, "", err
	}

	var response ListTablesRawResponse
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp1)
	if err != nil {
		// All 422s returned as PermissionDenied. Callers in tables.go propagate this
		// error via wrapError — there is no soft-skip at the List level, so returning
		// ErrObjectNotFound here would have no observable effect on sync behavior.
		if resp1 != nil && resp1.StatusCode == http.StatusUnprocessableEntity {
			l.Debug("Insufficient privileges for SHOW TABLES IN SCHEMA",
				zap.String("database", databaseName), zap.String("schema", schemaName))
			wrappedErr := fmt.Errorf("baton-snowflake: insufficient privileges for SHOW TABLES IN SCHEMA %s.%s: %w", databaseName, schemaName, err)
			return nil, "", status.Error(codes.PermissionDenied, wrappedErr.Error())
		}
		return nil, "", err
	}

	// Inline async poll: see ListSchemasInDatabase for why this does not use fetchStatementResultIfAsync.
	if resp1.StatusCode == http.StatusAccepted {
		req, err = c.GetStatementResponse(ctx, response.StatementHandle)
		if err != nil {
			return nil, "", err
		}
		resp2, err := c.Do(req, uhttp.WithJSONResponse(&response))
		defer closeResponseBody(resp2)
		if err != nil {
			// All 422s returned as PermissionDenied. Callers in tables.go propagate this
			// error via wrapError — there is no soft-skip at the List level, so returning
			// ErrObjectNotFound here would have no observable effect on sync behavior.
			if resp2 != nil && resp2.StatusCode == http.StatusUnprocessableEntity {
				l.Debug("Insufficient privileges for SHOW TABLES IN SCHEMA (statement result)",
					zap.String("database", databaseName), zap.String("schema", schemaName))
				wrappedErr := fmt.Errorf("baton-snowflake: insufficient privileges for SHOW TABLES IN SCHEMA %s.%s (statement result): %w", databaseName, schemaName, err)
				return nil, "", status.Error(codes.PermissionDenied, wrappedErr.Error())
			}
			return nil, "", err
		}
	}

	tables, err := response.ListTables()
	if err != nil {
		return nil, "", err
	}

	var nextCursor string
	// >= not >: if exactly limit rows returned, there may be more beyond the cursor.
	// Fewer than limit means this is definitively the last page.
	if limit > 0 && len(tables) >= limit {
		last := tables[len(tables)-1]
		nextCursor = last.Name
	}
	return tables, nextCursor, nil
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

func (c *Client) GetTable(ctx context.Context, database, schema, tableName string) (*Table, error) {
	likePattern := escapeLikePattern(tableName)
	queries := []string{
		fmt.Sprintf("SHOW TABLES LIKE '%s' ESCAPE '\\' IN SCHEMA \"%s\".\"%s\" LIMIT 1;", likePattern, escapeDoubleQuotedIdentifier(database), escapeDoubleQuotedIdentifier(schema)),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListTablesRawResponse
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp1)
	if err != nil {
		if resp1 != nil && resp1.StatusCode == http.StatusUnprocessableEntity {
			// Any 422 is treated as object-not-found rather than decoding for code 003001.
			// GetTable is only called as an owner fallback after ListTableGrants already succeeded,
			// so a genuine RBAC denial here is extremely unlikely within the same request cycle.
			// Even if it occurred, ErrObjectNotFound causes the caller to return partial grants
			// with a Warn log — a tolerable degradation, not a silent failure.
			// Note: the async path via fetchStatementResultIfAsync also has no 422 handling;
			// both paths uniformly soft-skip on any 422 here.
			return nil, ErrObjectNotFound
		}
		return nil, err
	}

	if err := c.fetchStatementResultIfAsync(ctx, resp1, response.StatementHandle, &response); err != nil {
		return nil, err
	}

	tables, err := response.ListTables()
	if err != nil {
		return nil, err
	}

	// Filter by exact match (database, schema, and name)
	for _, table := range tables {
		if table.DatabaseName == database && table.SchemaName == schema && table.Name == tableName {
			return &table, nil
		}
	}

	return nil, fmt.Errorf("%w: %s.%s.%s", ErrObjectNotFound, database, schema, tableName)
}

var tableGrantStructFieldToColumnMap = map[string]string{
	structFieldCreatedOn: columnCreatedOn,
	"Privilege":          "privilege",
	"GrantedOn":          "granted_on",
	structFieldName:      columnName,
	"GrantedTo":          "granted_to",
	"GranteeName":        "grantee_name",
	"GrantOption":        "grant_option",
	"GrantedBy":          "granted_by",
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

func tableGrantsCacheKey(database, schema, tableName, objectKind string) string {
	kind := "TABLE"
	if strings.EqualFold(objectKind, "VIEW") {
		kind = "VIEW"
	}
	return fmt.Sprintf("%s|%s|%s|%s", database, schema, tableName, kind)
}

// ListTableGrants uses objectKind to run SHOW GRANTS ON TABLE or ON VIEW (Snowflake requires the correct type).
func (c *Client) ListTableGrants(ctx context.Context, ss sessions.SessionStore, database, schema, tableName, objectKind string) ([]TableGrant, error) {
	cacheKey := tableGrantsCacheKey(database, schema, tableName, objectKind)
	if ss != nil {
		cached, found, err := session.GetJSON[[]TableGrant](ctx, ss, cacheKey, tableGrantsNamespace)
		if err != nil {
			ctxzap.Extract(ctx).Debug("table grants cache lookup error, falling through to API",
				zap.String("cache_key", cacheKey), zap.Error(err))
		} else if found {
			return cached, nil
		}
	}

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

			tableRef := fmt.Sprintf("%s.%s.%s", database, schema, tableName)
			if errMsg.Code == "003001" {
				// Genuine RBAC/privilege problem — keep as PermissionDenied so sync fails loudly
				l.Debug("Insufficient privileges to show grants on table", zap.String("table", tableRef))
				return nil, status.Errorf(codes.PermissionDenied, "baton-snowflake: insufficient privileges to show grants on table %s: %s", tableRef, errMsg.Message)
			}
			// Any other 422: object dropped mid-sync or otherwise no longer accessible
			l.Warn("Table no longer exists or not accessible (will soft-skip)",
				zap.String("table", tableRef),
				zap.String("snowflake_code", errMsg.Code),
				zap.String("message", errMsg.Message))
			return nil, ErrObjectNotFound
		}

		return nil, err
	}
	// Close POST response body explicitly — resp is reassigned below for the GET.
	closeResponseBody(resp)

	// closeResponseBody drains the body but does not nil resp; StatusCode is still readable.
	if resp != nil && resp.StatusCode == http.StatusAccepted {
		req, err = c.GetStatementResponse(ctx, response.StatementHandle)
		if err != nil {
			return nil, err
		}
		resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
		defer closeResponseBody(resp)
		if err != nil {
			if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity {
				tableRef := fmt.Sprintf("%s.%s.%s", database, schema, tableName)
				var errMsg struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				}
				// resp.Body is safe to read: uhttp.Do replaces the raw stream with
				// bytes.NewBuffer(body) before returning, so WithJSONResponse and this
				// decode draw from independent copies of the same bytes.
				decodeErr := json.NewDecoder(resp.Body).Decode(&errMsg)
				if decodeErr != nil {
					l.Warn("Failed to decode 422 response body on async poll, treating as object-not-found",
						zap.String("table", tableRef), zap.Error(decodeErr))
				} else if errMsg.Code == "003001" {
					l.Debug("Insufficient privileges to show grants on table (statement result)", zap.String("table", tableRef))
					return nil, status.Errorf(codes.PermissionDenied, "baton-snowflake: insufficient privileges to show grants on table %s: %s", tableRef, errMsg.Message)
				}
				l.Warn("Table no longer exists during statement result fetch (will soft-skip)", zap.String("table", tableRef))
				return nil, ErrObjectNotFound
			}
			return nil, err
		}
	}

	grants, err := response.GetTableGrants()
	if err != nil {
		return nil, err
	}

	if ss != nil {
		_ = session.SetJSON(ctx, ss, cacheKey, grants, tableGrantsNamespace)
	}

	return grants, nil
}
