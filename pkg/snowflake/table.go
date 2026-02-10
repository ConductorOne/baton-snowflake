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

// ListTables returns tables in the database.
func (c *Client) ListTables(ctx context.Context, database, cursor string, limit int) ([]Table, *http.Response, error) {
	l := ctxzap.Extract(ctx)

	var q string
	if limit <= 0 {
		q = fmt.Sprintf("SHOW TABLES IN DATABASE \"%s\";", database)
	} else {
		q = buildListTablesPaginatedQuery(database, cursor, limit)
	}
	queries := []string{q}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, nil, err
	}

	var response ListTablesRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity {
			var errMsg struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}

			err := json.NewDecoder(resp.Body).Decode(&errMsg)
			if err != nil {
				return nil, nil, err
			}

			// code: 003001
			// message: SQL access control error:\nInsufficient privileges to operate on database 'DB'
			if errMsg.Code == "003001" {
				l.Debug("Insufficient privileges to operate on database", zap.String("database", database))
			} else {
				l.Error(errMsg.Message, zap.String("database", database))
			}

			// Ignore if the account/role does not have permission to show tables of database
			return nil, nil, nil
		}

		return nil, nil, err
	}
	defer resp.Body.Close()

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, resp, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity {
			l.Debug("Insufficient privileges to operate on database (statement result)", zap.String("database", database))
			return nil, nil, nil
		}
		return nil, resp, err
	}
	defer resp.Body.Close()

	tables, err := response.ListTables()
	if err != nil {
		return nil, resp, err
	}

	return tables, resp, nil
}

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
			return nil, "", nil, nil
		}
		return nil, "", nil, err
	}
	defer resp.Body.Close()

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, "", resp, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity {
			l.Debug("Insufficient privileges for SHOW TABLES IN ACCOUNT (statement result)")
			return nil, "", nil, nil
		}
		return nil, "", resp, err
	}
	defer resp.Body.Close()

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

// buildListTablesPaginatedQuery builds SELECT from INFORMATION_SCHEMA with cursor (schema\x00name) and limit.
// Aliases match Table's GetColumnName so ParseRow works. Uses (schema, table_name) > (last_schema, last_name) for stable pagination.
func buildListTablesPaginatedQuery(database, cursor string, limit int) string {
	// Database as identifier in FROM; escape double quotes
	dbIdent := strings.ReplaceAll(database, `"`, `""`)
	// Database as string literal; Snowflake TABLE_CATALOG is often uppercase
	dbLiteral := escapeSingleQuote(database)
	// Filter table_type to match SHOW TABLES (base tables and views only; exclude temp/external/dynamic/etc).
	// Double-quoted aliases so Snowflake returns lowercase column names (ParseRow expects created_on, name, etc.)
	query := fmt.Sprintf(
		`SELECT created AS "created_on", table_name AS "name", table_schema AS "schema_name", `+
			`table_catalog AS "database_name", table_type AS "kind", comment AS "comment", table_owner AS "owner" `+
			`FROM "%s".information_schema.tables `+
			`WHERE UPPER(TRIM(table_catalog)) = UPPER('%s') AND table_type IN ('BASETABLE', 'VIEW')`,
		dbIdent, dbLiteral)
	if cursor != "" {
		parts := strings.SplitN(cursor, tableListCursorSep, 2)
		if len(parts) == 2 {
			lastSchema := escapeSingleQuote(parts[0])
			lastName := escapeSingleQuote(parts[1])
			query += fmt.Sprintf(" AND (table_schema, table_name) > ('%s', '%s')", lastSchema, lastName)
		}
	}
	query += fmt.Sprintf(" ORDER BY table_schema, table_name LIMIT %d;", limit)
	return query
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

func (c *Client) GetTable(ctx context.Context, database, schema, tableName string) (*Table, *http.Response, error) {
	likePattern := escapeLikePattern(tableName)
	queries := []string{
		fmt.Sprintf("SHOW TABLES LIKE '%s' ESCAPE '\\' IN SCHEMA \"%s\".\"%s\" LIMIT 1;", likePattern, database, schema),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, nil, err
	}

	var response ListTablesRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, resp, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, resp, err
	}
	defer resp.Body.Close()

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

func (c *Client) ListTableGrants(ctx context.Context, database, schema, tableName string) ([]TableGrant, error) {
	l := ctxzap.Extract(ctx)

	queries := []string{
		fmt.Sprintf("SHOW GRANTS ON TABLE \"%s\".\"%s\".\"%s\";", database, schema, tableName),
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

			err := json.NewDecoder(resp.Body).Decode(&errMsg)
			if err != nil {
				return nil, err
			}

			// code: 003001
			// message: SQL access control error:\nInsufficient privileges
			if errMsg.Code == "003001" {
				l.Debug("Insufficient privileges to show grants on table", zap.String("table", fmt.Sprintf("%s.%s.%s", database, schema, tableName)))
			} else {
				l.Error(errMsg.Message, zap.String("table", fmt.Sprintf("%s.%s.%s", database, schema, tableName)))
			}

			// Ignore if the account/role does not have permission to show grants
			return nil, nil
		}

		return nil, err
	}
	defer resp.Body.Close()

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity {
			l.Debug("Insufficient privileges to show grants on table (statement result)", zap.String("table", fmt.Sprintf("%s.%s.%s", database, schema, tableName)))
			return nil, nil
		}
		return nil, err
	}
	defer resp.Body.Close()

	grants, err := response.GetTableGrants()
	if err != nil {
		return nil, err
	}

	return grants, nil
}
