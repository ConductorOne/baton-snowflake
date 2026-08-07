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

	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

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
		if resp1 != nil && resp1.StatusCode == http.StatusUnprocessableEntity {
			l.Debug("Insufficient privileges for SHOW SCHEMAS IN DATABASE", zap.String("database", databaseName))
			wrappedErr := fmt.Errorf("baton-snowflake: insufficient privileges for SHOW SCHEMAS IN DATABASE %s: %w", databaseName, err)
			return nil, status.Error(codes.PermissionDenied, wrappedErr.Error())
		}
		return nil, err
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp2)
	if err != nil {
		if resp2 != nil && resp2.StatusCode == http.StatusUnprocessableEntity {
			l.Debug("Insufficient privileges for SHOW SCHEMAS IN DATABASE (statement result)", zap.String("database", databaseName))
			wrappedErr := fmt.Errorf("baton-snowflake: insufficient privileges for SHOW SCHEMAS IN DATABASE %s (statement result): %w", databaseName, err)
			return nil, status.Error(codes.PermissionDenied, wrappedErr.Error())
		}
		return nil, err
	}

	return response.ListSchemas()
}

var tableStructFieldToColumnMap = map[string]string{
	structFieldCreatedOn:    columnCreatedOn,
	structFieldName:         columnName,
	structFieldSchemaName:   columnSchemaName,
	structFieldDatabaseName: columnDatabaseName,
	structFieldKind:         columnKind,
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
		q = fmt.Sprintf("SHOW TABLES IN SCHEMA \"%s\".\"%s\" LIMIT %d FROM '%s';", escapedDB, escapedSchema, limit, escapeStringLiteral(cursor))
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
		if resp1 != nil && resp1.StatusCode == http.StatusUnprocessableEntity {
			l.Debug("Insufficient privileges for SHOW TABLES IN SCHEMA",
				zap.String("database", databaseName), zap.String("schema", schemaName))
			wrappedErr := fmt.Errorf("baton-snowflake: insufficient privileges for SHOW TABLES IN SCHEMA %s.%s: %w", databaseName, schemaName, err)
			return nil, "", status.Error(codes.PermissionDenied, wrappedErr.Error())
		}
		return nil, "", err
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, "", err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp2)
	if err != nil {
		if resp2 != nil && resp2.StatusCode == http.StatusUnprocessableEntity {
			l.Debug("Insufficient privileges for SHOW TABLES IN SCHEMA (statement result)",
				zap.String("database", databaseName), zap.String("schema", schemaName))
			wrappedErr := fmt.Errorf("baton-snowflake: insufficient privileges for SHOW TABLES IN SCHEMA %s.%s (statement result): %w", databaseName, schemaName, err)
			return nil, "", status.Error(codes.PermissionDenied, wrappedErr.Error())
		}
		return nil, "", err
	}

	tables, err := response.ListTables()
	if err != nil {
		return nil, "", err
	}

	var nextCursor string
	if limit > 0 && len(tables) >= limit {
		last := tables[len(tables)-1]
		nextCursor = last.Name
	}
	return tables, nextCursor, nil
}

// wildcardLookupLimit bounds SHOW ... LIKE name lookups (GetTable, GetAccountRole, GetDatabase);
// LIMIT 1 risks a wildcard-colliding row crowding out the real match before exact-match filtering.
const wildcardLookupLimit = 50

// escapeStringLiteral escapes a string for a single-quoted SQL literal: backslashes are doubled
// first (Snowflake treats \ as an escape char, so a trailing backslash could swallow the closing
// quote), then single quotes are doubled.
func escapeStringLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, "'", "''")
}

// escapeLikeStringLiteral escapes a string for a SHOW ... LIKE '<pattern>' argument: backslash is
// doubled first (LIKE's own escape char, ahead of the usual literal escaping) so a trailing
// backslash doesn't silently match zero rows. _ and % are left unescaped and still act as wildcards.
func escapeLikeStringLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return escapeStringLiteral(s)
}

// escapeDoubleQuotedIdentifier escapes a string for use inside Snowflake double-quoted identifiers.
// Double quotes inside the identifier must be escaped by doubling them ("").
func escapeDoubleQuotedIdentifier(s string) string {
	return strings.ReplaceAll(s, `"`, `""`)
}

func (c *Client) GetTable(ctx context.Context, database, schema, tableName string) (*Table, error) {
	// SHOW TABLES' LIKE has no ESCAPE clause, so _ and % stay live wildcards; adding "ESCAPE '\'"
	// here (a prior version did) makes Snowflake reject the query with a 422.
	likePattern := escapeLikeStringLiteral(tableName)
	queries := []string{
		fmt.Sprintf("SHOW TABLES LIKE '%s' IN SCHEMA \"%s\".\"%s\" LIMIT %d;", likePattern, escapeDoubleQuotedIdentifier(database), escapeDoubleQuotedIdentifier(schema), wildcardLookupLimit),
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
			return nil, nil
		}
		return nil, err
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp2)
	if err != nil {
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

	return nil, fmt.Errorf("table %s.%s.%s not found", database, schema, tableName)
}

var tableGrantStructFieldToColumnMap = map[string]string{
	structFieldCreatedOn:   columnCreatedOn,
	"Privilege":            "privilege",
	"GrantedOn":            "granted_on",
	structFieldName:        columnName,
	structFieldGrantedTo:   columnGrantedTo,
	structFieldGranteeName: columnGranteeName,
	"GrantOption":          columnGrantOption,
	"GrantedBy":            columnGrantedBy,
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
		grant.GranteeName = unquoteSnowflakeIdentifier(grant.GranteeName)

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

// tableGrantsCursor is the opaque page cursor for ListTableGrants. Unlike SHOW GRANTS OF ROLE
// (see ListAccountRoleGrantees), SHOW GRANTS ON TABLE/VIEW rows are parsed by column name via
// ResultSetMetadata.ParseRow, and Snowflake's SQL API only returns that column layout (rowType) on the
// partition-0 response - partitions 1..N return bare data with no metadata. The cursor therefore carries
// the rowType layout captured from partition 0 forward so later partitions can still be parsed, independent
// of whether a session store is available to cache it.
type tableGrantsCursor struct {
	Handle          string    `json:"handle"`
	PartitionID     int       `json:"partitionId"`
	TotalPartitions int       `json:"totalPartitions"`
	RowTypes        []RowType `json:"rowTypes"`
}

func encodeTableGrantsCursor(cur tableGrantsCursor) (string, error) {
	b, err := json.Marshal(cur)
	if err != nil {
		return "", fmt.Errorf("baton-snowflake: failed to encode table grant page cursor: %w", err)
	}
	return string(b), nil
}

func decodeTableGrantsCursor(cursor string) (tableGrantsCursor, error) {
	var cur tableGrantsCursor
	if err := json.Unmarshal([]byte(cursor), &cur); err != nil {
		return tableGrantsCursor{}, fmt.Errorf("baton-snowflake: invalid table grant page cursor: %w", err)
	}
	return cur, nil
}

// ListTableGrants uses objectKind to run SHOW GRANTS ON TABLE or ON VIEW (Snowflake requires the correct type).
//
// cursor is empty on the first call; subsequent calls pass the opaque cursor returned by the previous call.
// Each call returns only the grants found in that page/partition (not an accumulation), so callers can
// safely union grants across pages the same way they union pages of any other paginated resource.
//
// Internally, partial progress is accumulated in the session store as pages are consumed. Once the last
// partition has been fetched, the full grant list is cached under the "complete" key so that other callers
// needing the same table's grants within the same sync (e.g. both Entitlements and Grants) get a single-call,
// no-network cache hit instead of re-running the query and re-walking every partition.
func (c *Client) ListTableGrants(ctx context.Context, ss sessions.SessionStore, database, schema, tableName, objectKind, cursor string) ([]TableGrant, string, error) {
	cacheKey := tableGrantsCacheKey(database, schema, tableName, objectKind)

	if cursor != "" {
		return c.listTableGrantsPartition(ctx, ss, cacheKey, cursor)
	}

	if ss != nil {
		if cached, found, err := session.GetJSON[[]TableGrant](ctx, ss, cacheKey, tableGrantsNamespace); err == nil && found {
			return cached, "", nil
		}
	}

	page, err := c.fetchTableGrantsFirstPage(ctx, database, schema, tableName, objectKind)
	if err != nil {
		return nil, "", err
	}

	if page.NumPartitions <= 1 {
		if ss != nil {
			// Best-effort: a failure here just costs a future caller a cache miss (they
			// re-run this same single-partition query), never wrong data.
			_ = session.SetJSON(ctx, ss, cacheKey, page.Grants, tableGrantsNamespace)
		}
		return page.Grants, "", nil
	}

	if ss != nil {
		// Not best-effort: partition 0's rows must survive to be stitched together with
		// later partitions into the "complete" cache entry other callers trust unconditionally
		// (see listTableGrantsPartition). A silent failure here would make that entry
		// silently truncated once promoted - the exact bug class this pagination fix closes.
		if err := session.SetJSON(ctx, ss, cacheKey, page.Grants, tableGrantsPartialNamespace); err != nil {
			return nil, "", fmt.Errorf("baton-snowflake: failed to persist table grants pagination progress: %w", err)
		}
	}

	nextCursor, err := encodeTableGrantsCursor(tableGrantsCursor{
		Handle:          page.Handle,
		PartitionID:     1,
		TotalPartitions: page.NumPartitions,
		RowTypes:        page.RowTypes,
	})
	if err != nil {
		return nil, "", err
	}
	return page.Grants, nextCursor, nil
}

// tableGrantsFirstPage is the result of the initial SHOW GRANTS ON TABLE/VIEW request: partition
// 0's rows plus everything needed to page through the rest (mirrors what the cursor later carries
// into listTableGrantsPartition).
type tableGrantsFirstPage struct {
	Grants        []TableGrant
	Handle        string
	NumPartitions int
	RowTypes      []RowType
}

// fetchTableGrantsFirstPage executes the SHOW GRANTS ON TABLE/VIEW query (the POST-then-GET dance,
// with 422/insufficient-privilege handling on each leg) and returns partition 0. Split out of
// ListTableGrants so that function stays a thin cache/cursor orchestrator, the same way
// listTableGrantsPartition is a self-contained single-partition fetch for later pages.
func (c *Client) fetchTableGrantsFirstPage(ctx context.Context, database, schema, tableName, objectKind string) (tableGrantsFirstPage, error) {
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
		return tableGrantsFirstPage{}, err
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
				return tableGrantsFirstPage{}, fmt.Errorf("received 422 but failed to decode response body: %w (request error: %s)", decodeErr, err.Error())
			}

			// code: 003001
			// message: SQL access control error:\nInsufficient privileges
			tableRef := fmt.Sprintf("%s.%s.%s", database, schema, tableName)
			if errMsg.Code == "003001" {
				l.Debug("Insufficient privileges to show grants on table", zap.String("table", tableRef))
			} else {
				l.Error(errMsg.Message, zap.String("table", tableRef))
			}

			return tableGrantsFirstPage{}, status.Errorf(codes.PermissionDenied, "baton-snowflake: insufficient privileges to show grants on table %s: %s", tableRef, errMsg.Message)
		}

		return tableGrantsFirstPage{}, err
	}
	if resp != nil {
		defer resp.Body.Close()
	}

	handle := response.StatementHandle

	req, err = c.GetStatementResponse(ctx, handle)
	if err != nil {
		return tableGrantsFirstPage{}, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity {
			l.Debug("Insufficient privileges to show grants on table (statement result)", zap.String("table", fmt.Sprintf("%s.%s.%s", database, schema, tableName)))
			wrappedErr := fmt.Errorf("baton-snowflake: insufficient privileges to show grants on table %s.%s.%s (statement result): %w", database, schema, tableName, err)
			return tableGrantsFirstPage{}, status.Error(codes.PermissionDenied, wrappedErr.Error())
		}
		return tableGrantsFirstPage{}, err
	}
	if resp != nil {
		defer resp.Body.Close()
	}

	grants, err := response.GetTableGrants()
	if err != nil {
		return tableGrantsFirstPage{}, err
	}

	numPartitions := len(response.ResultSetMetadata.PartitionInfo)
	l.Debug("ListTableGrants",
		zap.String("table", fmt.Sprintf("%s.%s.%s", database, schema, tableName)),
		zap.Int("numPartitions", numPartitions),
		zap.Int("numRows", response.ResultSetMetadata.NumRows))

	return tableGrantsFirstPage{
		Grants:        grants,
		Handle:        handle,
		NumPartitions: numPartitions,
		RowTypes:      response.ResultSetMetadata.RowTypes,
	}, nil
}

// listTableGrantsPartition fetches a non-first partition of a paginated ListTableGrants call.
// It merges the newly-fetched partition into the in-progress accumulation kept in the session store,
// promoting it to the "complete" cache entry once the last partition has been consumed.
func (c *Client) listTableGrantsPartition(ctx context.Context, ss sessions.SessionStore, cacheKey, cursor string) ([]TableGrant, string, error) {
	cur, err := decodeTableGrantsCursor(cursor)
	if err != nil {
		return nil, "", err
	}

	req, err := c.GetStatementPartition(ctx, cur.Handle, cur.PartitionID)
	if err != nil {
		return nil, "", err
	}

	var response ListTableGrantsRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp)
	if err != nil {
		return nil, "", err
	}

	// Partitions after the first come back with data only, no resultSetMetadata - reuse the
	// column layout captured from partition 0 (carried in the cursor) to parse rows by name.
	response.ResultSetMetadata.RowTypes = cur.RowTypes

	grants, err := response.GetTableGrants()
	if err != nil {
		return nil, "", err
	}

	var nextCursor string
	if cur.PartitionID+1 < cur.TotalPartitions {
		nextCursor, err = encodeTableGrantsCursor(tableGrantsCursor{
			Handle:          cur.Handle,
			PartitionID:     cur.PartitionID + 1,
			TotalPartitions: cur.TotalPartitions,
			RowTypes:        cur.RowTypes,
		})
		if err != nil {
			return nil, "", err
		}
	}

	if ss != nil {
		// Not best-effort (see the matching comment in ListTableGrants): losing this read
		// silently reconstructs "accumulated" from only the partitions fetched after the
		// failure, which then gets promoted as the complete result below.
		accumulated, _, err := session.GetJSON[[]TableGrant](ctx, ss, cacheKey, tableGrantsPartialNamespace)
		if err != nil {
			return nil, "", fmt.Errorf("baton-snowflake: failed to read table grants pagination progress: %w", err)
		}
		accumulated = append(accumulated, grants...)

		if nextCursor == "" {
			// Best-effort: a failure here just means the next full lookup for this table
			// misses the cache and re-runs the paginated query from scratch - self-correcting.
			_ = session.SetJSON(ctx, ss, cacheKey, accumulated, tableGrantsNamespace)
			// Best-effort cleanup: once the complete entry above exists it always wins the
			// cache check in ListTableGrants, so a leftover partial entry is never read again.
			_ = session.DeleteJSON(ctx, ss, cacheKey, tableGrantsPartialNamespace)
		} else {
			// Not best-effort: this page's contribution must survive for the next call's
			// accumulation read above.
			if err := session.SetJSON(ctx, ss, cacheKey, accumulated, tableGrantsPartialNamespace); err != nil {
				return nil, "", fmt.Errorf("baton-snowflake: failed to persist table grants pagination progress: %w", err)
			}
		}
	}

	return grants, nextCursor, nil
}
