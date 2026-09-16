package snowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

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

	queries := []string{c.listSchemasStatement(databaseName)}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListSchemasRawResponse
	var apiErr SnowflakeError
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp1)
	if err != nil {
		if c.skippableDenial(resp1, &apiErr) {
			l.Debug("Insufficient privileges for SHOW SCHEMAS IN DATABASE", zap.String("database", databaseName))
			return nil, uhttp.WrapErrors(
				codes.PermissionDenied,
				fmt.Sprintf("baton-snowflake: insufficient privileges for SHOW SCHEMAS IN DATABASE %s", databaseName),
				ErrInsufficientPrivileges, err,
			)
		}
		if c.skippableSharedDatabase(resp1, &apiErr) {
			l.Debug("Shared database is no longer available for SHOW SCHEMAS IN DATABASE", zap.String("database", databaseName))
			return nil, uhttp.WrapErrors(
				codes.NotFound,
				fmt.Sprintf("baton-snowflake: shared database unavailable for SHOW SCHEMAS IN DATABASE %s", databaseName),
				ErrSharedDatabaseUnavailable, err,
			)
		}
		return nil, c.classifyReadError(accountUsageSchemataView, resp1, &apiErr, err)
	}
	if err := errIfStatementIncomplete(resp1, "the schema listing"); err != nil {
		return nil, err
	}

	// Captured before the statement-result GET: that response reuses this struct and does
	// not necessarily carry the handle, so reading it afterwards can see an empty string.
	handle := response.StatementHandle

	req, err = c.GetStatementResponse(ctx, handle)
	if err != nil {
		return nil, err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp2)
	if err != nil {
		if c.skippableDenial(resp2, &apiErr) {
			l.Debug("Insufficient privileges for SHOW SCHEMAS IN DATABASE (statement result)", zap.String("database", databaseName))
			return nil, uhttp.WrapErrors(
				codes.PermissionDenied,
				fmt.Sprintf("baton-snowflake: insufficient privileges for SHOW SCHEMAS IN DATABASE %s (statement result)", databaseName),
				ErrInsufficientPrivileges, err,
			)
		}
		if c.skippableSharedDatabase(resp2, &apiErr) {
			l.Debug("Shared database is no longer available for SHOW SCHEMAS IN DATABASE (statement result)", zap.String("database", databaseName))
			return nil, uhttp.WrapErrors(
				codes.NotFound,
				fmt.Sprintf("baton-snowflake: shared database unavailable for SHOW SCHEMAS IN DATABASE %s (statement result)", databaseName),
				ErrSharedDatabaseUnavailable, err,
			)
		}
		return nil, c.classifyReadError(accountUsageSchemataView, resp2, &apiErr, err)
	}
	if err := errIfStatementIncomplete(resp2, "the schema listing"); err != nil {
		return nil, err
	}

	schemas, err := response.ListSchemas()
	if err != nil {
		return nil, err
	}

	// Walk the remaining partitions rather than returning partition 0 alone.
	//
	// This used to read partition 0 only. Under SHOW that truncation was mostly theoretical,
	// because the statement returns just the schemas the role holds a privilege on. Under
	// ACCOUNT_USAGE it is not: SCHEMATA returns every schema in the database account-wide, so
	// a large database spills past partition 0 - and because tableBuilder.List pushes one
	// page state per schema returned, a dropped schema takes every table under it out of the
	// sync, which C1 reads as a deletion.
	rowTypes := response.ResultSetMetadata.RowTypes
	numPartitions := len(response.ResultSetMetadata.PartitionInfo)
	// Bounded rather than cursored, unlike the grant walks in this package, which hand one
	// partition per SDK page back through a cursor so the SDK drives and can checkpoint
	// between them. Schemas do not warrant that: the statement projects two narrow text
	// columns, so partition 0 alone holds thousands of rows and a real database is a single
	// partition. The bound exists so a pathological account cannot turn the first
	// tableBuilder.List call into an unbounded fetch; exceeding it is loud, because silently
	// truncating is the bug this walk was added to fix.
	if numPartitions > maxSchemaPartitions {
		return nil, fmt.Errorf(
			"baton-snowflake: %s returned %d partitions of schemas for database %q, over the %d "+
				"this connector walks. Sync the database with fewer schemas, or open an issue so "+
				"the schema listing can be paginated through the SDK instead",
			accountUsageSchemataView, numPartitions, databaseName, maxSchemaPartitions,
		)
	}
	for partitionID := 1; partitionID < numPartitions; partitionID++ {
		more, err := c.listSchemasPartition(ctx, handle, partitionID, rowTypes, &apiErr)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, more...)
	}

	if numPartitions > 1 {
		l.Debug("ListSchemasInDatabase walked multiple partitions",
			zap.String("database", databaseName),
			zap.Int("numPartitions", numPartitions),
			zap.Int("schemas", len(schemas)))
	}

	return schemas, nil
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

	queries := []string{c.listTablesStatement(databaseName, schemaName, cursor, limit)}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, "", err
	}

	var response ListTablesRawResponse
	var apiErr SnowflakeError
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp1)
	if err != nil {
		if c.skippableDenial(resp1, &apiErr) {
			l.Debug("Insufficient privileges for SHOW TABLES IN SCHEMA",
				zap.String("database", databaseName), zap.String("schema", schemaName))
			return nil, "", uhttp.WrapErrors(
				codes.PermissionDenied,
				fmt.Sprintf("baton-snowflake: insufficient privileges for SHOW TABLES IN SCHEMA %s.%s", databaseName, schemaName),
				ErrInsufficientPrivileges, err,
			)
		}
		if c.skippableSharedDatabase(resp1, &apiErr) {
			l.Debug("Shared database is no longer available for SHOW TABLES IN SCHEMA",
				zap.String("database", databaseName), zap.String("schema", schemaName))
			return nil, "", uhttp.WrapErrors(
				codes.NotFound,
				fmt.Sprintf("baton-snowflake: shared database unavailable for SHOW TABLES IN SCHEMA %s.%s", databaseName, schemaName),
				ErrSharedDatabaseUnavailable, err,
			)
		}
		return nil, "", c.classifyReadError(accountUsageTablesView, resp1, &apiErr, err)
	}
	if err := errIfStatementIncomplete(resp1, "the table listing"); err != nil {
		return nil, "", err
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, "", err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp2)
	if err != nil {
		if c.skippableDenial(resp2, &apiErr) {
			l.Debug("Insufficient privileges for SHOW TABLES IN SCHEMA (statement result)",
				zap.String("database", databaseName), zap.String("schema", schemaName))
			return nil, "", uhttp.WrapErrors(
				codes.PermissionDenied,
				fmt.Sprintf("baton-snowflake: insufficient privileges for SHOW TABLES IN SCHEMA %s.%s (statement result)", databaseName, schemaName),
				ErrInsufficientPrivileges, err,
			)
		}
		if c.skippableSharedDatabase(resp2, &apiErr) {
			l.Debug("Shared database is no longer available for SHOW TABLES IN SCHEMA (statement result)",
				zap.String("database", databaseName), zap.String("schema", schemaName))
			return nil, "", uhttp.WrapErrors(
				codes.NotFound,
				fmt.Sprintf("baton-snowflake: shared database unavailable for SHOW TABLES IN SCHEMA %s.%s (statement result)", databaseName, schemaName),
				ErrSharedDatabaseUnavailable, err,
			)
		}
		return nil, "", c.classifyReadError(accountUsageTablesView, resp2, &apiErr, err)
	}
	if err := errIfStatementIncomplete(resp2, "the table listing"); err != nil {
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

// wildcardLookupLimit bounds SHOW ... LIKE '<name>' name lookups (GetTable, GetAccountRole,
// GetDatabase). SHOW's LIKE has no ESCAPE clause, so _ and % stay live wildcards; LIMIT 1 could
// let a colliding row crowd out the real one before the exact-match filter sees it.
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
	queries := []string{c.getTableStatement(database, schema, tableName)}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListTablesRawResponse
	var apiErr SnowflakeError
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp1)
	if err != nil {
		// Same contract as ListSchemasInDatabase: only an access-control 422 means the table
		// is invisible to this role. Other 422s (SQL compilation from a bad LIKE/ESCAPE, etc.)
		// must stay fatal so a connector bug cannot look like a missing table.
		if c.skippableDenial(resp1, &apiErr) {
			return nil, nil
		}
		return nil, c.classifyReadError(accountUsageTablesView, resp1, &apiErr, err)
	}
	if err := errIfStatementIncomplete(resp1, "the single-table lookup"); err != nil {
		return nil, err
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp2)
	if err != nil {
		return nil, c.classifyReadError(accountUsageTablesView, resp2, &apiErr, err)
	}
	if err := errIfStatementIncomplete(resp2, "the single-table lookup"); err != nil {
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
	return fmt.Sprintf("%s|%s|%s|%s", database, schema, tableName, normalizeObjectKind(objectKind))
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
	queries := []string{c.tableGrantsStatement(database, schema, tableName, objectKind)}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return tableGrantsFirstPage{}, err
	}

	var response ListTableGrantsRawResponse
	var apiErr SnowflakeError
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp1)
	if err != nil {
		// uhttp already decoded the error body into apiErr, so the access-control code is read
		// from there rather than by consuming resp.Body a second time.
		if c.skippableDenial(resp1, &apiErr) {
			tableRef := fmt.Sprintf("%s.%s.%s", database, schema, tableName)
			l.Debug("Insufficient privileges to show grants on table", zap.String("table", tableRef))
			return tableGrantsFirstPage{}, uhttp.WrapErrors(
				codes.PermissionDenied,
				fmt.Sprintf("baton-snowflake: insufficient privileges to show grants on table %s: %s", tableRef, apiErr.Message()),
				ErrInsufficientPrivileges, err,
			)
		}

		return tableGrantsFirstPage{}, c.classifyReadError(accountUsageGrantsToRolesView, resp1, &apiErr, err)
	}
	if err := errIfStatementIncomplete(resp1, "the table grant read"); err != nil {
		return tableGrantsFirstPage{}, err
	}

	handle := response.StatementHandle

	req, err = c.GetStatementResponse(ctx, handle)
	if err != nil {
		return tableGrantsFirstPage{}, err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp2)
	if err != nil {
		if c.skippableDenial(resp2, &apiErr) {
			l.Debug("Insufficient privileges to show grants on table (statement result)", zap.String("table", fmt.Sprintf("%s.%s.%s", database, schema, tableName)))
			return tableGrantsFirstPage{}, uhttp.WrapErrors(
				codes.PermissionDenied,
				fmt.Sprintf("baton-snowflake: insufficient privileges to show grants on table %s.%s.%s (statement result)", database, schema, tableName),
				ErrInsufficientPrivileges, err,
			)
		}
		return tableGrantsFirstPage{}, c.classifyReadError(accountUsageGrantsToRolesView, resp2, &apiErr, err)
	}
	if err := errIfStatementIncomplete(resp2, "the table grant read"); err != nil {
		return tableGrantsFirstPage{}, err
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
	var apiErr SnowflakeError
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp)
	if err != nil {
		return nil, "", c.classifyReadError(accountUsageGrantsToRolesView, resp, &apiErr, err)
	}
	if err := errIfStatementIncomplete(resp, "the table grant partition read"); err != nil {
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

// maxSchemaPartitions bounds the schema partition walk. Two narrow text columns per row put
// thousands of schemas in a single partition, so this is far above any real database.
const maxSchemaPartitions = 64

// listSchemasPartition fetches one partition of a schema listing. It is a function rather
// than an inline loop body so each response body is closed when the partition is done,
// instead of every body staying open until the whole walk returns.
func (c *Client) listSchemasPartition(
	ctx context.Context,
	handle string,
	partitionID int,
	rowTypes []RowType,
	apiErr *SnowflakeError,
) ([]Schema, error) {
	req, err := c.GetStatementPartition(ctx, handle, partitionID)
	if err != nil {
		return nil, err
	}

	var partition ListSchemasRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&partition), uhttp.WithErrorResponse(apiErr))
	defer closeResponseBody(resp)
	if err != nil {
		return nil, c.classifyReadError(accountUsageSchemataView, resp, apiErr, err)
	}
	if err := errIfStatementIncomplete(resp, "the schema partition read"); err != nil {
		return nil, err
	}

	// Partition-only responses carry no rowType metadata, so restore it from partition 0 or
	// ParseRow cannot resolve column names by position.
	if len(partition.ResultSetMetadata.RowTypes) == 0 {
		partition.ResultSetMetadata.RowTypes = rowTypes
	}
	return partition.ListSchemas()
}

// listSchemasStatement is the discovery-mode-dependent statement for a database's schemas.
func (c *Client) listSchemasStatement(databaseName string) string {
	if c.usesAccountUsage() {
		return accountUsageListSchemasStatement(databaseName)
	}
	return fmt.Sprintf("SHOW SCHEMAS IN DATABASE \"%s\";", escapeDoubleQuotedIdentifier(databaseName))
}

// listTablesStatement is the discovery-mode-dependent statement for one page of a schema's
// tables. Both forms keyset-paginate on the table name and treat limit <= 0 as unbounded, so
// the cursor ListTablesInSchema returns means the same thing in either mode.
func (c *Client) listTablesStatement(databaseName, schemaName, cursor string, limit int) string {
	if c.usesAccountUsage() {
		return accountUsageListTablesStatement(databaseName, schemaName, cursor, limit)
	}
	escapedDB := escapeDoubleQuotedIdentifier(databaseName)
	escapedSchema := escapeDoubleQuotedIdentifier(schemaName)
	if cursor != "" {
		return fmt.Sprintf("SHOW TABLES IN SCHEMA \"%s\".\"%s\" LIMIT %d FROM '%s';", escapedDB, escapedSchema, limit, escapeStringLiteral(cursor))
	}
	return fmt.Sprintf("SHOW TABLES IN SCHEMA \"%s\".\"%s\" LIMIT %d;", escapedDB, escapedSchema, limit)
}

// getTableStatement is the discovery-mode-dependent single-table lookup. The SHOW form's
// LIKE has no ESCAPE clause, so _ and % stay live wildcards and GetTable has to filter the
// result for an exact match; the ACCOUNT_USAGE form matches exactly in SQL and has no such
// hazard, but it goes through the same filter so the two modes behave identically.
func (c *Client) getTableStatement(database, schema, tableName string) string {
	if c.usesAccountUsage() {
		return accountUsageGetTableStatement(database, schema, tableName)
	}
	return fmt.Sprintf("SHOW TABLES LIKE '%s' IN SCHEMA \"%s\".\"%s\" LIMIT %d;",
		escapeLikeStringLiteral(tableName), escapeDoubleQuotedIdentifier(database),
		escapeDoubleQuotedIdentifier(schema), wildcardLookupLimit)
}

// tableGrantsStatement is the discovery-mode-dependent statement for a table's or view's
// grants. Only the statement differs: the ACCOUNT_USAGE form aliases its columns to the
// SHOW GRANTS ON TABLE/VIEW names, so the response parsing, partition walk, page cursor and
// session-store caching in ListTableGrants are shared verbatim between the two modes.
func (c *Client) tableGrantsStatement(database, schema, tableName, objectKind string) string {
	if c.usesAccountUsage() {
		return accountUsageTableGrantsStatement(database, schema, tableName, objectKind)
	}
	return fmt.Sprintf("SHOW GRANTS ON %s \"%s\".\"%s\".\"%s\";",
		normalizeObjectKind(objectKind), escapeDoubleQuotedIdentifier(database),
		escapeDoubleQuotedIdentifier(schema), escapeDoubleQuotedIdentifier(tableName))
}
