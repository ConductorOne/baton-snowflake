package snowflake

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

var databaseStructFieldToColumnMap = map[string]string{
	"Name":   "name",
	"Owner":  "owner",
	"Kind":   "kind",
	"Origin": "origin",
}

type (
	Database struct {
		Name   string
		Owner  string
		Kind   string // STANDARD, SHARED, APPLICATION, etc.
		Origin string // empty for normal DBs, "<account>.<share>" for shared/system DBs
	}
	ListDatabasesRawResponse struct {
		StatementsApiResponseBase
	}
)

func (d *Database) GetColumnName(fieldName string) string {
	return databaseStructFieldToColumnMap[fieldName]
}

// IsSharedOrSystem returns true if the database is shared, imported, or a system database.
// Snowflake returns 422 on SHOW GRANTS for objects in these databases.
func (d *Database) IsSharedOrSystem() bool {
	if d.Origin != "" {
		return true
	}
	if d.Owner == "" || strings.EqualFold(d.Owner, "SNOWFLAKE") {
		return true
	}
	kind := strings.ToUpper(strings.TrimSpace(d.Kind))
	return kind == "SHARED" || kind == "APPLICATION" || kind == "IMPORTED DATABASE"
}

func (r *ListDatabasesRawResponse) GetDatabases() ([]Database, error) {
	var databases []Database
	for _, row := range r.Data {
		db := &Database{}
		if err := r.ResultSetMetadata.ParseRow(db, row); err != nil {
			return nil, err
		}

		databases = append(databases, *db)
	}
	return databases, nil
}

func (c *Client) ListDatabases(ctx context.Context, cursor string, limit int) ([]Database, *http.Response, error) {
	var queries []string

	if cursor != "" {
		queries = append(queries, fmt.Sprintf("SHOW DATABASES LIMIT %d FROM '%s';", limit, cursor))
	} else {
		queries = append(queries, fmt.Sprintf("SHOW DATABASES LIMIT %d;", limit))
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, nil, err
	}

	var response ListDatabasesRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, nil, err
	}

	l := ctxzap.Extract(ctx)
	l.Debug("ListDatabases", zap.String("response.code", response.Code), zap.String("response.message", response.Message))

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, resp, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, resp, err
	}

	dbs, err := response.GetDatabases()
	if err != nil {
		return nil, resp, err
	}

	return dbs, resp, nil
}

func (c *Client) GetDatabase(ctx context.Context, name string) (*Database, *http.Response, error) {
	queries := []string{
		fmt.Sprintf("SHOW DATABASES LIKE '%s' LIMIT 1;", name),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, nil, err
	}

	var response ListDatabasesRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		if IsUnprocessableEntity(resp, err) {
			return nil, resp, nil
		}
		return nil, resp, err
	}

	databases, err := response.GetDatabases()
	if err != nil {
		return nil, resp, err
	}
	if len(databases) == 0 {
		return nil, resp, fmt.Errorf("database with name %s not found", name)
	} else if len(databases) > 1 {
		return nil, resp, fmt.Errorf("expected 1 database with name %s, got %d", name, len(databases))
	}

	return &databases[0], resp, nil
}
