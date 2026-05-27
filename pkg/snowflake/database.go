package snowflake

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

var databaseStructFieldToColumnMap = map[string]string{
	structFieldName:  columnName,
	structFieldOwner: columnOwner,
	"Kind":           "kind",
	"Origin":         "origin",
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
	return kind == "SHARED" || kind == "APPLICATION" || kind == "IMPORTED DATABASE" || kind == "CATALOG-LINKED DATABASE"
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

func (c *Client) ListDatabases(ctx context.Context, cursor string, limit int) ([]Database, error) {
	var queries []string

	if cursor != "" {
		queries = append(queries, fmt.Sprintf("SHOW DATABASES LIMIT %d FROM '%s';", limit, cursor))
	} else {
		queries = append(queries, fmt.Sprintf("SHOW DATABASES LIMIT %d;", limit))
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListDatabasesRawResponse
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp1)
	if err != nil {
		return nil, err
	}

	l := ctxzap.Extract(ctx)
	l.Debug("ListDatabases", zap.String("response.code", response.Code), zap.String("response.message", response.Message))

	if err := c.fetchStatementResultIfAsync(ctx, resp1, response.StatementHandle, &response); err != nil {
		return nil, err
	}

	dbs, err := response.GetDatabases()
	if err != nil {
		return nil, err
	}

	return dbs, nil
}

func (c *Client) CacheDatabases(ctx context.Context, ss sessions.SessionStore, databases []Database) error {
	if ss == nil || len(databases) == 0 {
		return nil
	}
	for i := range databases {
		if err := session.SetJSON(ctx, ss, databases[i].Name, &databases[i], databaseNamespace); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) GetDatabase(ctx context.Context, ss sessions.SessionStore, name string) (*Database, int, error) {
	if ss != nil {
		cached, found, err := session.GetJSON[*Database](ctx, ss, name, databaseNamespace)
		if err != nil {
			ctxzap.Extract(ctx).Debug("database cache lookup error, falling through to API",
				zap.String("name", name), zap.Error(err))
		} else if found {
			return cached, http.StatusOK, nil
		}
	}
	queries := []string{
		fmt.Sprintf("SHOW DATABASES LIKE '%s' LIMIT 1;", name),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, 0, err
	}

	var response ListDatabasesRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	defer closeResponseBody(resp)
	if err != nil {
		statusCode := 0
		if resp != nil {
			statusCode = resp.StatusCode
		}
		return nil, statusCode, err
	}

	pollStatusCode := resp.StatusCode
	if resp.StatusCode == http.StatusAccepted {
		req, err = c.GetStatementResponse(ctx, response.StatementHandle)
		if err != nil {
			return nil, 0, err
		}
		pollResp, err := c.Do(req, uhttp.WithJSONResponse(&response))
		defer closeResponseBody(pollResp)
		if err != nil {
			return nil, 0, err
		}
		if pollResp != nil {
			pollStatusCode = pollResp.StatusCode
		}
	}

	databases, err := response.GetDatabases()
	if err != nil {
		return nil, pollStatusCode, err
	}

	if len(databases) == 0 {
		return nil, pollStatusCode, fmt.Errorf("database with name %s not found", name)
	} else if len(databases) > 1 {
		return nil, pollStatusCode, fmt.Errorf("expected 1 database with name %s, got %d", name, len(databases))
	}

	if ss != nil {
		// Write-back is best-effort: a cache miss on a subsequent call just falls through to the API.
		_ = session.SetJSON(ctx, ss, name, &databases[0], databaseNamespace)
	}
	return &databases[0], pollStatusCode, nil
}
