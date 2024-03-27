package snowflake

import (
	"context"
	"fmt"
	"net/http"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

type (
	Database struct {
		Name  string
		Owner string
	}
	ListDatabasesRawResponse struct {
		StatementsApiResponseBase
		Data [][]string `json:"data"`
	}
)

func (r *ListDatabasesRawResponse) GetDatabases() []Database {
	var databases []Database
	for _, database := range r.Data {
		databases = append(databases, Database{
			Name:  database[1],
			Owner: database[5],
		})
	}
	return databases
}

func (c *Client) ListDatabases(ctx context.Context, offset, limit int) ([]Database, *http.Response, error) {
	queries := []string{
		"SHOW DATABASES;",
		c.paginateLastQuery(offset, limit),
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

	req, err = c.GetStatementResponse(ctx, response.StatementHandles[1]) // TODO: validate that the statementHandlers[1] is the correct one
	if err != nil {
		return nil, resp, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, resp, err
	}

	return response.GetDatabases(), resp, nil
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
		return nil, nil, err
	}

	databases := response.GetDatabases()
	if len(databases) == 0 {
		return nil, resp, nil
	} else if len(databases) > 1 {
		return nil, resp, fmt.Errorf("expected 1 database with name %s, got %d", name, len(databases))
	}

	return &databases[0], resp, nil
}
