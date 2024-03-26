package snowflake

import (
	"context"
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
