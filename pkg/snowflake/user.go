package snowflake

import (
	"context"
	"fmt"
	"net/http"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

const ()

type (
	User struct {
		Username        string
		FirstName       string
		LastName        string
		Email           string
		Disabled        bool
		Locked          bool
		DefaultRole     string
		HasRSAPublicKey bool
		HasPassword     bool
	}
	ListUsersRawResponse struct {
		StatementsApiResponseBase
		Data [][]string `json:"data"`
	}
	GetUserRawResponse struct {
		StatementsApiResponseBase
		Data [][]string `json:"data"`
	}
)

func (r *ListUsersRawResponse) GetUsers() []User {
	var users []User
	for _, user := range r.Data {
		users = append(users, User{
			Username:        user[0],
			FirstName:       user[4],
			LastName:        user[5],
			Email:           user[6],
			Disabled:        user[10] == "true",
			Locked:          user[12] == "true",
			DefaultRole:     user[15],
			HasRSAPublicKey: user[25] == "true",
			HasPassword:     user[24] == "true",
		})
	}
	return users
}

func (r *GetUserRawResponse) GetUser() User {
	return User{
		Username:        r.Data[0][1],
		FirstName:       r.Data[4][1],
		LastName:        r.Data[6][1],
		Email:           r.Data[7][1],
		Disabled:        r.Data[10][1] == "true",
		Locked:          r.Data[11][1] == "true",
		DefaultRole:     r.Data[17][1],
		HasRSAPublicKey: r.Data[23][1] != "",
		HasPassword:     r.Data[8][1] != "true",
	}
}

func (c *Client) ListUsers(ctx context.Context, offset, limit int) ([]User, *http.Response, error) {
	queries := []string{
		"SHOW USERS;",
		c.paginateLastQuery(offset, limit),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, nil, err
	}

	var response ListUsersRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, resp, err
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandles[1]) // TODO: validate that the statementHandlers[1] is the correct one
	if err != nil {
		return nil, resp, err
	}
	resp, err = c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, resp, err
	}

	return response.GetUsers(), resp, nil
}

func (c *Client) GetUser(ctx context.Context, username string) (*User, *http.Response, error) {
	queries := []string{
		fmt.Sprintf("DESCRIBE USER %s;", username),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, nil, err
	}

	var response GetUserRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, resp, err
	}

	user := response.GetUser()

	return &user, resp, nil
}
