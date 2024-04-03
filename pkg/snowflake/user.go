package snowflake

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

var (
	userStructFieldToColumnMap = map[string]string{
		"Username":        "name",
		"FirstName":       "first_name",
		"LastName":        "last_name",
		"Email":           "email",
		"Disabled":        "disabled",
		"Locked":          "snowflake_lock",
		"DefaultRole":     "default_role",
		"HasRSAPublicKey": "has_rsa_public_key",
		"HasPassword":     "has_password",
	}
	// Sadly snowflake is inconsistent and returns different set of columns for DESC USER
	ignoredUserStructFieldsForDescribeOperation = []string{
		"HasRSAPublicKey",
		"HasPassword",
	}
)

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
	}
	GetUserRawResponse struct {
		StatementsApiResponseBase
		Data [][]string `json:"data"`
	}
)

func (u *User) GetColumnName(fieldName string) string {
	return userStructFieldToColumnMap[fieldName]
}

func (r *ListUsersRawResponse) GetUsers() ([]User, error) {
	var users []User
	for _, row := range r.Data {
		user := &User{}
		if err := r.ResultSetMetadata.ParseRow(user, row); err != nil {
			return nil, err
		}

		users = append(users, *user)
	}
	return users, nil
}

func (r *GetUserRawResponse) GetUser() (*User, error) {
	user := &User{}

	reflected := reflect.ValueOf(user).Elem()
	for i := 0; i < reflected.NumField(); i++ {
		field := reflected.Type().Field(i)
		// Sadly snowflake is inconsistent and returns the column names in uppercase for DESC USER
		if Contains(ignoredUserStructFieldsForDescribeOperation, field.Name) {
			continue
		}
		columnName := strings.ToUpper(user.GetColumnName(field.Name))

		value, found := r.GetValueByColumnName(columnName)
		if !found {
			return nil, fmt.Errorf("column %s not found", columnName)
		}

		switch field.Type.Kind() {
		case reflect.String:
			reflected.Field(i).SetString(value)
		case reflect.Bool:
			reflected.Field(i).SetBool(value == "true")
		default:
			return nil, fmt.Errorf("unsupported type %s", field.Type.Kind())
		}
	}

	return user, nil
}

func (r *GetUserRawResponse) GetValueByColumnName(columnName string) (string, bool) {
	for _, row := range r.Data {
		if strings.ToUpper(row[0]) == columnName {
			return row[1], true
		}
	}
	return "", false
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

	users, err := response.GetUsers()
	if err != nil {
		return nil, resp, err
	}

	return users, resp, nil
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

	user, err := response.GetUser()
	if err != nil {
		return nil, resp, err
	}

	return user, resp, nil
}
