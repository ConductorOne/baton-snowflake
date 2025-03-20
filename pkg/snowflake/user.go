package snowflake

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

var (
	userStructFieldToColumnMap = map[string]string{
		"Username":         "name",
		"Login":            "login_name",
		"DisplayName":      "display_name",
		"FirstName":        "first_name",
		"LastName":         "last_name",
		"Email":            "email",
		"Disabled":         "disabled",
		"Locked":           "snowflake_lock",
		"DefaultRole":      "default_role",
		"HasRSAPublicKey":  "has_rsa_public_key",
		"HasPassword":      "has_password",
		"LastSuccessLogin": "last_success_login",
		"Type":             "type",
		"HasMfa":           "has_mfa",
		"Comment":          "comment",
	}
	// Sadly snowflake is inconsistent and returns different set of columns for DESC USER.
	ignoredUserStructFieldsForDescribeOperation = []string{
		"HasRSAPublicKey",
		"HasPassword",
	}
)

type (
	User struct {
		Username         string
		Login            string
		DisplayName      string
		FirstName        string
		LastName         string
		Email            string
		Disabled         bool
		Locked           bool
		DefaultRole      string
		HasRSAPublicKey  bool
		HasPassword      bool
		LastSuccessLogin time.Time
		Type             string
		HasMfa           bool
		Comment          string
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

func (c *Client) ListUsers(ctx context.Context, cursor string, limit int) ([]User, *http.Response, error) {
	queries := []string{}
	if cursor != "" {
		queries = append(queries, fmt.Sprintf("SHOW USERS LIMIT %d FROM '%s';", limit, cursor))
	} else {
		queries = append(queries, fmt.Sprintf("SHOW USERS LIMIT %d;", limit))
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

	if len(response.StatementHandles) < 2 {
		return nil, resp, nil
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandles[1])
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
		fmt.Sprintf("DESCRIBE USER \"%s\";", username),
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
