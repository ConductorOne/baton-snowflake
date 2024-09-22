package snowflake

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

const (
	AuthTypeHeaderKey   = "X-Snowflake-Authorization-Token-Type"
	AuthTypeHeaderValue = "KEYPAIR_JWT"
)

const (
	rowTypeString       = "text"
	rowTypeTimestampLtz = "timestamp_ltz"
)

type (
	Client struct {
		uhttp.BaseHttpClient
		JWTConfig

		AccountUrl       string
		StatementsApiUrl *url.URL
	}
	ResultSetMetadata struct {
		NumRows  int       `json:"numRows"`
		RowTypes []RowType `json:"rowType"`
	}
	StatementsApiResponseBase struct {
		ResultSetMetadata ResultSetMetadata `json:"resultSetMetadata"`
		Code              string            `json:"code"`
		StatementHandle   string            `json:"statementHandle"`
		StatementHandles  []string          `json:"statementHandles"`
		Message           string            `json:"message"`
		Data              [][]string        `json:"data"`
	}
	StatementsRequestParameters struct {
		StatementsCount int `json:"MULTI_STATEMENT_COUNT"`
	}
	StatementsApiRequestBody struct {
		Statement  string                      `json:"statement"`
		Parameters StatementsRequestParameters `json:"parameters"`
	}
	QueryParameter struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	}
	RowType struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	Parsable interface {
		GetColumnName(fieldName string) string
	}
)

func (m *ResultSetMetadata) FindRowTypeByName(name string) (bool, int, *RowType) {
	for i, rowType := range m.RowTypes {
		if rowType.Name == name {
			return true, i, &rowType
		}
	}

	return false, -1, nil
}

func (m *ResultSetMetadata) GetTimeValueFromRow(row []string, key string) (time.Time, error) {
	found, i, rowType := m.FindRowTypeByName(key)
	if !found {
		return time.Time{}, fmt.Errorf("row type %s not found", key)
	}

	if rowType.Type != rowTypeTimestampLtz {
		return time.Time{}, fmt.Errorf("column %s is not a timestamp ltz (row type is '%s')", key, rowType.Type)
	}

	if row[i] == "" {
		return time.Time{}, nil
	}

	return parseTime(row[i])
}

func (m *ResultSetMetadata) GetStringValueFromRow(row []string, key string) (string, error) {
	found, i, rowType := m.FindRowTypeByName(key)
	if !found {
		return "", fmt.Errorf("row type %s not found", key)
	}

	if rowType.Type != rowTypeString {
		return "", fmt.Errorf("column %s is not a string (row type is '%s')", key, rowType.Type)
	}

	return row[i], nil
}

func (m *ResultSetMetadata) GetBoolValueFromRow(row []string, key string) (bool, error) {
	found, i, rowType := m.FindRowTypeByName(key)
	if !found {
		return false, fmt.Errorf("row type %s not found", key)
	}

	if rowType.Type != rowTypeString {
		return false, fmt.Errorf("column %s is not a string", key)
	}

	// "NULL"-ish case
	if row[i] == "" {
		return false, nil
	}

	return strconv.ParseBool(row[i])
}

func (m *ResultSetMetadata) ParseRow(s Parsable, row []string) error {
	reflected := reflect.ValueOf(s).Elem()

	if reflected.Kind() != reflect.Struct {
		return fmt.Errorf("expected struct, got %s", reflected.Kind())
	}

	for i := 0; i < reflected.NumField(); i++ {
		field := reflected.Type().Field(i)
		columnName := s.GetColumnName(field.Name)

		switch field.Type.Kind() {
		case reflect.String:
			value, err := m.GetStringValueFromRow(row, columnName)
			if err != nil {
				return err
			}

			reflected.Field(i).SetString(value)
		case reflect.Bool:
			value, err := m.GetBoolValueFromRow(row, columnName)
			if err != nil {
				return err
			}

			reflected.Field(i).SetBool(value)
		case reflect.Struct:
			// Check if the field type is time.Time
			if field.Type == reflect.TypeOf(time.Time{}) {
				value, err := m.GetTimeValueFromRow(row, columnName)
				if err != nil {
					return err
				}
				reflected.Field(i).Set(reflect.ValueOf(value))
			} else {
				return fmt.Errorf("unsupported struct type %s", field.Type)
			}

		default:
			return fmt.Errorf("unsupported type %s", field.Type.Kind())
		}
	}

	return nil
}

func createStatementsApiUrl(accountUrl string) (*url.URL, error) {
	stringUrl, err := url.JoinPath(accountUrl, "api/v2/statements")
	if err != nil {
		return nil, err
	}

	return url.Parse(stringUrl)
}

func New(accountUrl string, jwtConfig JWTConfig, httpClient *http.Client) (*Client, error) {
	statementsApiUrl, err := createStatementsApiUrl(accountUrl)
	if err != nil {
		return nil, err
	}

	return &Client{
		BaseHttpClient:   *uhttp.NewBaseHttpClient(httpClient),
		JWTConfig:        jwtConfig,
		AccountUrl:       accountUrl,
		StatementsApiUrl: statementsApiUrl,
	}, nil
}

func (c *Client) PostStatementRequest(ctx context.Context, queries []string) (*http.Request, error) {
	body := &StatementsApiRequestBody{}
	if len(queries) == 1 {
		body.Statement = queries[0]
	} else {
		body.Statement = strings.Join(queries, "")
		body.Parameters = StatementsRequestParameters{
			StatementsCount: len(queries),
		}
	}

	return c.NewRequest(
		ctx,
		http.MethodPost,
		c.StatementsApiUrl,
		uhttp.WithJSONBody(body),
		uhttp.WithAcceptJSONHeader(),
		uhttp.WithHeader(AuthTypeHeaderKey, AuthTypeHeaderValue),
	)
}

func (c *Client) GetStatementResponse(ctx context.Context, statementHandle string) (*http.Request, error) {
	stringUrl, err := url.JoinPath(c.StatementsApiUrl.String(), statementHandle)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(stringUrl)
	if err != nil {
		return nil, err
	}

	return c.NewRequest(
		ctx,
		http.MethodGet,
		u,
		uhttp.WithAcceptJSONHeader(),
		uhttp.WithHeader(AuthTypeHeaderKey, AuthTypeHeaderValue),
	)
}

func (c *Client) paginateLastQuery(offset, limit int) string {
	return fmt.Sprintf("SELECT * FROM table(RESULT_SCAN(LAST_QUERY_ID())) LIMIT %d OFFSET %d;", limit, offset)
}

func Contains[T comparable](ts []T, val T) bool {
	for _, t := range ts {
		if t == val {
			return true
		}
	}
	return false
}
