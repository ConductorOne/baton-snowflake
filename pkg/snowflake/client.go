package snowflake

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

const (
	AuthTypeHeaderKey   = "X-Snowflake-Authorization-Token-Type"
	AuthTypeHeaderValue = "KEYPAIR_JWT"
)

type (
	Client struct {
		uhttp.BaseHttpClient
		JWTConfig

		AccountUrl       string
		StatementsApiUrl *url.URL
	}
	ResultSetMetadata struct {
		NumRows int `json:"numRows"`
	}
	StatementsApiResponseBase struct {
		ResultSetMetadata ResultSetMetadata `json:"resultSetMetadata"`
		Code              int               `json:"code"`
		StatementHandle   string            `json:"statementHandle"`
		StatementHandlers []string          `json:"statementHandlers"`
		Message           string            `json:"message"`
	}
	StatementsRequestParameters struct {
		StatementsCount int `json:"MULTI_STATEMENT_COUNT"`
	}
	StatementsApiRequestBody struct {
		Statement  string                      `json:"statement"`
		Parameters StatementsRequestParameters `json:"parameters"`
		Bindings   map[string]QueryParameter   `json:"bindings"`
	}
	QueryParameter struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	}
)

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

func (c *Client) PostStatementRequest(ctx context.Context, queries []string, parameters []QueryParameter) (*http.Request, error) {
	body := &StatementsApiRequestBody{
		Statement: strings.Join(queries, ""),
		Parameters: StatementsRequestParameters{
			StatementsCount: len(queries),
		},
	}

	if parameters != nil && len(parameters) > 0 {
		body.Bindings = make(map[string]QueryParameter)

		for i, parameter := range parameters {
			body.Bindings[strconv.Itoa(i+1)] = parameter
		}
	}

	return c.NewRequest(
		ctx,
		http.MethodPost,
		c.StatementsApiUrl,
		uhttp.WithJSONBody(body),
		uhttp.WithAcceptJSONHeader(),
		WithHeader(AuthTypeHeaderKey, AuthTypeHeaderValue),
	)
}

func (c *Client) GetStatementResponse(ctx context.Context, statementHandle string) (*http.Request, error) {
	stringUrl, err := url.JoinPath(c.StatementsApiUrl.RawPath, statementHandle)
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
		WithHeader(AuthTypeHeaderKey, AuthTypeHeaderValue),
	)
}

func WithHeader(key, value string) uhttp.RequestOption {
	return func() (io.ReadWriter, map[string]string, error) {
		return nil, map[string]string{
			key: value,
		}, nil
	}
}
