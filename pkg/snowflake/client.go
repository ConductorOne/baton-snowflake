package snowflake

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
)

var (
	accountRoleNamespace        = sessions.WithPrefix("account_role")
	userNamespace               = sessions.WithPrefix("user")
	tableGrantsNamespace        = sessions.WithPrefix("table_grants")
	tableGrantsPartialNamespace = sessions.WithPrefix("table_grants_partial")
)

const (
	AuthTypeHeaderKey   = "X-Snowflake-Authorization-Token-Type"
	AuthTypeHeaderValue = "KEYPAIR_JWT"
	RoleHeaderKey       = "X-Snowflake-Role"
	// UserAdminRole is the default role for write operations (user lifecycle and
	// programmatic access tokens). It is only a default: a tenant that cannot grant the
	// USERADMIN system role to its service user overrides it with Client.WriteRole so a
	// custom role holding just CREATE USER on the account and MODIFY on the in-scope
	// users can be used instead. See ClientOption WithWriteRole.
	UserAdminRole = "USERADMIN"
	// GlobalOrgAdminRole is the default role for SHOW ORGANIZATION ACCOUNTS. The SQL API
	// takes the role in the request body, not the RoleHeaderKey header. Overridable via
	// Client.OrganizationRole so a delegated org role can be used in place of the
	// top-level org admin.
	GlobalOrgAdminRole = "GLOBALORGADMIN"
)

// DiscoveryMode selects which Snowflake surface the connector reads inventory from.
type DiscoveryMode string

const (
	// DiscoveryModeShow is the default: discovery runs through SHOW commands. Every SHOW
	// command returns only the objects the session role holds at least one privilege on,
	// so full-account visibility requires either per-object OWNERSHIP or account-level
	// MANAGE GRANTS.
	DiscoveryModeShow DiscoveryMode = "show"
	// DiscoveryModeAccountUsage reads inventory from the SNOWFLAKE.ACCOUNT_USAGE schema
	// instead. It needs no MANAGE GRANTS and no per-object grants at all - only the
	// SNOWFLAKE.ACCOUNT_USAGE SECURITY_VIEWER and OBJECT_VIEWER database roles - at the
	// cost of Snowflake's documented ACCOUNT_USAGE latency (90 minutes to 3 hours
	// depending on the view) and a running warehouse to execute the SELECTs.
	DiscoveryModeAccountUsage DiscoveryMode = "account_usage"
)

// ParseDiscoveryMode maps a configured discovery-mode string to its DiscoveryMode.
// An empty value is the SHOW path, so an unset config keeps today's behavior.
func ParseDiscoveryMode(s string) (DiscoveryMode, error) {
	switch DiscoveryMode(strings.ToLower(strings.TrimSpace(s))) {
	case "", DiscoveryModeShow:
		return DiscoveryModeShow, nil
	case DiscoveryModeAccountUsage:
		return DiscoveryModeAccountUsage, nil
	default:
		return "", fmt.Errorf("baton-snowflake: unknown discovery mode %q (expected %q or %q)", s, DiscoveryModeShow, DiscoveryModeAccountUsage)
	}
}

const (
	rowTypeString       = "text"
	rowTypeTimestampLtz = "timestamp_ltz"
	rowNull             = "null"
)

type (
	Client struct {
		uhttp.BaseHttpClient
		JWTConfig

		AccountUrl       string
		StatementsApiUrl *url.URL

		// WriteRole is the Snowflake role the connector asks for on write operations:
		// user create/delete (REST), ALTER USER SET DISABLED, and programmatic access
		// token add/remove. Defaults to UserAdminRole.
		WriteRole string
		// OrganizationRole is the Snowflake role the connector asks for on
		// organization-scoped reads (SHOW ORGANIZATION ACCOUNTS). Defaults to
		// GlobalOrgAdminRole.
		OrganizationRole string
		// DiscoveryMode selects the inventory read path. Defaults to DiscoveryModeShow.
		DiscoveryMode DiscoveryMode
	}
	PartitionInfo struct {
		RowCount int `json:"rowCount"`
	}
	ResultSetMetadata struct {
		NumRows       int             `json:"numRows"`
		RowTypes      []RowType       `json:"rowType"`
		PartitionInfo []PartitionInfo `json:"partitionInfo"`
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
		Role       string                      `json:"role,omitempty"`
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
	if row[i] == "" || row[i] == rowNull {
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

// ClientOption customizes a Client at construction time. Options are variadic so the
// three-argument New call used across the tests keeps working and keeps defaulting to the
// Snowflake built-in system roles.
type ClientOption func(*Client)

// WithWriteRole overrides the role used for write operations. An empty or whitespace-only
// role is ignored so a config field left blank falls back to UserAdminRole rather than
// sending an empty role (which would silently run as the session's default role).
func WithWriteRole(role string) ClientOption {
	return func(c *Client) {
		if r := strings.TrimSpace(role); r != "" {
			c.WriteRole = r
		}
	}
}

// WithOrganizationRole overrides the role used for organization-scoped reads. Blank is
// ignored, same rationale as WithWriteRole.
func WithOrganizationRole(role string) ClientOption {
	return func(c *Client) {
		if r := strings.TrimSpace(role); r != "" {
			c.OrganizationRole = r
		}
	}
}

// WithDiscoveryMode selects the inventory read path. An empty mode is ignored.
func WithDiscoveryMode(mode DiscoveryMode) ClientOption {
	return func(c *Client) {
		if mode != "" {
			c.DiscoveryMode = mode
		}
	}
}

func New(accountUrl string, jwtConfig JWTConfig, httpClient *http.Client, opts ...ClientOption) (*Client, error) {
	statementsApiUrl, err := createStatementsApiUrl(accountUrl)
	if err != nil {
		return nil, err
	}

	client := &Client{
		BaseHttpClient:   *uhttp.NewBaseHttpClient(httpClient),
		JWTConfig:        jwtConfig,
		AccountUrl:       accountUrl,
		StatementsApiUrl: statementsApiUrl,
		WriteRole:        UserAdminRole,
		OrganizationRole: GlobalOrgAdminRole,
		DiscoveryMode:    DiscoveryModeShow,
	}
	for _, opt := range opts {
		opt(client)
	}

	return client, nil
}

// writeRole returns the role to send on write operations, falling back to the USERADMIN
// default for a zero-valued Client (tests construct Client literals directly).
func (c *Client) writeRole() string {
	if r := strings.TrimSpace(c.WriteRole); r != "" {
		return r
	}
	return UserAdminRole
}

// organizationRole returns the role to send on organization-scoped reads, falling back to
// the GLOBALORGADMIN default for a zero-valued Client.
func (c *Client) organizationRole() string {
	if r := strings.TrimSpace(c.OrganizationRole); r != "" {
		return r
	}
	return GlobalOrgAdminRole
}

// usesAccountUsage reports whether inventory reads should go through the
// SNOWFLAKE.ACCOUNT_USAGE views rather than SHOW commands.
func (c *Client) usesAccountUsage() bool {
	return c.DiscoveryMode == DiscoveryModeAccountUsage
}

func (c *Client) PostStatementRequest(ctx context.Context, queries []string) (*http.Request, error) {
	return c.PostStatementRequestWithRole(ctx, queries, "")
}

func (c *Client) PostStatementRequestWithRole(ctx context.Context, queries []string, role string) (*http.Request, error) {
	body := &StatementsApiRequestBody{Role: role}
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

// https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses#getting-the-results-from-the-response.
func (c *Client) GetStatementPartition(ctx context.Context, statementHandle string, partitionID int) (*http.Request, error) {
	stringUrl, err := url.JoinPath(c.StatementsApiUrl.String(), statementHandle)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(stringUrl)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("partition", strconv.Itoa(partitionID))
	u.RawQuery = q.Encode()

	return c.NewRequest(
		ctx,
		http.MethodGet,
		u,
		uhttp.WithAcceptJSONHeader(),
		uhttp.WithHeader(AuthTypeHeaderKey, AuthTypeHeaderValue),
	)
}

func Contains[T comparable](ts []T, val T) bool {
	for _, t := range ts {
		if t == val {
			return true
		}
	}
	return false
}

// closeResponseBody drains and closes the response body if it exists.
// This ensures proper resource cleanup and allows connection reuse.
func closeResponseBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}

// uhttp.Do always joins a generic "<code> <http status text>" error ahead of
// WithErrorResponse's detailed one, and it's the generic one that carries any
// rate-limit gRPC details. Keep the detailed message but carry those details
// over so rate-limit-aware retry still sees them.
//
// Requires WithErrorResponse to be the last DoOption passed to c.Do: uhttp.Do
// joins errors in call order, so a later option would silently take its slot.
func dedupeAPIError(err error) error {
	if err == nil {
		return nil
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return err
	}
	errs := joined.Unwrap()
	if len(errs) == 0 {
		return err
	}
	last := errs[len(errs)-1]

	generic, ok := status.FromError(errs[0])
	if !ok || len(generic.Details()) == 0 {
		return last
	}
	lastStatus, ok := status.FromError(last)
	if !ok {
		return last
	}

	var details []protoadapt.MessageV1
	for _, d := range generic.Details() {
		if m, ok := d.(protoadapt.MessageV1); ok {
			details = append(details, m)
		}
	}
	if len(details) == 0 {
		return last
	}
	merged, err := lastStatus.WithDetails(details...)
	if err != nil {
		return last
	}
	return merged.Err()
}
