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
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
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
	UserAdminRole       = "USERADMIN"
	// GlobalOrgAdminRole is required by SHOW ORGANIZATION ACCOUNTS. The SQL API takes
	// the role in the request body, not the RoleHeaderKey header.
	GlobalOrgAdminRole = "GLOBALORGADMIN"
)

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

// statementResult is satisfied by every POST /api/v2/statements response type in this package:
// each one embeds StatementsApiResponseBase, which promotes resultSetInline.
type statementResult interface {
	resultSetInline() bool
}

// resultSetInline reports whether this decoded response already carries the statement's result
// set. Snowflake describes the columns in resultSetMetaData whenever the result is present, even
// for a zero-row result, so the column list is the signal - unlike len(Data), which cannot tell
// "no rows" apart from "no result yet".
func (r *StatementsApiResponseBase) resultSetInline() bool {
	return len(r.ResultSetMetadata.RowTypes) > 0
}

// needsStatementResultFetch reports whether a follow-up GET on the statement handle is required to
// obtain the result set of a statement just submitted via POST /api/v2/statements.
//
// Snowflake answers the POST with 200 and the complete result set - resultSetMetaData plus data -
// when the statement finishes inside the API's synchronous window, and with 202 and only a
// statement handle when it does not. Every SHOW command this connector issues is metadata-only and
// completes synchronously, so the follow-up GET returns a byte-identical body and is a wasted round
// trip. That matters at scale: a full sync issues one statement per table, so on a large account
// the redundant leg is tens of thousands of serial round trips.
//
// The check is deliberately two-sided - a 200 AND a result set actually present in the decoded
// body. Keying on the status alone would silently return an empty result for any response shaped
// differently from what this was written against, which is the one failure mode worth ruling out:
// an empty sync looks like deleted access, not like a bug.
func (c *Client) needsStatementResultFetch(ctx context.Context, resp *http.Response, res statementResult, op string) bool {
	status := 0
	if resp != nil {
		status = resp.StatusCode
	}

	// reason is empty when the result set is already in hand. Otherwise it names why this call
	// still has to spend the second round trip, which is the thing worth being able to grep for:
	// a sync that is unexpectedly slow, or unexpectedly empty, is diagnosed from these.
	reason := ""
	switch {
	case resp == nil:
		reason = "no_response"
	case status != http.StatusOK:
		// 202 is the documented async answer: the statement outlived the API's synchronous
		// window and the rows have to be collected from the handle.
		reason = "status_not_ok"
	case res == nil || !res.resultSetInline():
		// A 200 that carries no result set is not a shape Snowflake is documented to return.
		// Falling back to the GET keeps the sync correct if it ever happens; this reason is
		// how you would find out that it did.
		reason = "no_result_set"
	}

	ctxzap.Extract(ctx).Debug("snowflake: statement result",
		zap.String("op", op),
		zap.Int("post_status", status),
		zap.Bool("follow_up_get", reason != ""),
		zap.String("reason", reason),
	)
	return reason != ""
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

// fetchRemainingPartitions GETs partitions 1..numPartitions-1 of a statement whose partition 0
// already arrived (inline on the POST, or via the plain handle GET) and returns their rows,
// combined in partition order, plus the number of rows actually fetched from each one (so the
// caller can compare them against the counts Snowflake declared).
//
// Only partition 0 carries resultSetMetadata (Snowflake does not repeat the column layout on
// later partitions), so callers append the returned rows directly onto the partition-0 response's
// Data field and keep parsing with that response's RowTypes, rather than decoding each partition
// into a RowTypes of its own.
//
// numPartitions is the caller's own len(ResultSetMetadata.PartitionInfo) read on the partition-0
// response; a result with <= 1 partitions has nothing left to fetch and should not call this.
func (c *Client) fetchRemainingPartitions(ctx context.Context, handle string, numPartitions int) ([][]string, []int, error) {
	var rows [][]string
	var fetchedRowCounts []int
	for partitionID := 1; partitionID < numPartitions; partitionID++ {
		req, err := c.GetStatementPartition(ctx, handle, partitionID)
		if err != nil {
			return nil, nil, err
		}

		var partition StatementsApiResponseBase
		var apiErr SnowflakeError
		resp, err := c.Do(req, uhttp.WithJSONResponse(&partition), uhttp.WithErrorResponse(&apiErr))
		closeResponseBody(resp)
		if err != nil {
			return nil, nil, dedupeAPIError(err)
		}

		rows = append(rows, partition.Data...)
		fetchedRowCounts = append(fetchedRowCounts, len(partition.Data))
	}
	return rows, fetchedRowCounts, nil
}

// drainRemainingPartitions checks resp (already populated from a statement's partition-0 reply -
// inline on the POST, or via the plain handle GET) for additional partitions and, if present,
// fetches and appends them to resp.Data so callers get the complete result from a single
// resp.ListX()-style call afterward.
//
// Snowflake splits large results into additional partitions by response byte size, not row
// count, so even a small page can span several. Only partition 0 is returned inline; this
// function fetches the rest so callers see the complete result.
//
// A multi-partition result is an expected condition, so it is logged at Info. A drain whose row
// count still disagrees with the count Snowflake declared returns an error: a short page would
// otherwise be read by paginated callers as the last page and silently end pagination.
func (c *Client) drainRemainingPartitions(ctx context.Context, op string, resp *StatementsApiResponseBase, extra ...zap.Field) error {
	l := ctxzap.Extract(ctx)
	numPartitions := len(resp.ResultSetMetadata.PartitionInfo)

	if numPartitions <= 1 {
		// No declared second partition, but a response can still under-report its own row
		// count without ever claiming more than one partition (e.g. partitionInfo omitted
		// entirely, or present with a single zero-length entry). This is the no-extra-request
		// version of the invariant check below, and it has to fail the same way that one does:
		// a short page here is read by every paginated caller (ListTablesInSchema, ListUsers,
		// ...) as "this is the last page," which stops pagination and silently drops every
		// later page too - the exact amplified failure mode this whole fix exists to close, so
		// a Warn-and-continue here would just recreate it under a different condition.
		if resp.ResultSetMetadata.NumRows > 0 && len(resp.Data) != resp.ResultSetMetadata.NumRows {
			return fmt.Errorf("baton-snowflake: %s: got %d rows but Snowflake declared %d (statementHandle=%s)",
				op, len(resp.Data), resp.ResultSetMetadata.NumRows, resp.StatementHandle)
		}
		return nil
	}

	declaredRowCounts := make([]int, numPartitions)
	for i, p := range resp.ResultSetMetadata.PartitionInfo {
		declaredRowCounts[i] = p.RowCount
	}
	partition0Rows := len(resp.Data)

	baseFields := append([]zap.Field{
		zap.String("op", op),
		zap.String("statementHandle", resp.StatementHandle),
		zap.Int("numPartitions", numPartitions),
		zap.Int("numRows", resp.ResultSetMetadata.NumRows),
		zap.Ints("declaredPartitionRowCounts", declaredRowCounts),
	}, extra...)
	l.Info("snowflake: statement result spans multiple partitions, fetching remaining pages", baseFields...)

	rest, restRowCounts, err := c.fetchRemainingPartitions(ctx, resp.StatementHandle, numPartitions)
	if err != nil {
		return fmt.Errorf("baton-snowflake: failed to fetch remaining partitions for %s: %w", op, err)
	}
	resp.Data = append(resp.Data, rest...)

	fetchedRowCounts := append([]int{partition0Rows}, restRowCounts...)
	totalRows := len(resp.Data)
	if resp.ResultSetMetadata.NumRows > 0 && totalRows != resp.ResultSetMetadata.NumRows {
		// The condition worth caring about: the partition walk ran to completion by its own
		// accounting (every declared partition was fetched without error) and still didn't add
		// up to the row count Snowflake declared up front. That means either a partition
		// under-reported its own rows, or NumRows/PartitionInfo disagree in some other way this
		// code wasn't written against - a case the simple "numPartitions > 1" check above
		// cannot catch on its own. The error carries declared vs fetched per-partition counts
		// directly, rather than also logging them here - this failure already propagates to the
		// caller, which logs it; a second log line at this level would just duplicate it.
		return fmt.Errorf("baton-snowflake: %s: fetched %d rows across %d partitions (declared=%v fetched=%v) but Snowflake declared %d total (statementHandle=%s)",
			op, totalRows, numPartitions, declaredRowCounts, fetchedRowCounts, resp.ResultSetMetadata.NumRows, resp.StatementHandle)
	}

	l.Info("snowflake: finished draining statement result partitions", append(baseFields,
		zap.Ints("fetchedPartitionRowCounts", fetchedRowCounts),
		zap.Int("totalRowsFetched", totalRows),
	)...)
	return nil
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
