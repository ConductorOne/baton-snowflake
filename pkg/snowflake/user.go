package snowflake

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

var (
	userStructFieldToColumnMap = map[string]string{
		"Username":         columnName,
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
		structFieldType:    columnType,
		"HasMfa":           "has_mfa",
		structFieldComment: columnComment,
	}

	// Sadly snowflake is inconsistent and returns different set of columns for DESC USER.
	// These fields are ignored when parsing DESCRIBE USER output.
	ignoredUserStructFieldsForDescribeOperation = []string{
		"HasRSAPublicKey",
		"HasPassword",
		"LastSuccessLogin", // May not be present for newly created users
	}

	secretStructFieldToColumnMap = map[string]string{
		structFieldCreatedOn:    columnCreatedOn,
		structFieldName:         columnName,
		structFieldSchemaName:   columnSchemaName,
		structFieldDatabaseName: columnDatabaseName,
		structFieldOwner:        columnOwner,
		structFieldComment:      columnComment,
		"SecretType":            "secret_type",
		"OAuthScopes":           "oauth_scopes",
		"OwnerRoleType":         "owner_role_type",
	}

	userDescriptionStructFieldToColumnMap = map[string]string{
		"Property":    "property",
		"Value":       "value",
		"Default":     "default",
		"Description": "description",
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

	UserRsa struct {
		Username                 string
		RsaPublicKeyLastSetTime  *time.Time
		RsaPublicKeyLastSetTime2 *time.Time
	}

	UserDescriptionProperty struct {
		Property    string
		Value       string
		Default     string
		Description string
	}

	ListUsersRawResponse struct {
		StatementsApiResponseBase
	}
	GetUserRawResponse struct {
		StatementsApiResponseBase
		Data [][]string `json:"data"`
	}

	ListSecretsRawResponse struct {
		StatementsApiResponseBase
	}

	RsaGetUserRawResponse struct {
		StatementsApiResponseBase
	}

	Secret struct {
		CreatedOn     time.Time
		Name          string
		SchemaName    string
		DatabaseName  string
		Owner         string
		Comment       string
		SecretType    string
		OAuthScopes   string
		OwnerRoleType string
	}
)

func (u *Secret) GetColumnName(fieldName string) string {
	return secretStructFieldToColumnMap[fieldName]
}

func (u *User) GetColumnName(fieldName string) string {
	return userStructFieldToColumnMap[fieldName]
}

func (u *UserDescriptionProperty) GetColumnName(fieldName string) string {
	return userDescriptionStructFieldToColumnMap[fieldName]
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

// listUsersStatement is the discovery-mode-dependent statement for one page of users.
// The ACCOUNT_USAGE form aliases its columns to the SHOW USERS names, so both feed the same
// ListUsersRawResponse parser.
func (c *Client) listUsersStatement(cursor string, limit int) string {
	if c.usesAccountUsage() {
		return accountUsageListUsersStatement(cursor, limit)
	}
	if cursor != "" {
		return fmt.Sprintf("SHOW USERS LIMIT %d FROM '%s';", limit, escapeStringLiteral(cursor))
	}
	return fmt.Sprintf("SHOW USERS LIMIT %d;", limit)
}

func (c *Client) ListUsers(ctx context.Context, cursor string, limit int) ([]User, error) {
	queries := []string{c.listUsersStatement(cursor, limit)}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListUsersRawResponse
	var apiErr SnowflakeError
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp1)
	if err != nil {
		return nil, c.classifyReadError(accountUsageUsersView, resp1, &apiErr, err)
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp2)
	if err != nil {
		return nil, c.classifyReadError(accountUsageUsersView, resp2, &apiErr, err)
	}
	if err := errIfStatementIncomplete(resp2, "the user listing"); err != nil {
		return nil, err
	}

	users, err := response.GetUsers()
	if err != nil {
		return nil, err
	}

	return users, nil
}

// SHOW USERS returns a superset of DESCRIBE USER fields, so cached entries are safe to reuse for GetUser.
func (c *Client) CacheUsers(ctx context.Context, ss sessions.SessionStore, users []User) error {
	if ss == nil || len(users) == 0 {
		return nil
	}
	m := make(map[string]*User, len(users))
	for i := range users {
		user := users[i]
		m[user.Username] = &user
	}
	if err := session.SetManyJSON(ctx, ss, m, userNamespace); err != nil {
		return fmt.Errorf("snowflake: cache users: %w", err)
	}
	return nil
}

func (c *Client) GetUser(ctx context.Context, ss sessions.SessionStore, username string) (*User, int, error) {
	if ss != nil {
		if cached, found, err := session.GetJSON[*User](ctx, ss, username, userNamespace); err == nil && found {
			return cached, http.StatusOK, nil
		}
	}

	// DESCRIBE USER requires OWNERSHIP on the target user, so under ACCOUNT_USAGE
	// discovery this reads the same view ListUsers does instead. That form returns one
	// row per user rather than DESCRIBE USER's property/value pairs, so it is parsed by
	// the row parser and returns early here.
	if c.usesAccountUsage() {
		return c.getUserFromAccountUsage(ctx, ss, username)
	}

	return c.describeUser(ctx, ss, username)
}

// DescribeUser reads a single user through DESCRIBE USER regardless of the configured
// discovery mode, as the session's default role.
//
// Provisioning must not read through ACCOUNT_USAGE. Those views lag the live account by up
// to three hours, so a user the connector just created is reliably absent from them - a
// post-create read-back or a pre-issuance property lookup would fail on a correct account.
//
// It deliberately does NOT consult the session store: that cache is shared with the
// discovery path, which under ACCOUNT_USAGE populates it from a view that lags the live
// account, and serving a provisioning read from there would defeat this function's whole
// purpose. Discovery reads go through GetUser, which honors the discovery mode and the cache.
//
// Use DescribeUserAsWriteRole instead for a user the connector itself just created; see the
// role note there.
func (c *Client) DescribeUser(ctx context.Context, _ sessions.SessionStore, username string) (*User, int, error) {
	return c.describeUserAsRole(ctx, nil, username, "")
}

// DescribeUserAsWriteRole is DescribeUser for a user the connector just created, run as the
// configured write role.
//
// DESCRIBE USER requires OWNERSHIP on the target user, and Snowflake gives ownership of a
// new object to the role that created it - so for a freshly created user the write role is
// the owner, and the session's default role may well not be. A tenant that moved user
// lifecycle onto a dedicated --write-role would otherwise create a user successfully and
// then fail to read it back.
//
// This does NOT generalize to users the connector did not create. The write role owns only
// what it created, so a pre-existing user is read by DescribeUser as the session's default
// role, which is the role the per-user OWNERSHIP setup grants.
func (c *Client) DescribeUserAsWriteRole(ctx context.Context, username string) (*User, int, error) {
	return c.describeUserAsRole(ctx, nil, username, c.writeRole())
}

// describeUser issues DESCRIBE USER as the session's default role. This is the discovery
// path: GetUser's callers hold no write privileges, and requiring the write role here would
// make a read-only sync depend on a provisioning role.
func (c *Client) describeUser(ctx context.Context, ss sessions.SessionStore, username string) (*User, int, error) {
	return c.describeUserAsRole(ctx, ss, username, "")
}

// describeUserAsRole is the shared DESCRIBE USER implementation. An empty role runs as the
// session's default role; a non-empty one is sent in the SQL API request body.
func (c *Client) describeUserAsRole(ctx context.Context, ss sessions.SessionStore, username, role string) (*User, int, error) {
	// Escape double quotes in username by doubling them before quoting
	escapedUsername := escapeDoubleQuotedIdentifier(username)
	queries := []string{
		fmt.Sprintf("DESCRIBE USER \"%s\";", escapedUsername),
	}

	req, err := c.PostStatementRequestWithRole(ctx, queries, role)
	if err != nil {
		return nil, 0, err
	}

	var response GetUserRawResponse
	var apiErr SnowflakeError
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp)
	if err != nil {
		statusCode := 0
		if resp != nil {
			statusCode = resp.StatusCode
		}
		return nil, statusCode, dedupeAPIError(err)
	}
	if err := errIfStatementIncomplete(resp, "DESCRIBE USER"); err != nil {
		return nil, statusCodeOf(resp), err
	}

	user, err := response.GetUser()
	if err != nil {
		return nil, resp.StatusCode, err
	}

	if ss != nil {
		// Best-effort: user was already fetched above and is returned regardless; a failed
		// cache write only costs a future GetUser call a redundant re-query.
		_ = session.SetJSON(ctx, ss, username, user, userNamespace)
	}

	return user, resp.StatusCode, nil
}

// SetUserDisabled enables or disables a Snowflake user via ALTER USER SET DISABLED,
// a targeted property-set statement (not a full-representation replace), so omitted
// user attributes (login_name, default_role, comment, etc.) are left untouched.
// Idempotent: setting the same DISABLED value on a user that's already in that state
// succeeds, so no "already in this state" error handling is needed here.
// Runs as the configured write role (UserAdminRole by default), matching
// CreateUserREST/DeleteUserREST - the session's default role is not guaranteed to have
// ALTER USER privilege on other users.
func (c *Client) SetUserDisabled(ctx context.Context, userName string, disabled bool) error {
	queries := []string{
		fmt.Sprintf("ALTER USER \"%s\" SET DISABLED = %t;", escapeDoubleQuotedIdentifier(userName), disabled),
	}

	req, err := c.PostStatementRequestWithRole(ctx, queries, c.writeRole())
	if err != nil {
		return fmt.Errorf("baton-snowflake: failed to set user %s disabled=%t: %w", userName, disabled, err)
	}

	var apiErr SnowflakeError
	resp, err := c.Do(req, uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp)
	if err != nil {
		return fmt.Errorf("baton-snowflake: failed to set user %s disabled=%t: %w", userName, disabled, dedupeAPIError(err))
	}
	// No statement-result GET follows, so this POST is the outcome leg. Without the guard a
	// 202 returns nil and the enable_user / disable_user actions report success for an
	// ALTER USER that has not executed.
	if err := errIfWriteIncomplete(resp, "ALTER USER SET DISABLED"); err != nil {
		return err
	}

	return nil
}

func (r *ListSecretsRawResponse) ListSecrets() ([]Secret, error) {
	var secrets []Secret
	for _, row := range r.Data {
		secret := &Secret{}
		if err := r.ResultSetMetadata.ParseRow(secret, row); err != nil {
			return nil, err
		}

		secrets = append(secrets, *secret)
	}
	return secrets, nil
}

// getUserFromAccountUsage is GetUser's ACCOUNT_USAGE implementation. It reuses the row
// parser rather than DESCRIBE USER's property/value parser.
//
// A user the view does not contain is an error, not a nil user: GetUser's callers
// dereference the returned user whenever err is nil, so returning (nil, nil) here would
// panic them. DESCRIBE USER behaves the same way - it fails rather than returning an empty
// result - so both modes keep the same contract.
func (c *Client) getUserFromAccountUsage(ctx context.Context, ss sessions.SessionStore, username string) (*User, int, error) {
	queries := []string{accountUsageGetUserStatement(username)}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, 0, err
	}

	var response ListUsersRawResponse
	var apiErr SnowflakeError
	resp1, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp1)
	if err != nil {
		return nil, statusCodeOf(resp1), classifyAccountUsageError(accountUsageUsersView, resp1, &apiErr, err)
	}

	req, err = c.GetStatementResponse(ctx, response.StatementHandle)
	if err != nil {
		return nil, 0, err
	}
	resp2, err := c.Do(req, uhttp.WithJSONResponse(&response), uhttp.WithErrorResponse(&apiErr))
	defer closeResponseBody(resp2)
	if err != nil {
		return nil, statusCodeOf(resp2), classifyAccountUsageError(accountUsageUsersView, resp2, &apiErr, err)
	}
	if err := errIfStatementIncomplete(resp2, "the ACCOUNT_USAGE user read"); err != nil {
		return nil, statusCodeOf(resp2), err
	}

	users, err := response.GetUsers()
	if err != nil {
		return nil, statusCodeOf(resp2), err
	}
	if len(users) == 0 {
		// ACCOUNT_USAGE lag is the likely cause for a recently created user, and it is
		// worth naming: the view can be up to three hours behind the live account.
		return nil, statusCodeOf(resp2), fmt.Errorf(
			"baton-snowflake: user %q not found in %s; the view lags the live account by up to "+
				"three hours, so a recently created user may not be visible yet",
			username, accountUsageUsersView,
		)
	}

	user := users[0]
	if ss != nil {
		// Best-effort, same as the SHOW path: a failed cache write only costs a future
		// GetUser call a redundant re-query.
		_ = session.SetJSON(ctx, ss, username, &user, userNamespace)
	}

	return &user, statusCodeOf(resp2), nil
}
