package snowflake

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// CreateUserRequest represents the request body for creating a user via REST API.
type CreateUserRequest struct {
	Name                  string `json:"name"`
	LoginName             string `json:"loginName,omitempty"`
	DisplayName           string `json:"displayName,omitempty"`
	FirstName             string `json:"firstName,omitempty"`
	LastName              string `json:"lastName,omitempty"`
	Email                 string `json:"email,omitempty"`
	Comment               string `json:"comment,omitempty"`
	Password              string `json:"password,omitempty"`
	MustChangePassword    bool   `json:"mustChangePassword,omitempty"`
	Disabled              bool   `json:"disabled,omitempty"`
	DefaultWarehouse      string `json:"defaultWarehouse,omitempty"`
	DefaultNamespace      string `json:"defaultNamespace,omitempty"`
	DefaultRole           string `json:"defaultRole,omitempty"`
	DefaultSecondaryRoles string `json:"defaultSecondaryRoles,omitempty"` // ALL or NONE
}

// CreateUserResponse represents the response from creating a user.
type CreateUserResponse struct {
	Status  string `json:"status,omitempty"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// DeleteUserOptions represents optional parameters for deleting a user.
type DeleteUserOptions struct {
	IfExists bool `json:"ifExists,omitempty"`
}

// SnowflakeError represents an error response from Snowflake REST API.
type SnowflakeError struct {
	Code   string `json:"code"`
	ErrMsg string `json:"message"`
}

// Message implements the ErrorResponse interface.
func (e *SnowflakeError) Message() string {
	if e.ErrMsg != "" {
		return e.ErrMsg
	}
	if e.Code != "" {
		return e.Code
	}
	return "unknown error"
}

// createUsersApiUrl creates the URL for the users REST API endpoint.
func createUsersApiUrl(accountUrl string) (*url.URL, error) {
	stringUrl, err := url.JoinPath(accountUrl, "api/v2/users")
	if err != nil {
		return nil, err
	}

	return url.Parse(stringUrl)
}

// createUserApiUrl creates the URL for a specific user REST API endpoint.
// The userName should be the actual identifier (case-sensitive if created with quotes).
// url.JoinPath will properly encode the path segment.
func createUserApiUrl(accountUrl string, userName string) (*url.URL, error) {
	stringUrl, err := url.JoinPath(accountUrl, "api/v2/users", userName)
	if err != nil {
		return nil, err
	}

	return url.Parse(stringUrl)
}

// doRequest is a helper method that wraps HTTP request logic for REST API calls.
// Returns the response headers, rate limit description, status code, and error.
func (c *Client) doRequest(
	ctx context.Context,
	method string,
	endpoint *url.URL,
	target interface{},
	body interface{},
	opts ...uhttp.RequestOption,
) (*http.Header, *v2.RateLimitDescription, int, error) {
	var requestOptions []uhttp.RequestOption
	requestOptions = append(requestOptions,
		uhttp.WithAcceptJSONHeader(),
		uhttp.WithHeader(AuthTypeHeaderKey, AuthTypeHeaderValue))

	// Append any additional options passed in
	requestOptions = append(requestOptions, opts...)

	if body != nil {
		requestOptions = append(requestOptions, uhttp.WithContentTypeJSONHeader(), uhttp.WithJSONBody(body))
	}

	request, err := c.NewRequest(ctx, method, endpoint, requestOptions...)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("baton-snowflake: failed to create request: %w", err)
	}

	var rateLimitData v2.RateLimitDescription
	var errorResponse SnowflakeError
	doOptions := []uhttp.DoOption{
		uhttp.WithRatelimitData(&rateLimitData),
		uhttp.WithErrorResponse(&errorResponse),
	}
	if target != nil {
		doOptions = append(doOptions, uhttp.WithJSONResponse(target))
	}

	response, err := c.Do(request, doOptions...)
	defer func() {
		if response == nil || response.Body == nil {
			return
		}
		_, _ = io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if closeErr != nil {
			log.Printf("baton-snowflake: warning: failed to close response body: %v", closeErr)
		}
	}()
	if err != nil {
		return nil, &rateLimitData, 0, fmt.Errorf("baton-snowflake: request failed: %w", err)
	}

	statusCode := response.StatusCode
	if statusCode >= 300 {
		// Try to extract error message from response
		if errorResponse.Code != "" || errorResponse.ErrMsg != "" {
			return &response.Header, &rateLimitData, statusCode, fmt.Errorf("baton-snowflake: snowflake API error: %s - %s", errorResponse.Code, errorResponse.Message())
		}
		return &response.Header, &rateLimitData, statusCode, fmt.Errorf("baton-snowflake: unexpected status code %d", statusCode)
	}

	return &response.Header, &rateLimitData, statusCode, nil
}

// CreateUserREST creates a new Snowflake user using the REST API.
// POST /api/v2/users.
// Returns completed=true if status code is 200, false if 202 (accepted but not completed).
func (c *Client) CreateUserREST(ctx context.Context, req *CreateUserRequest) (bool, *v2.RateLimitDescription, error) {
	l := ctxzap.Extract(ctx)

	usersApiUrl, err := createUsersApiUrl(c.AccountUrl)
	if err != nil {
		return false, nil, fmt.Errorf("baton-snowflake: failed to create users API URL: %w", err)
	}

	var response CreateUserResponse
	_, rateLimitDesc, statusCode, err := c.doRequest(ctx, http.MethodPost, usersApiUrl, &response, req, uhttp.WithHeader(RoleHeaderKey, UserAdminRole))
	if err != nil {
		return false, rateLimitDesc, err
	}

	completed := statusCode == http.StatusOK

	l.Debug("baton-snowflake: user creation request completed",
		zap.String("user_name", req.Name),
		zap.Int("status_code", statusCode),
		zap.Bool("completed", completed),
		zap.String("status", response.Status),
		zap.String("message", response.Message),
	)

	return completed, rateLimitDesc, nil
}

// DeleteUserREST deletes a Snowflake user using the REST API.
// DELETE /api/v2/users/{name}.
func (c *Client) DeleteUserREST(ctx context.Context, userName string, options *DeleteUserOptions) (*v2.RateLimitDescription, error) {
	l := ctxzap.Extract(ctx)

	userApiUrl, err := createUserApiUrl(c.AccountUrl, userName)
	if err != nil {
		return nil, fmt.Errorf("baton-snowflake: failed to create user API URL: %w", err)
	}

	// Add query parameters if options are provided
	if options != nil {
		query := userApiUrl.Query()
		if options.IfExists {
			query.Set("ifExists", "true")
		}
		userApiUrl.RawQuery = query.Encode()
	}

	_, rateLimitDesc, _, err := c.doRequest(ctx, http.MethodDelete, userApiUrl, nil, nil, uhttp.WithHeader(RoleHeaderKey, UserAdminRole))
	if err != nil {
		l.Error("baton-snowflake: failed to delete user",
			zap.String("user_name", userName),
			zap.Error(err),
		)
		return rateLimitDesc, err
	}

	l.Debug("baton-snowflake: user deleted successfully",
		zap.String("user_name", userName),
	)

	return rateLimitDesc, nil
}
