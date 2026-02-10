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
	User    User   `json:"user"`
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
func createUserApiUrl(accountUrl string, userName string) (*url.URL, error) {
	stringUrl, err := url.JoinPath(accountUrl, "api/v2/users", userName)
	if err != nil {
		return nil, err
	}

	return url.Parse(stringUrl)
}

// doRequest is a helper method that wraps HTTP request logic for REST API calls.
func (c *Client) doRequest(ctx context.Context, method string, endpoint *url.URL, target interface{}, body interface{}, opts ...uhttp.RequestOption) (*http.Header, *v2.RateLimitDescription, error) {
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
		return nil, nil, fmt.Errorf("failed to create request: %w", err)
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
	if err != nil {
		return nil, &rateLimitData, fmt.Errorf("request failed: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if closeErr != nil {
			log.Printf("warning: failed to close response body: %v", closeErr)
		}
	}()

	if response.StatusCode >= 300 {
		// Try to extract error message from response
		if errorResponse.Code != "" || errorResponse.ErrMsg != "" {
			return &response.Header, &rateLimitData, fmt.Errorf("snowflake API error: %s - %s", errorResponse.Code, errorResponse.Message())
		}
		return &response.Header, &rateLimitData, fmt.Errorf("unexpected status code %d", response.StatusCode)
	}

	return &response.Header, &rateLimitData, nil
}

// CreateUserREST creates a new Snowflake user using the REST API.
// POST /api/v2/users.
func (c *Client) CreateUserREST(ctx context.Context, req *CreateUserRequest) (*User, *v2.RateLimitDescription, error) {
	l := ctxzap.Extract(ctx)

	usersApiUrl, err := createUsersApiUrl(c.AccountUrl)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create users API URL: %w", err)
	}

	var response CreateUserResponse
	_, rateLimitDesc, err := c.doRequest(ctx, http.MethodPost, usersApiUrl, &response, req, uhttp.WithHeader(RoleHeaderKey, UserAdminRole))
	if err != nil {
		l.Error("failed to create user",
			zap.String("user_name", req.Name),
			zap.Error(err),
		)
		return nil, rateLimitDesc, err
	}

	l.Debug("user created successfully",
		zap.String("user_name", response.User.Username),
	)

	return &response.User, rateLimitDesc, nil
}

// DeleteUserREST deletes a Snowflake user using the REST API.
// DELETE /api/v2/users/{name}.
func (c *Client) DeleteUserREST(ctx context.Context, userName string, options *DeleteUserOptions) (*v2.RateLimitDescription, error) {
	l := ctxzap.Extract(ctx)

	userApiUrl, err := createUserApiUrl(c.AccountUrl, userName)
	if err != nil {
		return nil, fmt.Errorf("failed to create user API URL: %w", err)
	}

	// Add query parameters if options are provided
	if options != nil {
		query := userApiUrl.Query()
		if options.IfExists {
			query.Set("ifExists", "true")
		}
		userApiUrl.RawQuery = query.Encode()
	}

	_, rateLimitDesc, err := c.doRequest(ctx, http.MethodDelete, userApiUrl, nil, nil, uhttp.WithHeader(RoleHeaderKey, UserAdminRole))
	if err != nil {
		l.Error("failed to delete user",
			zap.String("user_name", userName),
			zap.Error(err),
		)
		return rateLimitDesc, err
	}

	l.Debug("user deleted successfully",
		zap.String("user_name", userName),
	)

	return rateLimitDesc, nil
}
