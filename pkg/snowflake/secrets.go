package snowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

const snowflakeDateFormat = "2006-01-02 15:04:05.999"

func (c *Client) ListSecrets(ctx context.Context, database string) ([]Secret, error) {
	l := ctxzap.Extract(ctx)

	queries := []string{
		fmt.Sprintf("SHOW SECRETS IN DATABASE \"%s\";", database),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response ListSecretsRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnprocessableEntity {
			var errMsg struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}

			err := json.NewDecoder(resp.Body).Decode(&errMsg)
			if err != nil {
				return nil, err
			}

			// code: 003001
			// message: SQL access control error:\nInsufficient privileges to operate on database 'DB'
			if errMsg.Code == "003001" {
				l.Warn("Insufficient privileges to operate on database", zap.String("database", database))
			} else {
				l.Error(errMsg.Message, zap.String("database", database))
			}

			// Ignore if the account/role does not have permission to show secrets of database
			return nil, nil
		}

		return nil, err
	}
	defer resp.Body.Close()

	secrets, err := response.ListSecrets()
	if err != nil {
		return nil, err
	}

	return secrets, nil
}

func (c *Client) UserRsa(ctx context.Context, username string) (*UserRsa, error) {
	queries := []string{
		fmt.Sprintf("DESCRIBE USER \"%s\";", username),
	}

	req, err := c.PostStatementRequest(ctx, queries)
	if err != nil {
		return nil, err
	}

	var response RsaGetUserRawResponse
	resp, err := c.Do(req, uhttp.WithJSONResponse(&response))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	secrets, err := response.GetUserRsa(ctx)
	if err != nil {
		return nil, err
	}

	return secrets, nil
}

func findUserDescriptionPropertyValue(properties []UserDescriptionProperty, name string) string {
	for _, property := range properties {
		if property.Property == name {
			return property.Value
		}
	}

	return ""
}

func (r *RsaGetUserRawResponse) GetUserRsa(ctx context.Context) (*UserRsa, error) {
	rsa := &UserRsa{}

	var userDescriptions []UserDescriptionProperty
	for _, row := range r.Data {
		description := &UserDescriptionProperty{}
		if err := r.ResultSetMetadata.ParseRow(description, row); err != nil {
			return nil, err
		}

		userDescriptions = append(userDescriptions, *description)
	}

	rsa.Username = findUserDescriptionPropertyValue(userDescriptions, "NAME")

	rsa1 := findUserDescriptionPropertyValue(userDescriptions, "RSA_PUBLIC_KEY_LAST_SET_TIME")
	if rsa1 != "" && rsa1 != rowNull {
		rsa1Time, err := time.Parse(snowflakeDateFormat, rsa1)
		if err != nil {
			return nil, err
		}
		rsa.RsaPublicKeyLastSetTime = &rsa1Time
	}

	rsa2 := findUserDescriptionPropertyValue(userDescriptions, "RSA_PUBLIC_KEY_2_LAST_SET_TIME")
	if rsa2 != "" && rsa2 != rowNull {
		rsa2Time, err := time.Parse(snowflakeDateFormat, rsa2)
		if err != nil {
			return nil, err
		}

		rsa.RsaPublicKeyLastSetTime2 = &rsa2Time
	}

	return rsa, nil
}
