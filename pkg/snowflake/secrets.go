package snowflake

import (
	"context"
	"fmt"
	"time"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

const snowflakeDateFormat = "2006-01-02 15:04:05.999"

func (c *Client) ListSecrets(ctx context.Context, database string) ([]Secret, error) {
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
