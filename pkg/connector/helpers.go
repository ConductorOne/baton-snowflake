package connector

import "fmt"

func wrapError(err error, message string) error {
	return fmt.Errorf("snowflake-connector: %s: %w", message, err)
}

const resourcePageSize = 50
