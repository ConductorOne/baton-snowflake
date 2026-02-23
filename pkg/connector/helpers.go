package connector

import (
	"fmt"
	"strings"
)

func wrapError(err error, message string) error {
	return fmt.Errorf("snowflake-connector: %s: %w", message, err)
}

const resourcePageSize = 50

// quoteSnowflakeIdentifier properly escapes and quotes a Snowflake identifier.
// In Snowflake, double quotes inside identifiers must be escaped by doubling them.
// Example: o"donnel becomes "o""donnel".
func quoteSnowflakeIdentifier(identifier string) string {
	// Escape double quotes by doubling them
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	// Wrap in double quotes
	return fmt.Sprintf(`"%s"`, escaped)
}
