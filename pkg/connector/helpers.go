package connector

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/status"
)

func wrapError(err error, message string) error {
	return fmt.Errorf("snowflake-connector: %s: %w", message, err)
}

const resourcePageSize = 50

// isSnowflake422 returns true if the error originates from a Snowflake API
// 422 Unprocessable Entity response. The SDK maps this HTTP status into a gRPC
// status whose message is the raw HTTP status text.
func isSnowflake422(err error) bool {
	if st, ok := status.FromError(err); ok {
		return strings.Contains(st.Message(), "422 Unprocessable Entity")
	}
	return false
}

// quoteSnowflakeIdentifier properly escapes and quotes a Snowflake identifier.
// In Snowflake, double quotes inside identifiers must be escaped by doubling them.
// Example: o"donnel becomes "o""donnel".
func quoteSnowflakeIdentifier(identifier string) string {
	// Escape double quotes by doubling them
	escaped := strings.ReplaceAll(identifier, `"`, `""`)
	// Wrap in double quotes
	return fmt.Sprintf(`"%s"`, escaped)
}
