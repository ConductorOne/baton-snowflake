package connector

import (
	"strings"

	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

// normalizeFp drops the "SHA256:" prefix DESCRIBE USER adds; the rest is base64 and stays case-sensitive.
func normalizeFp(fp string) string {
	fp = strings.TrimSpace(fp)
	if len(fp) >= len("SHA256:") && strings.EqualFold(fp[:len("SHA256:")], "SHA256:") {
		fp = fp[len("SHA256:"):]
	}
	return fp
}

func findCredential(credentials []snowflake.Credential, match func(snowflake.Credential) bool) *snowflake.Credential {
	for i := range credentials {
		if match(credentials[i]) {
			return &credentials[i]
		}
	}
	return nil
}
