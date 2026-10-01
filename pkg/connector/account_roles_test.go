package connector

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	snowflake "github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// lambdaResponseLimit is AWS Lambda's synchronous response payload limit. The transport
// base64-encodes the serialized response, so the encoded size is what must fit.
const lambdaResponseLimit = 6 * 1024 * 1024

// maxLengthName returns a 255-character identifier - Snowflake's limit - built from ch, with a
// numeric suffix so grantee names are distinct.
func maxLengthName(ch string, i int) string {
	suffix := fmt.Sprintf("%06d", i)
	return strings.Repeat(ch, 255-len(suffix)) + suffix
}

// TestAccountRoleGrantPagesFitLambdaLimit builds real Grants() pages with accountRoleGrant, sized
// the way pageAccountRoleGrantees would slice them under accountRoleGrantsPageLimit, and checks
// they fit the Lambda response limit with headroom. It also checks accountRoleGrantBytes
// really is an upper bound for every grant, which is what makes the page bound hold.
func TestAccountRoleGrantPagesFitLambdaLimit(t *testing.T) {
	short := func(i int) string { return fmt.Sprintf("USER_%06d", i) }
	tests := []struct {
		name        string
		roleName    string
		granteeType string
		granteeName func(i int) string
		wantRows    int // 0 means only the byte bound is checked
	}{
		{"typical names, users", "EMPLOYEES", "USER", short, accountRoleGrantsMaxRows},
		{"typical names, roles", "EMPLOYEES", "ROLE", short, accountRoleGrantsMaxRows},
		{"255 ASCII, roles", maxLengthName("R", 0), "ROLE", func(i int) string { return maxLengthName("g", i) }, 0},
		{"255 x 3-byte chars, users", maxLengthName("日", 0), "USER", func(i int) string { return maxLengthName("本", i) }, 0},
		{"255 x 4-byte chars, roles", maxLengthName("😀", 0), "ROLE", func(i int) string { return maxLengthName("😃", i) }, 0},
		{"short role, 255 x 4-byte grantees", "R", "ROLE", func(i int) string { return maxLengthName("😃", i) }, 0},
		{"255 x 4-byte role, short grantees", maxLengthName("😀", 0), "USER", short, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			role, err := accountRoleResource(&snowflake.AccountRole{Name: tc.roleName})
			require.NoError(t, err)
			limit := accountRoleGrantsPageLimit(role)

			// Fill a page the way pageAccountRoleGrantees does: stop at MaxRows, or before the
			// grantee whose estimate would push the page past MaxBytes.
			var grants []*v2.Grant
			estimated := 0
			for i := 0; len(grants) < limit.MaxRows; i++ {
				grantee := snowflake.AccountRoleGrantee{RoleName: tc.roleName, GranteeType: tc.granteeType, GranteeName: tc.granteeName(i)}
				rowBytes := limit.RowBytes(grantee)
				if len(grants) > 0 && estimated+rowBytes > limit.MaxBytes {
					break
				}
				g, err := accountRoleGrant(role, grantee)
				require.NoError(t, err)
				require.LessOrEqual(t, proto.Size(g), rowBytes, "accountRoleGrantBytes must not underestimate a grant")
				estimated += rowBytes
				grants = append(grants, g)
			}
			if tc.wantRows > 0 {
				assert.Len(t, grants, tc.wantRows, "the byte bound must not shrink pages of typical names")
			}

			// A realistic next-page token rides along in the same response.
			resp := v2.GrantsServiceListGrantsResponse_builder{List: grants, NextPageToken: strings.Repeat("t", 2048)}.Build()
			encoded := base64.StdEncoding.EncodedLen(proto.Size(resp))
			assert.Less(t, encoded, lambdaResponseLimit*3/4,
				"%d grants encode to %d bytes; must stay well under the %d-byte Lambda limit", len(grants), encoded, lambdaResponseLimit)
		})
	}
}
