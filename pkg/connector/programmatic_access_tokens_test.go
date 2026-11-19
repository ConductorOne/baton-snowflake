package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
)

type credentialIssueMock struct {
	userType    string
	defaultRole string
	roleGranted bool
	// showTokenName lets a test make SHOW return a name that does not match the token
	// just created, which is the "provider did not return the token" failure path.
	showTokenName string
	// denyPrefix makes every statement with this prefix answer 422/003001, the shape
	// Snowflake uses for an access-control denial.
	denyPrefix string
	// grantsStatus, when nonzero, fails SHOW GRANTS TO USER with that HTTP status
	// and a non-privilege error, the generic role-check failure.
	grantsStatus int
	// addStatus, when nonzero, fails the ADD PROGRAMMATIC ACCESS TOKEN statement with
	// that HTTP status, the ambiguous shape where Snowflake may have created the token.
	addStatus int
	// preflightDelay is spent inside DESCRIBE USER, standing in for the round-trip
	// latency between sampling the clock and issuing the ALTER USER.
	preflightDelay time.Duration
	// liveExpiry makes SHOW derive expires_at from the mock's own clock and the
	// statement's DAYS_TO_EXPIRY, the way Snowflake does, instead of a fixed instant.
	liveExpiry bool
	statements *[]string
}

var daysToExpiryRE = regexp.MustCompile(`DAYS_TO_EXPIRY = (\d+)`)

func serveCredentialIssueMock(t *testing.T, mock credentialIssueMock) *httptest.Server {
	t.Helper()
	var days int64 = 1
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var request snowflake.StatementsApiRequestBody
		if err := json.Unmarshal(body, &request); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		*mock.statements = append(*mock.statements, request.Statement)
		w.Header().Set("Content-Type", "application/json")
		if mock.grantsStatus != 0 && strings.HasPrefix(request.Statement, "SHOW GRANTS TO USER") {
			w.WriteHeader(mock.grantsStatus)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": "002003", "message": "SQL compilation error"})
			return
		}
		if mock.denyPrefix != "" && strings.HasPrefix(request.Statement, mock.denyPrefix) {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code":    "003001",
				"message": "SQL access control error: Insufficient privileges to operate on user",
			})
			return
		}

		switch {
		case strings.HasPrefix(request.Statement, "DESCRIBE USER"):
			time.Sleep(mock.preflightDelay)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"resultSetMetadata": map[string]any{"numRows": 12},
				"data": [][]string{
					{"NAME", "service-user"}, {"LOGIN_NAME", "service-user"}, {"DISPLAY_NAME", "Service User"},
					{"FIRST_NAME", ""}, {"LAST_NAME", ""}, {"EMAIL", ""}, {"DISABLED", "false"},
					{"SNOWFLAKE_LOCK", "false"}, {"DEFAULT_ROLE", mock.defaultRole}, {"TYPE", mock.userType},
					{"HAS_MFA", "false"}, {"COMMENT", ""},
				},
			})
		case strings.HasPrefix(request.Statement, "SHOW GRANTS TO USER"):
			data := [][]string{}
			if mock.roleGranted {
				data = append(data, []string{"ROLE", mock.defaultRole})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"resultSetMetadata": map[string]any{
					"numRows": len(data),
					"rowType": []map[string]any{{"name": "granted_on", "type": "text"}, {"name": "name", "type": "text"}},
				},
				"data": data,
			})
		case strings.Contains(request.Statement, "ADD PROGRAMMATIC ACCESS TOKEN"):
			if mock.addStatus != 0 {
				w.WriteHeader(mock.addStatus)
				return
			}
			if m := daysToExpiryRE.FindStringSubmatch(request.Statement); m == nil {
				t.Errorf("no DAYS_TO_EXPIRY in %q", request.Statement)
			} else if parsed, err := strconv.ParseInt(m[1], 10, 64); err != nil {
				t.Errorf("parse DAYS_TO_EXPIRY from %q: %v", request.Statement, err)
			} else {
				days = parsed
			}
			// Column metadata matches a live Snowflake response: cols are
			// [token_name, token_secret]. The secret is read by name, not position.
			_ = json.NewEncoder(w).Encode(map[string]any{
				"resultSetMetadata": map[string]any{
					"numRows": 1,
					"rowType": []map[string]any{
						{"name": "token_name", "type": "text"},
						{"name": "token_secret", "type": "text"},
					},
				},
				"data": [][]string{{"C1_REQUEST_1", "redacted"}},
			})
		case strings.HasPrefix(request.Statement, "SHOW USER PROGRAMMATIC ACCESS TOKENS"):
			expiresAt := "1893456000"
			if mock.liveExpiry {
				// Fractional seconds, the way Snowflake reports a TIMESTAMP_LTZ. Truncating
				// to whole seconds would hide sub-second clock drift, which is the whole
				// quantity the expiry-sampling test measures.
				expiresAt = strconv.FormatFloat(
					float64(time.Now().UTC().AddDate(0, 0, int(days)).UnixNano())/1e9, 'f', 6, 64,
				)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"resultSetMetadata": map[string]any{
					"numRows": 1,
					"rowType": []map[string]any{{"name": "name", "type": "text"}, {"name": "expires_at", "type": "timestamp_ltz"}},
				},
				"data": [][]string{{mock.showTokenName, expiresAt}},
			})
		case strings.Contains(request.Statement, "REMOVE PROGRAMMATIC ACCESS TOKEN"):
			_ = json.NewEncoder(w).Encode(map[string]any{"data": [][]string{}})
		default:
			t.Errorf("unexpected statement: %s", request.Statement)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}

func TestProgrammaticAccessTokenIDRoundTrip(t *testing.T) {
	userName, tokenName, err := parseProgrammaticAccessTokenID(programmaticAccessTokenID(`Mixed Case User`, "c1-request_1"))
	if err != nil {
		t.Fatalf("parseProgrammaticAccessTokenID() error = %v", err)
	}
	if userName != `Mixed Case User` || tokenName != "c1-request_1" {
		t.Fatalf("round trip = (%q, %q), want (%q, %q)", userName, tokenName, `Mixed Case User`, "c1-request_1")
	}
}

func TestProgrammaticAccessTokenListDeniedUserFailsSync(t *testing.T) {
	// A full sync is authoritative: an error-free empty result on a per-user
	// access-control denial would make C1 delete the user's previously synced
	// tokens and drop the revocation handles on credentials that stay live in
	// Snowflake. The denial therefore fails the sync, and the error carries the
	// privilege to grant; the OptInRequired annotation is what lets a tenant
	// without the privilege keep the type off.
	var statements []string
	server := serveCredentialIssueMock(t, credentialIssueMock{
		denyPrefix: "SHOW USER PROGRAMMATIC ACCESS TOKENS",
		statements: &statements,
	})
	defer server.Close()
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	resources, results, err := newProgrammaticAccessTokenBuilder(client).List(context.Background(),
		v2.ResourceId_builder{ResourceType: userResourceType.Id, Resource: "no-access-user"}.Build(), rs.SyncOpAttrs{})
	if !snowflake.IsInsufficientPrivileges(err) {
		t.Fatalf("List() error = %v, want the 003001 denial to fail the sync", err)
	}
	if resources != nil || results != nil {
		t.Fatalf("List() = (%v, %v), want nil resources and results on a denial", resources, results)
	}
	if !strings.Contains(err.Error(), "MODIFY PROGRAMMATIC AUTHENTICATION METHODS") {
		t.Fatalf("List() error %q does not name the privilege to grant", err)
	}
}
