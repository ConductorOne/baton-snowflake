package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// describeUserBody is a minimal DESCRIBE USER result containing every property GetUser reads.
const describeUserBody = `{"resultSetMetaData":{"numRows":0},"code":"090001","statementHandle":"h1",
"data":[["NAME","alice"],["LOGIN_NAME","alice"],["DISPLAY_NAME","alice"],["FIRST_NAME",""],["LAST_NAME",""],
["EMAIL",""],["DISABLED","false"],["SNOWFLAKE_LOCK","false"],["DEFAULT_ROLE",""],["TYPE","PERSON"],
["HAS_MFA","false"],["COMMENT",""]]}`

func createAccountWith(t *testing.T, opts *v2.LocalCredentialOptions) (map[string]any, int) {
	t.Helper()
	var createBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && r.URL.Path == "/api/v2/users" {
			raw, _ := io.ReadAll(r.Body)
			require.NoError(t, json.Unmarshal(raw, &createBody))
			_, _ = w.Write([]byte(`{"status":"created"}`))
			return
		}
		// Any statement (DESCRIBE USER) gets the canned describe result.
		resp := strings.Replace(describeUserBody, `"numRows":0`, `"numRows":12,"rowType":[{"name":"property","type":"text"},{"name":"value","type":"text"}]`, 1)
		_, _ = w.Write([]byte(resp))
	}))
	defer srv.Close()

	client, err := snowflake.New(srv.URL, snowflake.JWTConfig{}, srv.Client())
	require.NoError(t, err)
	b := newUserBuilder(client, secretOptions{})

	profile, err := structpb.NewStruct(map[string]any{"name": "alice"})
	require.NoError(t, err)
	_, plaintext, _, err := b.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, opts)
	require.NoError(t, err)
	return createBody, len(plaintext)
}

func TestCreateAccountNoPassword(t *testing.T) {
	body, nPlaintext := createAccountWith(t, &v2.LocalCredentialOptions{
		Options: &v2.LocalCredentialOptions_NoPassword_{NoPassword: &v2.LocalCredentialOptions_NoPassword{}},
	})
	require.NotNil(t, body)
	require.NotContains(t, body, "password")
	require.NotContains(t, body, "must_change_password")
	require.Zero(t, nPlaintext)
}

func TestCreateAccountRandomPasswordStillSendsPassword(t *testing.T) {
	body, nPlaintext := createAccountWith(t, &v2.LocalCredentialOptions{
		Options: &v2.LocalCredentialOptions_RandomPassword_{RandomPassword: &v2.LocalCredentialOptions_RandomPassword{Length: 16}},
	})
	require.NotEmpty(t, body["password"])
	require.Equal(t, 1, nPlaintext)
}

func TestCreateAccountCapabilityDetailsIncludesNoPassword(t *testing.T) {
	b := &userBuilder{}
	d, _, err := b.CreateAccountCapabilityDetails(context.Background())
	require.NoError(t, err)
	require.Contains(t, d.SupportedCredentialOptions, v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_NO_PASSWORD)
	require.Equal(t, v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_RANDOM_PASSWORD, d.PreferredCredentialOption)
}

func TestCreateAccountConflictAndSuccess(t *testing.T) {
	for _, tc := range []struct {
		name         string
		username     string
		createStatus int
		disabled     bool
		locked       bool
	}{
		{name: "conflict", username: "alice", createStatus: http.StatusConflict},
		{name: "mixed case", username: "Alice", createStatus: http.StatusConflict},
		{name: "embedded quote", username: `al"ice`, createStatus: http.StatusConflict},
		{name: "literal surrounding quotes", username: `"Alice"`, createStatus: http.StatusConflict},
		{name: "disabled", username: "alice", createStatus: http.StatusConflict, disabled: true},
		{name: "locked", username: "alice", createStatus: http.StatusConflict, locked: true},
		{name: "created", username: "alice", createStatus: http.StatusOK},
		{name: "accepted", username: "Alice", createStatus: http.StatusAccepted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user := &snowflake.User{
				Username: tc.username, Login: "existing-login", DisplayName: "Existing User",
				FirstName: "Existing", LastName: "User", Email: "existing@example.com",
				Comment: "existing comment", Type: "PERSON", HasMfa: true,
				Disabled: tc.disabled, Locked: tc.locked,
			}
			builder, calls := createAccountFixture(t, tc.username, tc.createStatus, http.StatusOK, user)
			profile, err := structpb.NewStruct(map[string]any{
				"name": tc.username, "email": user.Email, "disabled": false,
			})
			require.NoError(t, err)
			result, plaintext, annos, err := builder.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, &v2.LocalCredentialOptions{
				Options: &v2.LocalCredentialOptions_RandomPassword_{RandomPassword: &v2.LocalCredentialOptions_RandomPassword{Length: 16}},
			})
			require.NoError(t, err)
			expected, err := userResource(context.Background(), user, builder.secrets)
			require.NoError(t, err)
			if tc.createStatus == http.StatusConflict {
				existing, ok := result.(*v2.CreateAccountResponse_AlreadyExistsResult)
				require.True(t, ok, "expected already-exists result, got %T", result)
				require.True(t, proto.Equal(expected, existing.Resource), "resource must match the fetched user, not the request")
				require.Empty(t, plaintext, "a generated password was not set on the existing user")
			} else {
				created, ok := result.(*v2.CreateAccountResponse_SuccessResult)
				require.True(t, ok, "expected success result, got %T", result)
				require.True(t, proto.Equal(expected, created.Resource))
				require.Len(t, plaintext, 1)
				require.Len(t, plaintext[0].Bytes, 16)
			}
			rateLimit := &v2.RateLimitDescription{}
			found, err := annos.Pick(rateLimit)
			require.NoError(t, err)
			require.True(t, found)
			require.EqualValues(t, 100, rateLimit.GetLimit())
			require.Equal(t, []string{"create", "describe"}, *calls, "no updates to existing users are allowed")
		})
	}
}

func TestCreateAccountConflictIdentity(t *testing.T) {
	for _, tc := range []struct {
		name          string
		email         string
		login         string
		existingEmail string
		existingLogin string
		wantMismatch  string
		unreadable    bool
	}{
		{name: "different email refuses", email: "requested@example.com", existingEmail: "existing@example.com", wantMismatch: "email"},
		{name: "different login refuses", login: "requested-login", existingLogin: "existing-login", wantMismatch: "login"},
		{name: "matching identity", email: "alice@example.com", login: "alice", existingEmail: "alice@example.com", existingLogin: "alice"},
		{name: "email case insensitive", email: "Alice@Example.COM", existingEmail: "alice@example.com"},
		{name: "login case insensitive", login: "ALICE", existingLogin: "alice"},
		{
			name: "matching email does not override login mismatch", email: "alice@example.com", existingEmail: "alice@example.com",
			login: "requested-login", existingLogin: "existing-login", wantMismatch: "login",
		},
		{
			name: "matching login does not override email mismatch", email: "requested@example.com", existingEmail: "existing@example.com",
			login: "alice", existingLogin: "alice", wantMismatch: "email",
		},
		{
			name: "email mismatch reported first", email: "requested@example.com", existingEmail: "existing@example.com",
			login: "requested-login", existingLogin: "existing-login", wantMismatch: "email",
		},
		{name: "request email missing checks matching login", login: "ALICE", existingEmail: "alice@example.com", existingLogin: "alice"},
		{name: "request email missing checks conflicting login", login: "requested-login", existingEmail: "alice@example.com", existingLogin: "existing-login", wantMismatch: "login"},
		{name: "existing email missing checks matching login", email: "alice@example.com", login: "ALICE", existingLogin: "alice"},
		{name: "existing email missing checks conflicting login", email: "alice@example.com", login: "requested-login", existingLogin: "existing-login", wantMismatch: "login"},
		{name: "request login missing checks email", email: "ALICE@example.com", existingEmail: "alice@example.com", existingLogin: "alice"},
		{name: "existing login missing refuses despite matching email", email: "ALICE@example.com", login: "alice", existingEmail: "alice@example.com", unreadable: true},
		{name: "name only profile", existingEmail: "alice@example.com", existingLogin: "alice"},
		{name: "existing email missing falls back to name", email: "alice@example.com", existingLogin: "alice"},
		{name: "request email missing falls back to name", existingEmail: "alice@example.com"},
		{name: "existing login missing refuses name fallback", login: "alice", existingEmail: "alice@example.com", unreadable: true},
		{name: "no identity fields"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user := &snowflake.User{
				Username: "alice", DisplayName: "Existing User", Type: "PERSON",
				Email: tc.existingEmail, Login: tc.existingLogin,
			}
			if !tc.unreadable && user.Login == "" {
				user.Login = "alice" // Snowflake defaults the mandatory login to the name.
			}
			builder, calls := createAccountFixture(t, "alice", http.StatusConflict, http.StatusOK, user)
			profile, err := structpb.NewStruct(map[string]any{
				"name": "alice", "email": tc.email, "login": tc.login,
			})
			require.NoError(t, err)
			result, plaintext, annos, err := builder.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, &v2.LocalCredentialOptions{
				Options: &v2.LocalCredentialOptions_RandomPassword_{RandomPassword: &v2.LocalCredentialOptions_RandomPassword{Length: 16}},
			})
			switch {
			case tc.unreadable:
				require.Error(t, err)
				require.Equal(t, codes.AlreadyExists, status.Code(err))
				require.Equal(t, "baton-snowflake: user already exists but its properties could not be read", status.Convert(err).Message())
				require.Nil(t, result)
			case tc.wantMismatch != "":
				require.Error(t, err)
				require.Equal(t, codes.AlreadyExists, status.Code(err))
				require.Equal(t, "baton-snowflake: user already exists but its "+tc.wantMismatch+" differs from the request", status.Convert(err).Message())
				require.Nil(t, result, "must not link a different identity")
			default:
				require.NoError(t, err)
				existing, ok := result.(*v2.CreateAccountResponse_AlreadyExistsResult)
				require.True(t, ok, "expected already-exists result, got %T", result)
				expected, resourceErr := userResource(context.Background(), user, builder.secrets)
				require.NoError(t, resourceErr)
				require.True(t, proto.Equal(expected, existing.Resource))
			}
			require.Empty(t, plaintext, "no password was set on the existing account")
			rateLimit := &v2.RateLimitDescription{}
			found, pickErr := annos.Pick(rateLimit)
			require.NoError(t, pickErr)
			require.True(t, found)
			require.EqualValues(t, 100, rateLimit.GetLimit())
			require.Equal(t, []string{"create", "describe"}, *calls, "no updates to existing users are allowed")
		})
	}
}

func TestCreateAccountConflictServiceUsers(t *testing.T) {
	for _, userType := range []string{"SERVICE", "SERVICE_AGENT", "LEGACY_SERVICE", " service ", "service_agent", "legacy_service"} {
		t.Run(userType, func(t *testing.T) {
			user := &snowflake.User{Username: "alice", Login: "alice", Email: "alice@example.com", Type: userType}
			builder, calls := createAccountFixture(t, "alice", http.StatusConflict, http.StatusOK, user)
			profile, err := structpb.NewStruct(map[string]any{"name": "alice", "email": user.Email, "login": user.Login})
			require.NoError(t, err)
			result, plaintext, annos, err := builder.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, nil)
			require.Error(t, err)
			require.Equal(t, codes.AlreadyExists, status.Code(err))
			require.Equal(t, "baton-snowflake: user already exists but is a service user, not a person", status.Convert(err).Message())
			require.Nil(t, result, "even matching identifiers must not link a service user")
			require.Empty(t, plaintext)
			require.NotEmpty(t, annos)
			require.Equal(t, []string{"create", "describe"}, *calls)
		})
	}
}

func TestCreateAccountConflictMaskedUser(t *testing.T) {
	for _, requestedEmail := range []string{"", "requester@example.com"} {
		t.Run(requestedEmail, func(t *testing.T) {
			user := &snowflake.User{Username: "alice", Login: "null", Email: "null", DisplayName: "null", Type: "null"}
			builder, calls := createAccountFixture(t, "alice", http.StatusConflict, http.StatusOK, user)
			profile, err := structpb.NewStruct(map[string]any{"name": "alice", "email": requestedEmail})
			require.NoError(t, err)
			result, plaintext, annos, err := builder.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, nil)
			require.Error(t, err)
			require.Equal(t, codes.AlreadyExists, status.Code(err))
			require.ErrorContains(t, err, "its properties could not be read")
			require.Nil(t, result)
			require.Empty(t, plaintext)
			require.NotEmpty(t, annos)
			require.Equal(t, []string{"create", "describe"}, *calls)
		})
	}
}

func TestCreateAccountDescribeNulls(t *testing.T) {
	for _, createStatus := range []int{http.StatusOK, http.StatusConflict} {
		t.Run(strconv.Itoa(createStatus), func(t *testing.T) {
			// NAME may legitimately be "null"; optional unset properties must not
			// reach the returned resource as an email or profile value of "null".
			user := &snowflake.User{Username: "null", Login: "null-login", Type: "PERSON"}
			builder, calls := createAccountFixture(t, user.Username, createStatus, http.StatusOK, user)
			profile, err := structpb.NewStruct(map[string]any{"name": user.Username, "email": "requester@example.com"})
			require.NoError(t, err)
			result, plaintext, _, err := builder.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, nil)
			require.NoError(t, err)
			var resource *v2.Resource
			if createStatus == http.StatusConflict {
				existing, ok := result.(*v2.CreateAccountResponse_AlreadyExistsResult)
				require.True(t, ok)
				resource = existing.Resource
			} else {
				created, ok := result.(*v2.CreateAccountResponse_SuccessResult)
				require.True(t, ok)
				resource = created.Resource
			}
			require.Equal(t, "null", resource.GetId().GetResource())
			trait, traitErr := rs.GetUserTrait(resource)
			require.NoError(t, traitErr)
			require.Empty(t, trait.GetEmails(), "an unset EMAIL is not a primary email of null")
			returnedProfile := rs.GetProfile(resource).AsMap()
			for _, key := range []string{"email", "display_name", "first_name", "last_name", profileKeyComment} {
				require.Empty(t, returnedProfile[key], "unset profile %s", key)
			}
			require.Equal(t, "null-login", returnedProfile["login"])
			require.Empty(t, plaintext)
			require.Equal(t, []string{"create", "describe"}, *calls)
		})
	}
}

func TestCreateAccountConflictLookupFailures(t *testing.T) {
	for _, tc := range []struct {
		lookupStatus int
		wantCode     codes.Code
	}{
		{lookupStatus: http.StatusNotFound, wantCode: codes.AlreadyExists},
		{lookupStatus: http.StatusOK, wantCode: codes.Unknown},
		{lookupStatus: http.StatusTooManyRequests, wantCode: codes.Unavailable},
		{lookupStatus: http.StatusInternalServerError, wantCode: codes.Unavailable},
	} {
		t.Run(strconv.Itoa(tc.lookupStatus), func(t *testing.T) {
			builder, calls := createAccountFixture(t, "alice", http.StatusConflict, tc.lookupStatus, nil)
			profile, err := structpb.NewStruct(map[string]any{"name": "alice"})
			require.NoError(t, err)
			result, plaintext, annos, err := builder.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, nil)
			require.ErrorContains(t, err, "failed to fetch existing user")
			require.Equal(t, tc.wantCode, status.Code(err))
			if tc.lookupStatus == http.StatusOK {
				require.ErrorContains(t, err, "column NAME not found")
			}
			require.Nil(t, result)
			require.Empty(t, plaintext)
			require.NotEmpty(t, annos)
			require.Equal(t, []string{"create", "describe"}, *calls)
		})
	}
}

func TestCreateAccountOtherErrors(t *testing.T) {
	for _, statusCode := range []int{http.StatusBadRequest, http.StatusForbidden, http.StatusInternalServerError, http.StatusTooManyRequests} {
		t.Run(strconv.Itoa(statusCode), func(t *testing.T) {
			builder, calls := createAccountFixture(t, "alice", statusCode, 0, nil)
			profile, err := structpb.NewStruct(map[string]any{"name": "alice"})
			require.NoError(t, err)
			result, plaintext, annos, err := builder.CreateAccount(context.Background(), &v2.AccountInfo{Profile: profile}, nil)
			require.Error(t, err)
			require.NotEqual(t, codes.AlreadyExists, status.Code(err), "error message alone must not be treated as a conflict")
			require.Nil(t, result)
			require.Empty(t, plaintext)
			require.Equal(t, []string{"create"}, *calls)
			rateLimit := &v2.RateLimitDescription{}
			found, pickErr := annos.Pick(rateLimit)
			require.NoError(t, pickErr)
			require.True(t, found)
			require.EqualValues(t, 100, rateLimit.GetLimit())
			if statusCode == http.StatusTooManyRequests {
				require.Equal(t, codes.Unavailable, status.Code(err))
				var detail *v2.RateLimitDescription
				for _, d := range status.Convert(err).Details() {
					if rl, ok := d.(*v2.RateLimitDescription); ok {
						detail = rl
					}
				}
				require.NotNil(t, detail, "retry/backoff must retain rate-limit error details")
				require.EqualValues(t, 0, detail.GetRemaining())
			}
		})
	}
}

func createAccountFixture(t *testing.T, username string, createStatus, lookupStatus int, user *snowflake.User) (*userBuilder, *[]string) {
	t.Helper()
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/users":
			calls = append(calls, "create")
			var body snowflake.CreateUserRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode create request: %v", err)
			}
			assert.Equal(t, quoteSnowflakeIdentifier(username), body.Name)
			w.Header().Set("X-Ratelimit-Limit", "100")
			w.Header().Set("X-Ratelimit-Remaining", "0")
			w.Header().Set("X-Ratelimit-Reset", "2000000000")
			w.WriteHeader(createStatus)
			_, _ = w.Write([]byte(`{"code":"002002","message":"object already exists"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/statements":
			calls = append(calls, "describe")
			var body struct {
				Statement string `json:"statement"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode SQL request: %v", err)
			}
			assert.Equal(t, "DESCRIBE USER "+quoteSnowflakeIdentifier(username)+";", body.Statement)
			w.WriteHeader(lookupStatus)
			if lookupStatus != http.StatusOK {
				_, _ = w.Write([]byte(`{"code":"002003","message":"user not found"}`))
				return
			}
			if user == nil {
				_, _ = w.Write([]byte(`{"data":[]}`))
				return
			}
			data := [][]string{
				{"NAME", user.Username}, {"LOGIN_NAME", user.Login}, {"DISPLAY_NAME", user.DisplayName},
				{"FIRST_NAME", user.FirstName}, {"LAST_NAME", user.LastName}, {"EMAIL", user.Email},
				{"DISABLED", strconv.FormatBool(user.Disabled)}, {"SNOWFLAKE_LOCK", strconv.FormatBool(user.Locked)},
				{"DEFAULT_ROLE", user.DefaultRole}, {"TYPE", user.Type}, {"HAS_MFA", strconv.FormatBool(user.HasMfa)},
				{"COMMENT", user.Comment},
			}
			// DESCRIBE USER returns unset properties as literal text, not empty strings.
			for _, row := range data {
				if row[0] != "NAME" && row[1] == "" {
					row[1] = "null"
				}
			}
			if err := json.NewEncoder(w).Encode(map[string]any{"data": data}); err != nil {
				t.Errorf("encode describe response: %v", err)
			}
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, server.Client())
	require.NoError(t, err)
	return newUserBuilder(client, secretOptions{syncSecrets: true}), &calls
}
