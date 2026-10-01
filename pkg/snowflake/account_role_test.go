package snowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureStatement returns an httptest.Server that records the "statement" field of
// the initial POST body it receives (the SQL text sent to the Statements API) into
// capturedSQL, then replies with a minimal valid response - including to any follow-up
// GET made to fetch the statement result - so the client's read path doesn't error.
func captureStatement(t *testing.T, capturedSQL *string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)

			var req StatementsApiRequestBody
			require.NoError(t, json.Unmarshal(body, &req))
			*capturedSQL = req.Statement
		}

		enc := json.NewEncoder(w)
		_ = enc.Encode(map[string]interface{}{
			"statementHandle": "handle",
			"resultSetMetadata": map[string]interface{}{
				"numRows": 0,
			},
			"data": [][]string{},
		})
	}))
}

// granteeRowTypes matches the column layout GetAccountRoleGrantees' ParseRow expects for SHOW
// GRANTS OF ROLE, aligned with the index positions granteeRow's rows use: 0=created_on (unused
// by AccountRoleGrantee, included only to occupy the position), 1=role, 2=granted_to,
// 3=grantee_name.
func granteeRowTypes() []map[string]interface{} {
	return []map[string]interface{}{
		{"name": columnCreatedOn, "type": "text"},
		{"name": columnRole, "type": "text"},
		{"name": columnGrantedTo, "type": "text"},
		{"name": columnGranteeName, "type": "text"},
	}
}

// serveGrantees returns an httptest.Server that implements the Snowflake Statements
// API for SHOW GRANTS OF ROLE. partition0Rows is returned on the initial GET
// (partition 0); if partition1Rows is non-nil a second partition is advertised
// and served on ?partition=1; ?partition=0 re-serves partition0Rows. Only the partition-0 response carries rowType metadata -
// matching real Snowflake behavior - so later partitions rely on the cursor to carry it
// forward (see accountRoleGranteesCursor).
func serveGrantees(t *testing.T, handle string, partition0Rows, partition1Rows [][]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)

		switch r.Method {
		case http.MethodPost:
			// Step 1: execute statement → return handle only.
			_ = enc.Encode(map[string]interface{}{
				"statementHandle": handle,
			})

		case http.MethodGet:
			_, hasPartition := r.URL.Query()["partition"]
			if !hasPartition {
				// Step 2: partition 0 + full partitionInfo and rowType metadata.
				partitionInfo := []map[string]interface{}{
					{"rowCount": len(partition0Rows)},
				}
				if partition1Rows != nil {
					partitionInfo = append(partitionInfo, map[string]interface{}{
						"rowCount": len(partition1Rows),
					})
				}
				_ = enc.Encode(map[string]interface{}{
					"statementHandle": handle,
					"resultSetMetadata": map[string]interface{}{
						"numRows":       len(partition0Rows) + len(partition1Rows),
						"partitionInfo": partitionInfo,
						"rowType":       granteeRowTypes(),
					},
					"data": partition0Rows,
				})
			} else {
				// Step 3: explicit partition fetch — data only, no metadata. Partition 0 is
				// re-fetched this way when it holds more rows than one page.
				var rows [][]string
				switch r.URL.Query().Get("partition") {
				case "0":
					rows = partition0Rows
				case "1":
					rows = partition1Rows
				default:
					t.Errorf("unexpected partition: %s", r.URL.Query().Get("partition"))
				}
				_ = enc.Encode(map[string]interface{}{
					"data": rows,
				})
			}

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}

// testGranteeLimit is the page limit for tests that do not exercise page bounds: large enough
// that every test partition fits in one page.
var testGranteeLimit = GranteePageLimit{MaxRows: 1000}

// granteeRow builds a data row in the order GetAccountRoleGrantees expects:
// index 1 = roleName, index 2 = granteeType, index 3 = granteeName.
func granteeRow(roleName, granteeType, granteeName string) []string {
	return []string{"", roleName, granteeType, granteeName}
}

// TestListAccountRoleGrantees_ToleratesColumnReorder verifies GetAccountRoleGrantees parses
// SHOW GRANTS OF ROLE rows by column name (via ResultSetMetadata.ParseRow), not by fixed
// positional index. rowType here deliberately lists columns in a different order than
// granteeRowTypes uses elsewhere in this file, with extra columns (granted_by, grant_option)
// interleaved - the kind of reordering/addition a Snowflake behavior bundle could introduce.
// A fixed-index implementation would silently misread these fields; parsing by name must not.
func TestListAccountRoleGrantees_ToleratesColumnReorder(t *testing.T) {
	response := ListAccountRoleGranteesRawResponse{
		StatementsApiResponseBase: StatementsApiResponseBase{
			ResultSetMetadata: ResultSetMetadata{
				RowTypes: []RowType{
					{Name: columnGranteeName, Type: "text"},
					{Name: columnGrantedBy, Type: "text"},
					{Name: columnRole, Type: "text"},
					{Name: columnGrantOption, Type: "text"},
					{Name: columnGrantedTo, Type: "text"},
					{Name: columnCreatedOn, Type: "text"},
				},
			},
			Data: [][]string{
				{"alice", "ACCOUNTADMIN", "MYROLE", "false", "USER", ""},
			},
		},
	}

	grantees, err := response.GetAccountRoleGrantees()
	require.NoError(t, err)
	require.Len(t, grantees, 1)
	assert.Equal(t, AccountRoleGrantee{RoleName: "MYROLE", GranteeType: "USER", GranteeName: "alice"}, grantees[0])
}

func TestListAccountRoleGrantees_SinglePartition(t *testing.T) {
	const handle = "handle-single"
	const role = "MYROLE"

	rows := [][]string{
		granteeRow(role, "USER", "alice"),
		granteeRow(role, "ROLE", "SYSADMIN"),
	}
	server := serveGrantees(t, handle, rows, nil)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	grantees, nextCursor, err := client.ListAccountRoleGrantees(context.Background(), role, "", testGranteeLimit)
	require.NoError(t, err)
	assert.Empty(t, nextCursor, "single partition should produce no next cursor")
	require.Len(t, grantees, 2)
	assert.Equal(t, AccountRoleGrantee{RoleName: role, GranteeType: "USER", GranteeName: "alice"}, grantees[0])
	assert.Equal(t, AccountRoleGrantee{RoleName: role, GranteeType: "ROLE", GranteeName: "SYSADMIN"}, grantees[1])
}

// TestListAccountRoleGrantees_UnquotesGranteeName
// SHOW GRANTS OF ROLE renders grantee names that require quoting (mixed case, spaces) wrapped
// in double quotes, with any embedded double quote doubled. GranteeName must come back
// unquoted so it matches the canonical (unquoted) ID that SHOW ROLES produces for the same
// role - otherwise nested-role expansion and principal-ID matching silently fail.
func TestListAccountRoleGrantees_UnquotesGranteeName(t *testing.T) {
	const handle = "handle-quoted"
	const role = "MYROLE"

	rows := [][]string{
		granteeRow(role, "ROLE", `"Data Engineer"`),
		granteeRow(role, "USER", `"He said ""hi"""`),
		granteeRow(role, "ROLE", "SYSADMIN"),
	}
	server := serveGrantees(t, handle, rows, nil)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	grantees, nextCursor, err := client.ListAccountRoleGrantees(context.Background(), role, "", testGranteeLimit)
	require.NoError(t, err)
	assert.Empty(t, nextCursor)
	require.Len(t, grantees, 3)
	assert.Equal(t, "Data Engineer", grantees[0].GranteeName, "quoted mixed-case name should be unquoted")
	assert.Equal(t, `He said "hi"`, grantees[1].GranteeName, "embedded escaped quotes should be unescaped")
	assert.Equal(t, "SYSADMIN", grantees[2].GranteeName, "already-unquoted system role should be unaffected")
}

// accountRoleRowTypes matches the column layout ListAccountRoles' ParseRow expects for SHOW ROLES.
func accountRoleRowTypes() []map[string]interface{} {
	return []map[string]interface{}{
		{"name": "name", "type": "text"},
	}
}

// serveAccountRoles returns an httptest.Server implementing the Snowflake Statements API for
// SHOW ROLES, returning a single page of rows.
func serveAccountRoles(t *testing.T, handle string, rows [][]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)

		switch r.Method {
		case http.MethodPost:
			_ = enc.Encode(map[string]interface{}{
				"statementHandle": handle,
			})
		case http.MethodGet:
			_ = enc.Encode(map[string]interface{}{
				"statementHandle": handle,
				"resultSetMetadata": map[string]interface{}{
					"numRows": len(rows),
					"rowType": accountRoleRowTypes(),
				},
				"data": rows,
			})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}

// TestListAccountRoles_MatchesUnquotedGranteeID is the acceptance-criteria regression test
// for a role whose name requires quoting in SHOW GRANTS output, the resource ID Baton
// builds from SHOW ROLES (canonical, bare name) must exactly match the principal ID built from
// the corresponding SHOW GRANTS OF ROLE grantee entry for that same role (unquoted by this fix).
// Before the fix, these diverged whenever the role name contained spaces/mixed case, silently
// breaking nested-role expansion.
func TestListAccountRoles_MatchesUnquotedGranteeID(t *testing.T) {
	const roleName = "Data Engineer"

	rolesServer := serveAccountRoles(t, "handle-roles", [][]string{{roleName}})
	defer rolesServer.Close()

	rolesClient, err := New(rolesServer.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	roles, err := rolesClient.ListAccountRoles(context.Background(), "", 100)
	require.NoError(t, err)
	require.Len(t, roles, 1)
	assert.Equal(t, roleName, roles[0].Name, "SHOW ROLES-derived name must remain bare/unquoted")

	granteesServer := serveGrantees(t, "handle-grantees", [][]string{
		granteeRow("PARENT_ROLE", "ROLE", `"Data Engineer"`),
	}, nil)
	defer granteesServer.Close()

	granteesClient, err := New(granteesServer.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	grantees, _, err := granteesClient.ListAccountRoleGrantees(context.Background(), "PARENT_ROLE", "", testGranteeLimit)
	require.NoError(t, err)
	require.Len(t, grantees, 1)

	assert.Equal(t, roles[0].Name, grantees[0].GranteeName,
		"resource ID from SHOW ROLES must byte-for-byte match the principal ID derived from SHOW GRANTS OF ROLE")
}

func TestListAccountRoleGrantees_MultiPartition(t *testing.T) {
	const handle = "handle-multi"
	const role = "MYROLE"

	partition0 := [][]string{
		granteeRow(role, "USER", "alice"),
		granteeRow(role, "ROLE", "SYSADMIN"),
	}
	partition1 := [][]string{
		granteeRow(role, "USER", "bob"),
	}
	server := serveGrantees(t, handle, partition0, partition1)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	ctx := context.Background()

	// Page 1: empty cursor → executes query, returns partition 0 + cursor.
	page1, cursor1, err := client.ListAccountRoleGrantees(ctx, role, "", testGranteeLimit)
	require.NoError(t, err)
	require.Len(t, page1, 2)
	assert.Equal(t, "alice", page1[0].GranteeName)
	assert.Equal(t, "USER", page1[0].GranteeType)
	assert.Equal(t, "SYSADMIN", page1[1].GranteeName)
	assert.Equal(t, "ROLE", page1[1].GranteeType)
	assert.NotEmpty(t, cursor1)

	// Page 2: cursor from page 1 → fetches ?partition=1, no further cursor.
	page2, cursor2, err := client.ListAccountRoleGrantees(ctx, role, cursor1, testGranteeLimit)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, "bob", page2[0].GranteeName)
	assert.Equal(t, "USER", page2[0].GranteeType)
	assert.Empty(t, cursor2, "last partition should return empty cursor")
}

// TestListAccountRoleGrantees_SlicesLargePartition verifies that a partition holding more rows
// than one page allows is returned across several pages instead of one oversized page (a whole
// partition can overflow the 6MB Lambda response limit), and that walking every cursor
// yields each row exactly once, in order, before moving to the next partition.
func TestListAccountRoleGrantees_SlicesLargePartition(t *testing.T) {
	const handle = "handle-large"
	const role = "EMPLOYEES"
	limit := GranteePageLimit{MaxRows: 100}

	partition0 := make([][]string, 0, 2*limit.MaxRows+1)
	for i := range 2*limit.MaxRows + 1 {
		partition0 = append(partition0, granteeRow(role, "USER", fmt.Sprintf("user%05d", i)))
	}
	partition1 := [][]string{
		granteeRow(role, "USER", "last"),
	}
	server := serveGrantees(t, handle, partition0, partition1)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	ctx := context.Background()
	var all []AccountRoleGrantee
	var pageSizes []int
	cursor := ""
	for {
		page, next, err := client.ListAccountRoleGrantees(ctx, role, cursor, limit)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page), limit.MaxRows)
		all = append(all, page...)
		pageSizes = append(pageSizes, len(page))
		if next == "" {
			break
		}
		require.Less(t, len(pageSizes), 10, "pagination did not terminate")
		cursor = next
	}

	assert.Equal(t, []int{limit.MaxRows, limit.MaxRows, 1, 1}, pageSizes)
	require.Len(t, all, len(partition0)+len(partition1))
	for i := range partition0 {
		assert.Equal(t, fmt.Sprintf("user%05d", i), all[i].GranteeName)
	}
	assert.Equal(t, "last", all[len(all)-1].GranteeName)
}

func TestPageAccountRoleGrantees(t *testing.T) {
	rows := []AccountRoleGrantee{{GranteeName: "a"}, {GranteeName: "b"}, {GranteeName: "c"}}
	base := accountRoleGranteesCursor{Handle: "h", TotalPartitions: 2, RowTypes: []RowType{{Name: columnRole}}}

	rowsOnly := func(maxRows int) GranteePageLimit { return GranteePageLimit{MaxRows: maxRows} }
	// Each row costs the length of its grantee name, so the byte bound is easy to reason about.
	byNameLength := func(maxRows, maxBytes int) GranteePageLimit {
		return GranteePageLimit{MaxRows: maxRows, MaxBytes: maxBytes, RowBytes: func(g AccountRoleGrantee) int { return len(g.GranteeName) }}
	}

	decode := func(t *testing.T, cursor string) accountRoleGranteesCursor {
		t.Helper()
		cur, err := decodeAccountRoleGranteesCursor(cursor)
		require.NoError(t, err)
		return cur
	}

	t.Run("mid-partition continues the same partition", func(t *testing.T) {
		page, next, err := pageAccountRoleGrantees(base, rows, rowsOnly(2))
		require.NoError(t, err)
		assert.Equal(t, rows[:2], page)
		cur := decode(t, next)
		assert.Equal(t, 0, cur.PartitionID)
		assert.Equal(t, 2, cur.Offset)
		assert.Equal(t, base.RowTypes, cur.RowTypes, "rowType layout must be carried forward")
	})

	t.Run("end of partition advances to the next partition", func(t *testing.T) {
		cur := base
		cur.Offset = 2
		page, next, err := pageAccountRoleGrantees(cur, rows, rowsOnly(2))
		require.NoError(t, err)
		assert.Equal(t, rows[2:], page)
		nextCur := decode(t, next)
		assert.Equal(t, 1, nextCur.PartitionID)
		assert.Equal(t, 0, nextCur.Offset)
	})

	t.Run("end of last partition terminates", func(t *testing.T) {
		cur := base
		cur.PartitionID = 1
		page, next, err := pageAccountRoleGrantees(cur, rows, rowsOnly(5))
		require.NoError(t, err)
		assert.Equal(t, rows, page)
		assert.Empty(t, next)
	})

	t.Run("no partition info terminates", func(t *testing.T) {
		cur := base
		cur.TotalPartitions = 0
		page, next, err := pageAccountRoleGrantees(cur, rows, rowsOnly(5))
		require.NoError(t, err)
		assert.Equal(t, rows, page)
		assert.Empty(t, next)
	})

	t.Run("offset past partition end errors", func(t *testing.T) {
		cur := base
		cur.Offset = 4
		_, _, err := pageAccountRoleGrantees(cur, rows, rowsOnly(2))
		require.Error(t, err)
	})

	t.Run("byte bound ends the page before the row that would exceed it", func(t *testing.T) {
		sized := []AccountRoleGrantee{{GranteeName: "aaaa"}, {GranteeName: "bbbb"}, {GranteeName: "cc"}}
		page, next, err := pageAccountRoleGrantees(base, sized, byNameLength(10, 9))
		require.NoError(t, err)
		assert.Equal(t, sized[:2], page, "4+4 fits in 9 bytes, 4+4+2 does not")
		assert.Equal(t, 2, decode(t, next).Offset)
	})

	t.Run("row bound applies when the byte bound is not reached", func(t *testing.T) {
		page, next, err := pageAccountRoleGrantees(base, rows, byNameLength(2, 1000))
		require.NoError(t, err)
		assert.Equal(t, rows[:2], page)
		assert.Equal(t, 2, decode(t, next).Offset)
	})

	t.Run("a row larger than the byte bound is still returned alone", func(t *testing.T) {
		huge := []AccountRoleGrantee{{GranteeName: "way-too-long"}, {GranteeName: "b"}}
		page, next, err := pageAccountRoleGrantees(base, huge, byNameLength(10, 3))
		require.NoError(t, err)
		assert.Equal(t, huge[:1], page, "paging must make progress")
		assert.Equal(t, 1, decode(t, next).Offset)
	})

	t.Run("non-positive row bound errors", func(t *testing.T) {
		_, _, err := pageAccountRoleGrantees(base, rows, GranteePageLimit{})
		require.Error(t, err)
	})

	t.Run("cursor without offset starts at partition start", func(t *testing.T) {
		cur := decode(t, `{"handle":"h","partitionId":1,"totalPartitions":2,"rowTypes":[]}`)
		assert.Equal(t, 0, cur.Offset)
	})
}

// TestListAccountRoleGrantees_EscapesRoleName verifies that a role name containing an
// embedded double quote (legal in Snowflake via a quoted identifier, e.g.
// CREATE ROLE "weird""role") is escaped before being interpolated into the
// SHOW GRANTS OF ROLE "..."; statement, rather than breaking out of the quoted identifier.
func TestListAccountRoleGrantees_EscapesRoleName(t *testing.T) {
	const role = `weird"role`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	_, _, err = client.ListAccountRoleGrantees(context.Background(), role, "", testGranteeLimit)
	require.NoError(t, err)
	assert.Equal(t, `SHOW GRANTS OF ROLE "weird""role";`, capturedSQL)
}

// TestGetAccountRole_EscapesRoleName verifies that a role name containing a single quote
// is escaped before being interpolated into the SHOW ROLES LIKE '...' statement.
func TestGetAccountRole_EscapesRoleName(t *testing.T) {
	const role = `o'brien`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	_, _, err = client.GetAccountRole(context.Background(), nil, role)
	require.NoError(t, err)
	assert.Equal(t, `SHOW ROLES LIKE 'o''brien' LIMIT 50;`, capturedSQL)
}

// TestGetAccountRole_NoEscapeClauseForLikeWildcards documents a Snowflake limitation:
// SHOW ROLES' LIKE filter has no ESCAPE clause (unlike the general SQL LIKE predicate/WHERE
// usage), so there is no syntax to make an underscore or percent sign in a role name match
// literally. Only the single quote is escaped, to keep the string literal well-formed;
// _ and % are sent through untouched and remain active wildcards. A prior version of this
// code added "ESCAPE '\'" to the statement to try to neutralize these wildcards, but SHOW
// ROLES does not support that clause at all - Snowflake rejects it as a 422 Unprocessable
// Entity (SQL compilation error) on every call, not just ones with wildcard characters.
func TestGetAccountRole_NoEscapeClauseForLikeWildcards(t *testing.T) {
	const role = `DATA_ENGINEER%1`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	_, _, err = client.GetAccountRole(context.Background(), nil, role)
	require.NoError(t, err)
	assert.Equal(t, `SHOW ROLES LIKE 'DATA_ENGINEER%1' LIMIT 50;`, capturedSQL)
}

// serveAccountRoleMatch returns an httptest.Server implementing the Snowflake Statements
// API's single-step POST flow that GetAccountRole uses (unlike ListAccountRoles, it does not
// follow up with a GET to fetch the statement result - the POST response must carry the data
// directly), returning a single row for the given role name.
func serveAccountRoleMatch(t *testing.T, roleName string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		_ = enc.Encode(map[string]interface{}{
			"statementHandle": "handle",
			"resultSetMetadata": map[string]interface{}{
				"numRows": 1,
				"rowType": accountRoleRowTypes(),
			},
			"data": [][]string{{roleName}},
		})
	}))
}

// TestGetAccountRole_ExactMatch verifies the normal case: the single row SHOW ROLES LIKE
// returns is an exact match for roleName, so it is returned as-is.
func TestGetAccountRole_ExactMatch(t *testing.T) {
	const role = "SYSADMIN"

	server := serveAccountRoleMatch(t, role)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	got, _, err := client.GetAccountRole(context.Background(), nil, role)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, role, got.Name)
}

// TestGetAccountRole_EscapesTrailingBackslash verifies a role name ending in a backslash
// still resolves to an exact match (see escapeLikeStringLiteral).
func TestGetAccountRole_EscapesTrailingBackslash(t *testing.T) {
	const role = `TST_BS\`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	_, _, err = client.GetAccountRole(context.Background(), nil, role)
	require.NoError(t, err)
	assert.Equal(t, `SHOW ROLES LIKE 'TST_BS\\\\' LIMIT 50;`, capturedSQL)
}

// TestGetAccountRole_RejectsWildcardMismatch guards against the exact bug the exact-match
// check exists to prevent: SHOW ROLES' LIKE filter is a pattern match with no ESCAPE clause
// to neutralize _/% wildcards (see the query comment in GetAccountRole), so a roleName
// containing a wildcard character can match some other role entirely. Here roleName is
// "DATA_ENGINEER" (the _ is a wildcard matching any single character) and the server - as
// Snowflake's LIMIT 1 would for an over-broad pattern - returns exactly one row for a
// DIFFERENT role, "DATAXENGINEER", that happens to match the loose pattern. Before the
// exact-match guard, GetAccountRole would have silently returned this wrong role. It must
// instead be treated as "not found": nil role, no error.
func TestGetAccountRole_RejectsWildcardMismatch(t *testing.T) {
	const requested = "DATA_ENGINEER"
	const actualMatch = "DATAXENGINEER"

	server := serveAccountRoleMatch(t, actualMatch)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	got, _, err := client.GetAccountRole(context.Background(), nil, requested)
	require.NoError(t, err)
	assert.Nil(t, got, "a role name that only loosely matches the LIKE wildcard pattern must not be returned as if it were an exact match")
}

// serveAccountRoleRows is like serveAccountRoleMatch but returns multiple rows, for tests
// exercising wildcard collisions where more than one role matches the LIKE pattern.
func serveAccountRoleRows(t *testing.T, roleNames []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		data := make([][]string, len(roleNames))
		for i, name := range roleNames {
			data[i] = []string{name}
		}
		_ = enc.Encode(map[string]interface{}{
			"statementHandle": "handle",
			"resultSetMetadata": map[string]interface{}{
				"numRows": len(roleNames),
				"rowType": accountRoleRowTypes(),
			},
			"data": data,
		})
	}))
}

// TestGetAccountRole_FindsExactMatchAmongWildcardCollisions verifies that GetAccountRole finds
// the real role even when a wildcard-colliding role ("DATAXENGINEER") is returned before it.
func TestGetAccountRole_FindsExactMatchAmongWildcardCollisions(t *testing.T) {
	const requested = "DATA_ENGINEER"
	const collision = "DATAXENGINEER"

	server := serveAccountRoleRows(t, []string{collision, requested})
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	got, _, err := client.GetAccountRole(context.Background(), nil, requested)
	require.NoError(t, err)
	require.NotNil(t, got, "the real role must still be found even though a wildcard-colliding role was returned first")
	assert.Equal(t, requested, got.Name)
}

// TestGrantAccountRole_EscapesIdentifiers verifies that role and user names containing
// embedded double quotes are escaped before being interpolated into the
// GRANT ROLE "..." TO USER "..."; statement.
func TestGrantAccountRole_EscapesIdentifiers(t *testing.T) {
	const role = `weird"role`
	const user = `weird"user`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	err = client.GrantAccountRole(context.Background(), role, user)
	require.NoError(t, err)
	assert.Equal(t, `GRANT ROLE "weird""role" TO USER "weird""user";`, capturedSQL)
}

// TestRevokeAccountRole_EscapesIdentifiers verifies that role and user names containing
// embedded double quotes are escaped before being interpolated into the
// REVOKE ROLE "..." FROM USER "..."; statement.
func TestRevokeAccountRole_EscapesIdentifiers(t *testing.T) {
	const role = `weird"role`
	const user = `weird"user`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	err = client.RevokeAccountRole(context.Background(), role, user)
	require.NoError(t, err)
	assert.Equal(t, `REVOKE ROLE "weird""role" FROM USER "weird""user";`, capturedSQL)
}

// TestListAccountRoles_EscapesCursor verifies that the pagination cursor - which is the
// bare name of the last role from a previous page, and so can itself contain a single
// quote (e.g. a role created as CREATE ROLE "o'brien") - is escaped before being
// interpolated into the SHOW ROLES LIMIT ... FROM '...' statement.
func TestListAccountRoles_EscapesCursor(t *testing.T) {
	const cursor = `o'brien`

	var capturedSQL string
	server := captureStatement(t, &capturedSQL)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	_, err = client.ListAccountRoles(context.Background(), cursor, 100)
	require.NoError(t, err)
	assert.Equal(t, `SHOW ROLES LIMIT 100 FROM 'o''brien';`, capturedSQL)
}
