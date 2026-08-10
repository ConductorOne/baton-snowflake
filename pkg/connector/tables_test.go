package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeTableResource creates a table resource with profile fields via the real tableResource() function.
func makeTableResource(t *testing.T, dbName, schemaName, tableName string) *v2.Resource {
	t.Helper()
	table := &snowflake.Table{
		DatabaseName: dbName,
		SchemaName:   schemaName,
		Name:         tableName,
		Kind:         "TABLE",
		CreatedOn:    time.Now(),
	}
	parentID := &v2.ResourceId{
		ResourceType: databaseResourceType.Id,
		Resource:     dbName,
	}
	resource, err := tableResource(context.Background(), table, parentID, false)
	require.NoError(t, err)
	return resource
}

// makeBareResource creates a resource with a raw ID string and no profile (simulates legacy sync).
func makeBareResource(t *testing.T, resourceID string) *v2.Resource {
	t.Helper()
	return &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: tableResourceType.Id,
			Resource:     resourceID,
		},
	}
}

func TestParseTableResourceID(t *testing.T) {
	tests := []struct {
		name       string
		resource   *v2.Resource
		wantDB     string
		wantSchema string
		wantTable  string
		wantErr    bool
	}{
		{
			name:       "normal names with profile",
			resource:   makeTableResource(t, "mydb", "public", "users"),
			wantDB:     "mydb",
			wantSchema: "public",
			wantTable:  "users",
		},
		{
			name:       "period in database name",
			resource:   makeTableResource(t, "my.db", "public", "users"),
			wantDB:     "my.db",
			wantSchema: "public",
			wantTable:  "users",
		},
		{
			name:       "period in schema name",
			resource:   makeTableResource(t, "mydb", "my.schema", "users"),
			wantDB:     "mydb",
			wantSchema: "my.schema",
			wantTable:  "users",
		},
		{
			name:       "period in table name",
			resource:   makeTableResource(t, "mydb", "public", "my.table"),
			wantDB:     "mydb",
			wantSchema: "public",
			wantTable:  "my.table",
		},
		{
			name:       "periods in all components",
			resource:   makeTableResource(t, "a.b", "c.d", "e.f"),
			wantDB:     "a.b",
			wantSchema: "c.d",
			wantTable:  "e.f",
		},
		{
			name:       "legacy fallback with valid format",
			resource:   makeBareResource(t, "mydb.public.users"),
			wantDB:     "mydb",
			wantSchema: "public",
			wantTable:  "users",
		},
		{
			name:     "legacy fallback with invalid format",
			resource: makeBareResource(t, "mydb.public"),
			wantErr:  true,
		},
		{
			name:       "partial profile falls back to split",
			resource:   makePartialProfileResource(t, "mydb", "public", "users"),
			wantDB:     "mydb",
			wantSchema: "public",
			wantTable:  "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, schema, table, err := parseTableResourceID(tt.resource)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantDB, db)
			require.Equal(t, tt.wantSchema, schema)
			require.Equal(t, tt.wantTable, table)
		})
	}
}

func TestTableGrantsPageState_RoundTrip(t *testing.T) {
	state := tableGrantsPageState{Cursor: `{"handle":"h","partitionId":2,"totalPartitions":5}`, OwnershipSeen: true}

	token, err := encodeTableGrantsPageState(state)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	decoded, err := decodeTableGrantsPageState(token)
	require.NoError(t, err)
	assert.Equal(t, state, decoded)
}

func TestTableGrantsPageState_EmptyTokenDecodesToZeroValue(t *testing.T) {
	decoded, err := decodeTableGrantsPageState("")
	require.NoError(t, err)
	assert.Equal(t, tableGrantsPageState{}, decoded)
}

func TestTableGrantsPageState_InvalidTokenErrors(t *testing.T) {
	_, err := decodeTableGrantsPageState("not json")
	require.Error(t, err)
}

// tableGrantRow builds a SHOW GRANTS ON TABLE data row in the column order the mock server below
// advertises: created_on, privilege, granted_on, name, granted_to, grantee_name, grant_option, granted_by.
func tableGrantRow(privilege, grantedTo, granteeName string) []string {
	return []string{"1700000000.000000000", privilege, "TABLE", "MYTABLE", grantedTo, granteeName, "false", "ACCOUNTADMIN"}
}

// Field/column name literals for the mock rowType payloads below. Named to avoid duplicating
// string literals that already appear elsewhere in this package (goconst) - goconst counts
// occurrences across the whole package even though it doesn't report findings in _test.go files,
// so an unnamed literal here can still trip a threshold in unrelated production code.
const (
	keyName         = "name"
	keyType         = "type"
	colName         = "name"
	colCreatedOn    = "created_on"
	colSchemaName   = "schema_name"
	colDatabaseName = "database_name"
	colKind         = "kind"
	colComment      = "comment"
	colOwner        = "owner"
	colOrigin       = "origin"
	colText         = "text"
	colTimestampLtz = "timestamp_ltz"
)

// newTableGrantsMockServer serves a single SHOW GRANTS ON TABLE query split across len(partitions)
// partitions, reusing one statement handle across the POST and every partitioned GET exactly like
// the real Snowflake Statements API. SHOW ROLES LIKE (role resolution) always answers 422, which
// exercises the IsUnprocessableEntity fallback path in tableBuilder.Grants() and lets the test
// avoid mocking full account-role resolution. SHOW TABLES LIKE (the Owner-column fallback) is
// tracked via getTableCalls so tests can assert it's never reached once ownership was already seen;
// when reached it resolves to tableOwner via a real POST+GET dance (not a shortcut, unlike roles).
func newTableGrantsMockServer(t *testing.T, partitions [][][]string, tableOwner string, getTableCalls *atomic.Int32) *httptest.Server {
	t.Helper()
	const grantsHandle = "grants-handle"
	const tableHandle = "table-handle"
	grantRowTypes := []map[string]interface{}{
		{keyName: colCreatedOn, keyType: colTimestampLtz},
		{keyName: "privilege", keyType: colText},
		{keyName: "granted_on", keyType: colText},
		{keyName: keyName, keyType: colText},
		{keyName: "granted_to", keyType: colText},
		{keyName: "grantee_name", keyType: colText},
		{keyName: "grant_option", keyType: colText},
		{keyName: "granted_by", keyType: colText},
	}
	tableRowTypes := []map[string]interface{}{
		{keyName: colCreatedOn, keyType: colTimestampLtz},
		{keyName: keyName, keyType: colText},
		{keyName: colSchemaName, keyType: colText},
		{keyName: colDatabaseName, keyType: colText},
		{keyName: colKind, keyType: colText},
		{keyName: colComment, keyType: colText},
		{keyName: colOwner, keyType: colText},
	}
	tableRow := []string{"1700000000.000000000", "MYTABLE", "SCHEMA", "DB", "TABLE", "", tableOwner}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)

		switch r.Method {
		case http.MethodPost:
			var body struct {
				Statement string `json:"statement"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			switch {
			case strings.Contains(body.Statement, "SHOW GRANTS ON"):
				_ = enc.Encode(map[string]interface{}{"statementHandle": grantsHandle})
			case strings.Contains(body.Statement, "SHOW ROLES LIKE"):
				w.WriteHeader(http.StatusUnprocessableEntity)
				_ = enc.Encode(map[string]interface{}{"code": "003001", "message": "insufficient privileges"})
			case strings.Contains(body.Statement, "SHOW TABLES LIKE"):
				getTableCalls.Add(1)
				_ = enc.Encode(map[string]interface{}{"statementHandle": tableHandle})
			default:
				t.Errorf("unexpected statement: %s", body.Statement)
				w.WriteHeader(http.StatusBadRequest)
			}

		case http.MethodGet:
			handle := strings.TrimSuffix(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:], "/")
			partitionIndex := 0
			if p := r.URL.Query().Get("partition"); p != "" {
				partitionIndex, _ = strconv.Atoi(p)
			}

			switch handle {
			case tableHandle:
				_ = enc.Encode(map[string]interface{}{
					"statementHandle": tableHandle,
					"resultSetMetadata": map[string]interface{}{
						"numRows":       1,
						"partitionInfo": []map[string]interface{}{{"rowCount": 1}},
						"rowType":       tableRowTypes,
					},
					"data": [][]string{tableRow},
				})

			case grantsHandle:
				if partitionIndex == 0 {
					partitionInfo := make([]map[string]interface{}, len(partitions))
					totalRows := 0
					for i, p := range partitions {
						partitionInfo[i] = map[string]interface{}{"rowCount": len(p)}
						totalRows += len(p)
					}
					_ = enc.Encode(map[string]interface{}{
						"statementHandle": grantsHandle,
						"resultSetMetadata": map[string]interface{}{
							"numRows":       totalRows,
							"partitionInfo": partitionInfo,
							"rowType":       grantRowTypes,
						},
						"data": partitions[0],
					})
				} else {
					_ = enc.Encode(map[string]interface{}{"data": partitions[partitionIndex]})
				}

			default:
				t.Errorf("unexpected statement handle: %s", handle)
				w.WriteHeader(http.StatusBadRequest)
			}

		default:
			t.Errorf("unexpected method: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}

// TestTableBuilder_Grants_OwnershipOnMiddlePage reproduces PR #131's reported scenario end to end:
// the OWNERSHIP grant lands on a page that is neither first nor last, and no session store is
// configured. It asserts both that ownership is still correctly detected (not missed, and not
// duplicated via a spurious Owner-column fallback) and that no extra API calls are made to
// determine that fact - the state carried in the SDK page token makes it free.
func TestTableBuilder_Grants_OwnershipOnMiddlePage(t *testing.T) {
	partitions := [][][]string{
		{tableGrantRow("SELECT", grantedToRole, "ANALYST")},
		{tableGrantRow("OWNERSHIP", grantedToRole, "SYSADMIN")},
		{tableGrantRow("INSERT", grantedToRole, "ANALYST")},
	}
	var getTableCalls atomic.Int32
	server := newTableGrantsMockServer(t, partitions, "SHOULD_NOT_BE_QUERIED", &getTableCalls)
	defer server.Close()

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	builder := &tableBuilder{client: client}
	resource := makeTableResource(t, "DB", "SCHEMA", "MYTABLE")
	ownerEntitlementID := fmt.Sprintf("%s:%s:%s", tableResourceType.Id, resource.Id.Resource, ownerEntitlement)

	var allGrants []*v2.Grant
	token := ""
	for i := 0; i < len(partitions)+1; i++ {
		grants, results, err := builder.Grants(context.Background(), resource, rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
		require.NoError(t, err)
		allGrants = append(allGrants, grants...)
		if results == nil || results.NextPageToken == "" {
			break
		}
		token = results.NextPageToken
	}

	assert.Equal(t, int32(0), getTableCalls.Load(), "owner fallback must not fire once ownership was seen on an earlier page")

	ownershipGrants := 0
	for _, g := range allGrants {
		if g.Entitlement.Id == ownerEntitlementID {
			ownershipGrants++
			assert.Equal(t, "SYSADMIN", g.Principal.Id.Resource)
		}
	}
	assert.Equal(t, 1, ownershipGrants, "exactly one ownership grant, not zero (missed) or duplicated")
}

// TestTableBuilder_Grants_NoOwnershipAnywhereFallsBackToOwnerColumn verifies the Owner-column
// fallback still fires - exactly once, on the last page - when no page's grants include an
// explicit OWNERSHIP row.
func TestTableBuilder_Grants_NoOwnershipAnywhereFallsBackToOwnerColumn(t *testing.T) {
	partitions := [][][]string{
		{tableGrantRow("SELECT", grantedToRole, "ANALYST")},
		{tableGrantRow("INSERT", grantedToRole, "ANALYST")},
	}
	var getTableCalls atomic.Int32
	server := newTableGrantsMockServer(t, partitions, "TABLE_OWNER_ROLE", &getTableCalls)
	defer server.Close()

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	builder := &tableBuilder{client: client}
	resource := makeTableResource(t, "DB", "SCHEMA", "MYTABLE")

	token := ""
	for i := 0; i < len(partitions)+1; i++ {
		_, results, err := builder.Grants(context.Background(), resource, rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
		require.NoError(t, err)
		if results == nil || results.NextPageToken == "" {
			break
		}
		token = results.NextPageToken
	}

	assert.Equal(t, int32(1), getTableCalls.Load(), "owner fallback should fire exactly once when no page had an explicit ownership grant")
}

// publicSchema is the default schema Snowflake creates in every database.
const publicSchema = "PUBLIC"

// serveUnresolvedParentDatabase mocks a parent database that GetDatabase can't resolve (zero
// rows for SHOW DATABASES LIKE), plus one schema ("SCHEMA") and one table ("MYTABLE") beneath it.
func serveUnresolvedParentDatabase(t *testing.T) *httptest.Server {
	t.Helper()
	const schemasHandle = "schemas-handle"
	const tablesHandle = "tables-handle"

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)

		switch r.Method {
		case http.MethodPost:
			var body struct {
				Statement string `json:"statement"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			switch {
			case strings.Contains(body.Statement, "SHOW DATABASES LIKE"):
				_ = enc.Encode(map[string]interface{}{
					"statementHandle": "databases-handle",
					"resultSetMetadata": map[string]interface{}{
						"numRows": 0,
						"rowType": []map[string]interface{}{
							{keyName: colName, keyType: colText},
							{keyName: colOwner, keyType: colText},
							{keyName: colKind, keyType: colText},
							{keyName: colOrigin, keyType: colText},
						},
					},
					"data": [][]string{},
				})
			case strings.Contains(body.Statement, "SHOW SCHEMAS IN DATABASE"):
				_ = enc.Encode(map[string]interface{}{"statementHandle": schemasHandle})
			case strings.Contains(body.Statement, "SHOW TABLES IN SCHEMA"):
				_ = enc.Encode(map[string]interface{}{"statementHandle": tablesHandle})
			default:
				t.Errorf("unexpected statement: %s", body.Statement)
				w.WriteHeader(http.StatusBadRequest)
			}

		case http.MethodGet:
			handle := strings.TrimSuffix(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:], "/")
			switch handle {
			case schemasHandle:
				_ = enc.Encode(map[string]interface{}{
					"statementHandle": schemasHandle,
					"resultSetMetadata": map[string]interface{}{
						"numRows": 1,
						"rowType": []map[string]interface{}{
							{keyName: colName, keyType: colText},
							{keyName: colDatabaseName, keyType: colText},
						},
					},
					"data": [][]string{{"SCHEMA", "DB"}},
				})
			case tablesHandle:
				_ = enc.Encode(map[string]interface{}{
					"statementHandle": tablesHandle,
					"resultSetMetadata": map[string]interface{}{
						"numRows": 1,
						"rowType": []map[string]interface{}{
							{keyName: colCreatedOn, keyType: colTimestampLtz},
							{keyName: colName, keyType: colText},
							{keyName: colSchemaName, keyType: colText},
							{keyName: colDatabaseName, keyType: colText},
							{keyName: colKind, keyType: colText},
							{keyName: colComment, keyType: colText},
							{keyName: colOwner, keyType: colText},
						},
					},
					"data": [][]string{{"1700000000.000000000", "MYTABLE", "SCHEMA", "DB", "TABLE", "", "SYSADMIN"}},
				})
			default:
				t.Errorf("unexpected statement handle: %s", handle)
				w.WriteHeader(http.StatusBadRequest)
			}

		default:
			t.Errorf("unexpected method: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}

// TestIsDBSharedOrSystem_ToleratesUnresolvedDatabase verifies an unresolved parent database
// is treated as "not shared/system" rather than an error.
func TestIsDBSharedOrSystem_ToleratesUnresolvedDatabase(t *testing.T) {
	server := serveUnresolvedParentDatabase(t)
	defer server.Close()

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	builder := &tableBuilder{client: client}
	// Bare resource (no profile) so isDBSharedOrSystem falls through to GetDatabase.
	resource := makeBareResource(t, "DB.SCHEMA.MYTABLE")

	isSharedOrSystem, err := builder.isDBSharedOrSystem(context.Background(), resource, "DB")
	require.NoError(t, err)
	assert.False(t, isSharedOrSystem, "an unresolved parent database must not be treated as shared/system")
}

// TestTableBuilder_List_ToleratesUnresolvedParentDatabase verifies List() still enumerates
// schemas/tables when the parent database can't be resolved, instead of aborting the sync.
func TestTableBuilder_List_ToleratesUnresolvedParentDatabase(t *testing.T) {
	server := serveUnresolvedParentDatabase(t)
	defer server.Close()

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	builder := &tableBuilder{client: client}
	parentResourceID := &v2.ResourceId{ResourceType: databaseResourceType.Id, Resource: "DB"}

	resources, results, err := builder.List(context.Background(), parentResourceID, rs.SyncOpAttrs{PageToken: pagination.Token{}})
	require.NoError(t, err)
	require.NotNil(t, results)
	require.Len(t, resources, 1)
	assert.Equal(t, "MYTABLE", resources[0].DisplayName)

	profile := rs.GetProfile(resources[0])
	isSharedOrSystemDB, _ := profile.GetFields()["database_is_shared_system"].AsInterface().(bool)
	assert.False(t, isSharedOrSystemDB, "an unresolved parent database must not mark its tables as shared/system")
}

// TestDatabaseBuilder_Grants_ToleratesUnresolvedDatabase verifies Grants returns no grants and no
// error when the database can't be resolved, rather than an error the SDK could read as a revocation.
func TestDatabaseBuilder_Grants_ToleratesUnresolvedDatabase(t *testing.T) {
	server := serveUnresolvedParentDatabase(t)
	defer server.Close()

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	builder := &databaseBuilder{resourceType: databaseResourceType, client: client}
	resource, err := databaseResource(&snowflake.Database{Name: "DB"}, false)
	require.NoError(t, err)

	grants, results, err := builder.Grants(context.Background(), resource, rs.SyncOpAttrs{PageToken: pagination.Token{}})
	require.NoError(t, err)
	assert.Nil(t, results)
	assert.Empty(t, grants, "an unresolved database must yield no ownership grants, not an error")
}

// accessControlErrorBody is the body Snowflake pairs with a 422 when the connector role lacks
// privileges on the object a SHOW statement names. Error code 003001 is the SQL access-control
// denial; note the message names neither the status nor the number 422.
var accessControlErrorBody = map[string]any{
	"code":    "003001",
	"message": "SQL access control error:\nInsufficient privileges",
}

// schemaListMock configures newSchemaListMockServer. Both denial knobs reproduce the same
// Snowflake behaviour at different depths: the API answers 422 with error code 003001 for any
// object the connector role is not entitled to observe.
type schemaListMock struct {
	// schemasStatus is the HTTP status SHOW SCHEMAS IN DATABASE answers with. 422 is what
	// Snowflake returns when the role holds no USAGE on the database.
	schemasStatus int
	// schemasErrorBody overrides the body paired with a non-200 schemasStatus. It defaults to the
	// access-control denial; set it to reproduce a 422 that carries a different error code.
	schemasErrorBody map[string]any
	// schemaNames are the rows SHOW SCHEMAS returns when schemasStatus is 200.
	schemaNames []string
	// unreadableSchemas answer SHOW TABLES IN SCHEMA with 422 - the role can see the database
	// but not that schema. Schema privileges are independent of the database's.
	unreadableSchemas map[string]bool
	// listedSchemas, when set, records every SHOW TABLES IN SCHEMA statement issued, including
	// the ones that go on to be denied.
	listedSchemas *[]string
}

// isUnreadable reports whether a SHOW TABLES IN SCHEMA statement targets a schema configured to
// answer 422. The statement quotes the schema as the second identifier: "DB"."SCHEMA".
func (c schemaListMock) isUnreadable(statement string) bool {
	for schema := range c.unreadableSchemas {
		if strings.Contains(statement, fmt.Sprintf(`."%s"`, schema)) {
			return true
		}
	}
	return false
}

// newSchemaListMockServer serves the calls tableBuilder.List makes: SHOW DATABASES LIKE (the
// parent-database lookup, which the real Statements API answers inline on the POST), SHOW SCHEMAS
// IN DATABASE, and SHOW TABLES IN SCHEMA per schema.
func newSchemaListMockServer(t *testing.T, cfg schemaListMock) *httptest.Server {
	t.Helper()
	const schemasHandle = "schemas-handle"
	const tablesHandle = "tables-handle"
	databaseRowTypes := []map[string]any{
		{keyName: keyName, keyType: colText},
		{keyName: colOwner, keyType: colText},
		{keyName: colKind, keyType: colText},
		{keyName: colOrigin, keyType: colText},
	}
	schemaRowTypes := []map[string]any{
		{keyName: keyName, keyType: colText},
		{keyName: colDatabaseName, keyType: colText},
	}
	tableRowTypes := []map[string]any{
		{keyName: colCreatedOn, keyType: colTimestampLtz},
		{keyName: keyName, keyType: colText},
		{keyName: colSchemaName, keyType: colText},
		{keyName: colDatabaseName, keyType: colText},
		{keyName: colKind, keyType: colText},
		{keyName: colComment, keyType: colText},
		{keyName: colOwner, keyType: colText},
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)

		switch r.Method {
		case http.MethodPost:
			var body struct {
				Statement string `json:"statement"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			switch {
			case strings.Contains(body.Statement, "SHOW DATABASES LIKE"):
				_ = enc.Encode(map[string]any{
					"resultSetMetadata": map[string]any{
						"numRows":       1,
						"partitionInfo": []map[string]any{{"rowCount": 1}},
						"rowType":       databaseRowTypes,
					},
					"data": [][]string{{"DB", "SYSADMIN", "STANDARD", ""}},
				})
			case strings.Contains(body.Statement, "SHOW SCHEMAS IN DATABASE"):
				if cfg.schemasStatus != http.StatusOK {
					errorBody := cfg.schemasErrorBody
					if errorBody == nil {
						errorBody = accessControlErrorBody
					}
					w.WriteHeader(cfg.schemasStatus)
					_ = enc.Encode(errorBody)
					return
				}
				_ = enc.Encode(map[string]any{"statementHandle": schemasHandle})
			case strings.Contains(body.Statement, "SHOW TABLES IN SCHEMA"):
				if cfg.listedSchemas != nil {
					*cfg.listedSchemas = append(*cfg.listedSchemas, body.Statement)
				}
				if cfg.isUnreadable(body.Statement) {
					w.WriteHeader(http.StatusUnprocessableEntity)
					_ = enc.Encode(accessControlErrorBody)
					return
				}
				_ = enc.Encode(map[string]any{"statementHandle": tablesHandle})
			default:
				t.Errorf("unexpected statement: %s", body.Statement)
				w.WriteHeader(http.StatusBadRequest)
			}

		case http.MethodGet:
			handle := strings.TrimSuffix(r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:], "/")
			switch handle {
			case schemasHandle:
				rows := make([][]string, 0, len(cfg.schemaNames))
				for _, name := range cfg.schemaNames {
					rows = append(rows, []string{name, "DB"})
				}
				_ = enc.Encode(map[string]any{
					"statementHandle": schemasHandle,
					"resultSetMetadata": map[string]any{
						"numRows":       len(rows),
						"partitionInfo": []map[string]any{{"rowCount": len(rows)}},
						"rowType":       schemaRowTypes,
					},
					"data": rows,
				})
			case tablesHandle:
				_ = enc.Encode(map[string]any{
					"statementHandle": tablesHandle,
					"resultSetMetadata": map[string]any{
						"numRows":       0,
						"partitionInfo": []map[string]any{{"rowCount": 0}},
						"rowType":       tableRowTypes,
					},
					"data": [][]string{},
				})
			default:
				t.Errorf("unexpected statement handle: %s", handle)
				w.WriteHeader(http.StatusBadRequest)
			}

		default:
			t.Errorf("unexpected method: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}

// TestTableBuilder_List_SkipsDatabaseWhenSchemasAreNotVisible pins CXH-2193. A 422 from
// SHOW SCHEMAS IN DATABASE means the connector role cannot see that database; the sibling
// GetDatabase call four lines above already tolerates the same status. Propagating it
// instead cancels the SDK's shared sync context, so a permission gap on one database
// discards every resource type that had already synced.
func TestTableBuilder_List_SkipsDatabaseWhenSchemasAreNotVisible(t *testing.T) {
	server := newSchemaListMockServer(t, schemaListMock{schemasStatus: http.StatusUnprocessableEntity})
	defer server.Close()

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	builder := &tableBuilder{client: client}
	parentID := &v2.ResourceId{ResourceType: databaseResourceType.Id, Resource: "DB"}

	resources, results, err := builder.List(context.Background(), parentID, rs.SyncOpAttrs{})

	require.NoError(t, err, "a 422 on SHOW SCHEMAS must skip the database, not fail the sync")
	assert.Empty(t, resources, "an unreadable database contributes no tables")
	require.NotNil(t, results)
	assert.Empty(t, results.NextPageToken, "skipping must not leave a page token that resumes the same database")
}

// TestTableBuilder_List_FailsOnNonAccessControl422 guards the blast radius of the fix above.
// Snowflake also answers 422 for SQL compilation errors, which mean the connector sent a statement
// the server could not run. Skipping those would turn a connector bug into a silently short sync
// that reports less access than the tenant actually has, so only error code 003001 is skippable.
func TestTableBuilder_List_FailsOnNonAccessControl422(t *testing.T) {
	server := newSchemaListMockServer(t, schemaListMock{
		schemasStatus:    http.StatusUnprocessableEntity,
		schemasErrorBody: map[string]any{"code": "002003", "message": "SQL compilation error:\nObject does not exist"},
	})
	defer server.Close()

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	builder := &tableBuilder{client: client}
	parentID := &v2.ResourceId{ResourceType: databaseResourceType.Id, Resource: "DB"}

	_, _, err = builder.List(context.Background(), parentID, rs.SyncOpAttrs{})

	require.Error(t, err, "a 422 that is not an access-control denial must still fail the sync")
	assert.False(t, snowflake.IsInsufficientPrivileges(err))
}

// TestTableBuilder_List_PropagatesNonSkippableHTTPStatuses is the connector-layer half of the
// CXH-2193 regression gate: auth / rate-limit / 5xx on SHOW SCHEMAS must still fail List, not
// degrade to an empty page the way an access-control 422 does.
func TestTableBuilder_List_PropagatesNonSkippableHTTPStatuses(t *testing.T) {
	statuses := []struct {
		name   string
		status int
		body   map[string]any
	}{
		{
			name:   "401 Unauthorized",
			status: http.StatusUnauthorized,
			body:   map[string]any{"code": "390144", "message": "JWT token is invalid"},
		},
		{
			name:   "403 Forbidden",
			status: http.StatusForbidden,
			body:   map[string]any{"code": "390189", "message": "Role is not authorized"},
		},
		{
			name:   "429 Too Many Requests",
			status: http.StatusTooManyRequests,
			body:   map[string]any{"code": "390100", "message": "rate limit exceeded"},
		},
		{
			name:   "500 Internal Server Error",
			status: http.StatusInternalServerError,
			body:   map[string]any{"code": "000000", "message": "Internal server error"},
		},
		{
			name:   "422 with empty error code",
			status: http.StatusUnprocessableEntity,
			body:   map[string]any{"code": "", "message": "something went wrong"},
		},
	}

	for _, tt := range statuses {
		t.Run(tt.name, func(t *testing.T) {
			server := newSchemaListMockServer(t, schemaListMock{
				schemasStatus:    tt.status,
				schemasErrorBody: tt.body,
			})
			defer server.Close()

			client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
			require.NoError(t, err)

			builder := &tableBuilder{client: client}
			parentID := &v2.ResourceId{ResourceType: databaseResourceType.Id, Resource: "DB"}

			resources, results, err := builder.List(context.Background(), parentID, rs.SyncOpAttrs{})

			require.Error(t, err, "must fail the sync rather than skipping the database")
			assert.Nil(t, resources)
			assert.Nil(t, results)
			assert.False(t, snowflake.IsInsufficientPrivileges(err))
		})
	}
}

// TestDatabaseBuilder_Grants_PropagatesNon422OwnerLookupFailures pins that the owner-skip branch
// added for undescribable system roles does not also absorb auth or server failures on SHOW ROLES.
func TestDatabaseBuilder_Grants_PropagatesNon422OwnerLookupFailures(t *testing.T) {
	statuses := []struct {
		name   string
		status int
		body   map[string]any
	}{
		{
			name:   "401 Unauthorized",
			status: http.StatusUnauthorized,
			body:   map[string]any{"code": "390144", "message": "JWT token is invalid"},
		},
		{
			name:   "500 Internal Server Error",
			status: http.StatusInternalServerError,
			body:   map[string]any{"code": "000000", "message": "Internal server error"},
		},
	}

	for _, tt := range statuses {
		t.Run(tt.name, func(t *testing.T) {
			server := newDatabaseGrantsStatusMockServer(t, "SYSADMIN", tt.status, tt.body)
			defer server.Close()

			client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
			require.NoError(t, err)

			builder := &databaseBuilder{client: client}
			resource := &v2.Resource{Id: &v2.ResourceId{ResourceType: databaseResourceType.Id, Resource: "DB"}}

			grants, results, err := builder.Grants(context.Background(), resource, rs.SyncOpAttrs{})

			require.Error(t, err, "owner lookup failures other than 422 must still fail Grants")
			assert.Nil(t, grants)
			assert.Nil(t, results)
		})
	}
}

// newDatabaseGrantsStatusMockServer is like newDatabaseGrantsMockServer but lets the caller pick
// the SHOW ROLES response status/body so non-422 failures can be asserted.
func newDatabaseGrantsStatusMockServer(t *testing.T, owner string, rolesStatus int, rolesBody map[string]any) *httptest.Server {
	t.Helper()

	databaseRowTypes := []map[string]any{
		{keyName: keyName, keyType: colText},
		{keyName: colOwner, keyType: colText},
		{keyName: colKind, keyType: colText},
		{keyName: colOrigin, keyType: colText},
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)

		var body struct {
			Statement string `json:"statement"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)

		switch {
		case strings.Contains(body.Statement, "SHOW DATABASES LIKE"):
			_ = enc.Encode(map[string]any{
				"resultSetMetadata": map[string]any{
					"numRows":       1,
					"partitionInfo": []map[string]any{{"rowCount": 1}},
					"rowType":       databaseRowTypes,
				},
				"data": [][]string{{"DB", owner, "STANDARD", ""}},
			})
		case strings.Contains(body.Statement, "SHOW ROLES LIKE"):
			w.WriteHeader(rolesStatus)
			_ = enc.Encode(rolesBody)
		default:
			t.Errorf("unexpected statement: %s", body.Statement)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}


// TestTableBuilder_List_EnumeratesSchemasWhenVisible is the control for the test above: it
// proves the mock drives the real code path, so the 422 case cannot pass for the wrong reason.
func TestTableBuilder_List_EnumeratesSchemasWhenVisible(t *testing.T) {
	var listed []string
	server := newSchemaListMockServer(t, schemaListMock{
		schemasStatus: http.StatusOK,
		schemaNames:   []string{publicSchema, "INFORMATION_SCHEMA"},
		listedSchemas: &listed,
	})
	defer server.Close()

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	builder := &tableBuilder{client: client}
	parentID := &v2.ResourceId{ResourceType: databaseResourceType.Id, Resource: "DB"}

	_, _, err = builder.List(context.Background(), parentID, rs.SyncOpAttrs{})

	require.NoError(t, err)
	require.Len(t, listed, 1, "PUBLIC is listed; INFORMATION_SCHEMA is skipped")
	assert.Contains(t, listed[0], `"DB"."PUBLIC"`)
}

// TestTableBuilder_List_SkipsUnreadableSchemaAndContinues covers the level below CXH-2193's
// reproduction: schema privileges are independent of the database's, so a readable database can
// still contain a schema the role cannot list tables in. Skipping it has to advance the pagination
// bag - returning the current token instead would re-request the same denied schema forever, so
// this drives the SDK's loop to completion rather than asserting on a single call.
func TestTableBuilder_List_SkipsUnreadableSchemaAndContinues(t *testing.T) {
	var listed []string
	server := newSchemaListMockServer(t, schemaListMock{
		schemasStatus:     http.StatusOK,
		schemaNames:       []string{"LOCKED", publicSchema},
		unreadableSchemas: map[string]bool{"LOCKED": true},
		listedSchemas:     &listed,
	})
	defer server.Close()

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	builder := &tableBuilder{client: client}
	parentID := &v2.ResourceId{ResourceType: databaseResourceType.Id, Resource: "DB"}

	const maxPages = 10
	token := ""
	pages := 0
	for {
		_, results, err := builder.List(context.Background(), parentID, rs.SyncOpAttrs{PageToken: pagination.Token{Token: token}})
		require.NoError(t, err, "an unreadable schema must not fail the whole database")
		pages++
		require.Less(t, pages, maxPages, "pagination must terminate")
		if results == nil || results.NextPageToken == "" {
			break
		}
		token = results.NextPageToken
	}

	require.Len(t, listed, 2, "each schema is attempted exactly once - a denied schema must not be retried")
	attempted := strings.Join(listed, "\n")
	assert.Contains(t, attempted, `"DB"."LOCKED"`)
	assert.Contains(t, attempted, `"DB"."PUBLIC"`, "the readable schema is still reached after the denied one")
}

// newGrantsDeniedServer answers SHOW GRANTS ON TABLE/VIEW with the 422 Snowflake returns when the
// connector role can see the table but not its grants.
func newGrantsDeniedServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Statement string `json:"statement"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if !strings.Contains(body.Statement, "SHOW GRANTS ON") {
			t.Errorf("unexpected statement: %s", body.Statement)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_ = json.NewEncoder(w).Encode(accessControlErrorBody)
	}))
}

// TestTableBuilder_Entitlements_FallsBackToOwnerWhenGrantsNotVisible covers the third call site of
// the same bug class. SHOW GRANTS ON TABLE reports its denial through Snowflake's own error text,
// which never mentions 422 - only the sentinel the client joins makes it recognisable here.
func TestTableBuilder_Entitlements_FallsBackToOwnerWhenGrantsNotVisible(t *testing.T) {
	server := newGrantsDeniedServer(t)
	defer server.Close()

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	builder := &tableBuilder{client: client}
	resource := makeTableResource(t, "DB", "SCHEMA", "MYTABLE")

	entitlements, results, err := builder.Entitlements(context.Background(), resource, rs.SyncOpAttrs{})

	require.NoError(t, err, "a table whose grants are not visible must not fail the sync")
	require.Len(t, entitlements, 1, "the owner entitlement is derived from the table itself, so it survives the denial")
	assert.Equal(t,
		fmt.Sprintf("%s:%s:%s", tableResourceType.Id, resource.Id.Resource, ownerEntitlement),
		entitlements[0].Id)
	require.NotNil(t, results)
	assert.Empty(t, results.NextPageToken)
}

// TestTableBuilder_Grants_SkipsTableWhenGrantsNotVisible is the Grants half of the case above.
func TestTableBuilder_Grants_SkipsTableWhenGrantsNotVisible(t *testing.T) {
	server := newGrantsDeniedServer(t)
	defer server.Close()

	client, err := snowflake.New(server.URL, snowflake.JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	builder := &tableBuilder{client: client}
	resource := makeTableResource(t, "DB", "SCHEMA", "MYTABLE")

	grants, results, err := builder.Grants(context.Background(), resource, rs.SyncOpAttrs{})

	require.NoError(t, err, "a table whose grants are not visible must not fail the sync")
	assert.Empty(t, grants)
	require.NotNil(t, results)
	assert.Empty(t, results.NextPageToken)
}

// makePartialProfileResource creates a resource with profile missing the "name" field,
// forcing a fallback to the split-based parsing.
func makePartialProfileResource(t *testing.T, dbName, schemaName, tableName string) *v2.Resource {
	t.Helper()
	profile := map[string]any{
		"database_name": dbName,
		"schema_name":   schemaName,
		// "name" intentionally omitted
	}
	tableId := fmt.Sprintf("%s.%s.%s", dbName, schemaName, tableName)
	resource, err := rs.NewAppResource(
		tableName,
		tableResourceType,
		tableId,
		nil,
		rs.WithResourceProfile(profile),
	)
	require.NoError(t, err)
	return resource
}
