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
	colCreatedOn    = "created_on"
	colSchemaName   = "schema_name"
	colDatabaseName = "database_name"
	colKind         = "kind"
	colComment      = "comment"
	colOwner        = "owner"
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

// makePartialProfileResource creates a resource with profile missing the "name" field,
// forcing a fallback to the split-based parsing.
func makePartialProfileResource(t *testing.T, dbName, schemaName, tableName string) *v2.Resource {
	t.Helper()
	profile := map[string]interface{}{
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
