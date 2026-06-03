package connector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mapSessionStore duplicates the type in pkg/snowflake/database_test.go —
// Go does not allow importing _test.go symbols across packages.
// IMPORTANT: Keep the two copies in sync if the SessionStore interface changes.
type mapSessionStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMapSessionStore() *mapSessionStore {
	return &mapSessionStore{data: make(map[string][]byte)}
}

func (m *mapSessionStore) compositeKey(key string, opt []sessions.SessionStoreOption) string {
	bag := &sessions.SessionStoreBag{}
	for _, o := range opt {
		_ = o(context.Background(), bag)
	}
	if bag.Prefix != "" {
		return bag.Prefix + "/" + key
	}
	return key
}

func (m *mapSessionStore) Get(_ context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[m.compositeKey(key, opt)]
	return v, ok, nil
}

func (m *mapSessionStore) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	result := make(map[string][]byte, len(keys))
	for _, k := range keys {
		v, found, err := m.Get(ctx, k, opt...)
		if err != nil {
			return nil, nil, err
		}
		if found {
			result[k] = v
		}
	}
	return result, nil, nil
}

func (m *mapSessionStore) Set(_ context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[m.compositeKey(key, opt)] = value
	return nil
}

func (m *mapSessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	for k, v := range values {
		if err := m.Set(ctx, k, v, opt...); err != nil {
			return err
		}
	}
	return nil
}

func (m *mapSessionStore) Delete(_ context.Context, key string, opt ...sessions.SessionStoreOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, m.compositeKey(key, opt))
	return nil
}

func (m *mapSessionStore) Clear(_ context.Context, _ ...sessions.SessionStoreOption) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string][]byte)
	return nil
}

func (m *mapSessionStore) GetAll(_ context.Context, _ string, _ ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make(map[string][]byte, len(m.data))
	for k, v := range m.data {
		cp[k] = v
	}
	return cp, "", nil
}

func newConnectorTestClient(t *testing.T, handler http.Handler) (*snowflake.Client, func()) {
	t.Helper()
	ts := httptest.NewServer(handler)
	client, err := snowflake.New(ts.URL, snowflake.JWTConfig{}, ts.Client())
	require.NoError(t, err)
	return client, ts.Close
}

// JSON response bodies for mock HTTP servers. Column order in rowType matches the
// struct field→column maps in pkg/snowflake/table.go and database.go.
const (
	// 422 with code "002003" (not "003001") — triggers ErrObjectNotFound in ListTableGrants / GetTable.
	// Code "003001" means genuine RBAC privilege denial (PermissionDenied); any other 422 soft-skips.
	body422ObjectNotFound = `{"code":"002003","message":"Object does not exist or not authorized."}`

	// 200 OK, one grant row with granted_to="APPLICATION" — bypasses the len==0 early-return
	// without triggering GetAccountRole or GetUser, and leaves ownerPrincipalID==nil.
	// created_on must be a float seconds-since-epoch value (Snowflake TIMESTAMP_LTZ wire format).
	body200OneApplicationGrant = `{"resultSetMetadata":{"numRows":1,"rowType":[` +
		`{"name":"created_on","type":"timestamp_ltz"},` +
		`{"name":"privilege","type":"text"},` +
		`{"name":"granted_on","type":"text"},` +
		`{"name":"name","type":"text"},` +
		`{"name":"granted_to","type":"text"},` +
		`{"name":"grantee_name","type":"text"},` +
		`{"name":"grant_option","type":"text"},` +
		`{"name":"granted_by","type":"text"}` +
		`]},"data":[["1704067200.000000000","SELECT","TABLE","MYDB.PUBLIC.MYTABLE","APPLICATION","MYAPP","false","SYSADMIN"]],"statementHandle":"","code":"","message":""}`

	// 200 OK, one STANDARD database row. rowType matches databaseStructFieldToColumnMap.
	body200StandardDB = `{"resultSetMetadata":{"numRows":1,"rowType":[` +
		`{"name":"name","type":"text"},` +
		`{"name":"owner","type":"text"},` +
		`{"name":"kind","type":"text"},` +
		`{"name":"origin","type":"text"}` +
		`]},"data":[["MYDB","SYSADMIN","STANDARD",""]],"statementHandle":"","code":"","message":""}`

	// 200 OK, one SHARED database row.
	body200SharedDB = `{"resultSetMetadata":{"numRows":1,"rowType":[` +
		`{"name":"name","type":"text"},` +
		`{"name":"owner","type":"text"},` +
		`{"name":"kind","type":"text"},` +
		`{"name":"origin","type":"text"}` +
		`]},"data":[["SHAREDDB","SYSADMIN","SHARED",""]],"statementHandle":"","code":"","message":""}`
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

// makePartialProfileResource creates a resource with profile missing the "name" field,
// forcing a fallback to the split-based parsing.
func makePartialProfileResource(t *testing.T, dbName, schemaName, tableName string) *v2.Resource {
	t.Helper()
	profile := map[string]interface{}{
		"database_name": dbName,
		"schema_name":   schemaName,
		// "name" intentionally omitted
	}
	tableTraits := []rs.AppTraitOption{
		rs.WithAppProfile(profile),
	}
	tableId := fmt.Sprintf("%s.%s.%s", dbName, schemaName, tableName)
	resource, err := rs.NewAppResource(
		tableName,
		tableResourceType,
		tableId,
		tableTraits,
	)
	require.NoError(t, err)
	return resource
}

func TestTableBuilderSkipsWhenSyncTablesDisabled(t *testing.T) {
	builder := &tableBuilder{syncTables: false}
	parentID := &v2.ResourceId{ResourceType: databaseResourceType.Id, Resource: "MYDB"}
	resources, results, err := builder.List(context.Background(), parentID, rs.SyncOpAttrs{})
	require.NoError(t, err)
	require.Empty(t, resources)
	require.NotNil(t, results)
}

// seedDB writes a STANDARD MYDB entry to ss so isDBSharedOrSystem is answered
// from the cache without an HTTP call.
func seedStandardDB(t *testing.T, ctx context.Context, ss *mapSessionStore) {
	t.Helper()
	// CacheDatabases only uses the session store, not any Client fields,
	// so a zero-value Client is safe to use here.
	err := (&snowflake.Client{}).CacheDatabases(ctx, ss, []snowflake.Database{
		{Name: "MYDB", Owner: "SYSADMIN", Kind: "STANDARD"},
	})
	require.NoError(t, err)
}

// --- Gap 1: ErrObjectNotFound soft-skip ---

func TestEntitlements_ErrObjectNotFound_SoftSkip(t *testing.T) {
	ctx := context.Background()
	ss := newMapSessionStore()
	seedStandardDB(t, ctx, ss)

	client, cleanup := newConnectorTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(body422ObjectNotFound))
	}))
	defer cleanup()

	builder := &tableBuilder{client: client, syncTables: true}
	resource := makeTableResource(t, "MYDB", "PUBLIC", "MYTABLE")
	ents, results, err := builder.Entitlements(ctx, resource, rs.SyncOpAttrs{Session: ss})

	require.NoError(t, err)
	assert.NotNil(t, results)
	assert.Nil(t, ents)
}

func TestGrants_ErrObjectNotFound_ListTableGrants_SoftSkip(t *testing.T) {
	ctx := context.Background()
	ss := newMapSessionStore()
	seedStandardDB(t, ctx, ss)

	client, cleanup := newConnectorTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(body422ObjectNotFound))
	}))
	defer cleanup()

	builder := &tableBuilder{client: client, syncTables: true}
	resource := makeTableResource(t, "MYDB", "PUBLIC", "MYTABLE")
	grants, results, err := builder.Grants(ctx, resource, rs.SyncOpAttrs{Session: ss})

	require.NoError(t, err)
	assert.NotNil(t, results)
	assert.Nil(t, grants)
}

func TestGrants_ErrObjectNotFound_GetTable_ReturnsPartialGrants(t *testing.T) {
	ctx := context.Background()
	ss := newMapSessionStore()
	seedStandardDB(t, ctx, ss)

	var callCount int
	client, cleanup := newConnectorTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		if callCount == 1 {
			// ListTableGrants: one APPLICATION grant row — bypasses the len==0 early-return
			// without triggering GetAccountRole/GetUser; leaves ownerPrincipalID==nil.
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body200OneApplicationGrant))
		} else {
			// GetTable owner fallback: 422 → ErrObjectNotFound → partial grants returned.
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(body422ObjectNotFound))
		}
	}))
	defer cleanup()

	builder := &tableBuilder{client: client, syncTables: true}
	resource := makeTableResource(t, "MYDB", "PUBLIC", "MYTABLE")
	grants, results, err := builder.Grants(ctx, resource, rs.SyncOpAttrs{Session: ss})

	require.NoError(t, err)
	assert.NotNil(t, results)
	// grants is nil because the only grant row has granted_to="APPLICATION", which the
	// grant-building switch skips (only ROLE and USER are handled), so nothing is ever appended.
	// GetTable then returns ErrObjectNotFound, triggering the early return — which returns
	// whatever is in grants at that point (nil here). "Partial" means this return could contain
	// a non-empty slice in other fixtures.
	assert.Nil(t, grants)
	assert.Equal(t, 2, callCount, "expected ListTableGrants + GetTable calls")
}

// --- Gap 2: isDBSharedOrSystem cache paths ---

func TestIsDBSharedOrSystem_CacheMiss_StandardDB_ReturnsFalse(t *testing.T) {
	ctx := context.Background()
	ss := newMapSessionStore() // empty — cache miss forces HTTP call

	client, cleanup := newConnectorTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body200StandardDB))
	}))
	defer cleanup()

	builder := &tableBuilder{client: client}
	resource := makeBareResource(t, "MYDB.PUBLIC.MYTABLE") // no profile → fast path skipped
	result, err := builder.isDBSharedOrSystem(ctx, ss, resource, "MYDB")

	require.NoError(t, err)
	assert.False(t, result)
}

func TestIsDBSharedOrSystem_CacheMiss_SharedDB_ReturnsTrue(t *testing.T) {
	ctx := context.Background()
	ss := newMapSessionStore()

	client, cleanup := newConnectorTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body200SharedDB))
	}))
	defer cleanup()

	builder := &tableBuilder{client: client}
	resource := makeBareResource(t, "SHAREDDB.PUBLIC.MYTABLE")
	result, err := builder.isDBSharedOrSystem(ctx, ss, resource, "SHAREDDB")

	require.NoError(t, err)
	assert.True(t, result)
}

func TestIsDBSharedOrSystem_CacheHit_NoHTTPCall(t *testing.T) {
	ctx := context.Background()
	ss := newMapSessionStore()
	seedStandardDB(t, ctx, ss)

	// &snowflake.Client{} has no transport — any real HTTP call would panic.
	builder := &tableBuilder{client: &snowflake.Client{}}
	resource := makeBareResource(t, "MYDB.PUBLIC.MYTABLE")
	result, err := builder.isDBSharedOrSystem(ctx, ss, resource, "MYDB")

	require.NoError(t, err)
	assert.False(t, result)
}

func TestIsDBSharedOrSystem_FastPath_ProfileFieldTrue(t *testing.T) {
	ctx := context.Background()
	parentID := &v2.ResourceId{ResourceType: databaseResourceType.Id, Resource: "SHAREDDB"}
	table := &snowflake.Table{DatabaseName: "SHAREDDB", SchemaName: "PUBLIC", Name: "T", Kind: "TABLE"}
	resource, err := tableResource(ctx, table, parentID, true) // embeds database_is_shared_system: true
	require.NoError(t, err)

	// &snowflake.Client{} has no transport — any real HTTP call would panic.
	builder := &tableBuilder{client: &snowflake.Client{}}
	result, callErr := builder.isDBSharedOrSystem(ctx, nil, resource, "SHAREDDB")

	require.NoError(t, callErr)
	assert.True(t, result)
}
