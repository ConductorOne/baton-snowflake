package snowflake

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSessionStore is a minimal in-memory sessions.SessionStore for exercising the
// ListTableGrants caching/accumulation logic without a real gRPC session backend.
type fakeSessionStore struct {
	data map[string][]byte
}

func newFakeSessionStore() *fakeSessionStore {
	return &fakeSessionStore{data: map[string][]byte{}}
}

func (f *fakeSessionStore) resolveKey(key string, opt ...sessions.SessionStoreOption) (string, error) {
	bag := &sessions.SessionStoreBag{}
	for _, o := range opt {
		if err := o(context.Background(), bag); err != nil {
			return "", err
		}
	}
	return bag.SyncID + "/" + bag.Prefix + "/" + key, nil
}

func (f *fakeSessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	k, err := f.resolveKey(key, opt...)
	if err != nil {
		return nil, false, err
	}
	v, ok := f.data[k]
	return v, ok, nil
}

func (f *fakeSessionStore) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	k, err := f.resolveKey(key, opt...)
	if err != nil {
		return err
	}
	f.data[k] = value
	return nil
}

func (f *fakeSessionStore) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	k, err := f.resolveKey(key, opt...)
	if err != nil {
		return err
	}
	delete(f.data, k)
	return nil
}

func (f *fakeSessionStore) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	result := make(map[string][]byte)
	var missing []string
	for _, key := range keys {
		v, ok, err := f.Get(ctx, key, opt...)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			result[key] = v
		} else {
			missing = append(missing, key)
		}
	}
	return result, missing, nil
}

func (f *fakeSessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	for key, value := range values {
		if err := f.Set(ctx, key, value, opt...); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeSessionStore) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	f.data = map[string][]byte{}
	return nil
}

func (f *fakeSessionStore) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	return f.data, "", nil
}

var _ sessions.SessionStore = (*fakeSessionStore)(nil)

// namespacePrefix resolves the prefix a sessions.SessionStoreOption (e.g. tableGrantsPartialNamespace)
// applies to a SessionStoreBag, so tests can target a specific namespace without hardcoding its string.
func namespacePrefix(opt sessions.SessionStoreOption) string {
	bag := &sessions.SessionStoreBag{}
	_ = opt(context.Background(), bag)
	return bag.Prefix
}

// failingSessionStore wraps a fakeSessionStore and injects an error from Set and/or Get
// whenever the call resolves to the given namespace prefix, to exercise the
// not-best-effort error propagation in ListTableGrants' pagination accumulation.
type failingSessionStore struct {
	*fakeSessionStore
	failSetForPrefix string
	failGetForPrefix string
}

func (f *failingSessionStore) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	if f.failSetForPrefix != "" && namespacePrefix(opt[0]) == f.failSetForPrefix {
		return errors.New("injected set failure")
	}
	return f.fakeSessionStore.Set(ctx, key, value, opt...)
}

func (f *failingSessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	if f.failGetForPrefix != "" && namespacePrefix(opt[0]) == f.failGetForPrefix {
		return nil, false, errors.New("injected get failure")
	}
	return f.fakeSessionStore.Get(ctx, key, opt...)
}

var _ sessions.SessionStore = (*failingSessionStore)(nil)

const testObjectKind = "TABLE"

func tableGrantRowTypes() []map[string]interface{} {
	return []map[string]interface{}{
		{"name": "created_on", "type": "timestamp_ltz"},
		{"name": "privilege", "type": "text"},
		{"name": "granted_on", "type": "text"},
		{"name": "name", "type": "text"},
		{"name": "granted_to", "type": "text"},
		{"name": "grantee_name", "type": "text"},
		{"name": "grant_option", "type": "text"},
		{"name": "granted_by", "type": "text"},
	}
}

// tableGrantRow builds a data row matching the column order from tableGrantRowTypes.
func tableGrantRow(privilege, grantedTo, granteeName string) []string {
	return []string{"1700000000.000000000", privilege, testObjectKind, "MYTABLE", grantedTo, granteeName, "false", "ACCOUNTADMIN"}
}

// serveTableGrants returns an httptest.Server implementing the Snowflake Statements API for
// SHOW GRANTS ON TABLE/VIEW. partition0Rows comes back on the initial GET along with the full
// resultSetMetadata (rowType + partitionInfo); if partition1Rows is non-nil, a second partition
// is advertised and served on ?partition=1 with data only (no metadata), matching real API behavior.
func serveTableGrants(t *testing.T, handle string, partition0Rows, partition1Rows [][]string) *httptest.Server {
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
			_, hasPartition := r.URL.Query()["partition"]
			if !hasPartition {
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
						"rowType":       tableGrantRowTypes(),
					},
					"data": partition0Rows,
				})
			} else {
				require.Equal(t, "1", r.URL.Query().Get("partition"), "only partition 1 expected in this test")
				_ = enc.Encode(map[string]interface{}{
					"data": partition1Rows,
				})
			}

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
}

func TestListTableGrants_SinglePartition(t *testing.T) {
	const handle = "handle-single"

	rows := [][]string{
		tableGrantRow("SELECT", "ROLE", "ANALYST"),
		tableGrantRow("OWNERSHIP", "ROLE", "SYSADMIN"),
	}
	server := serveTableGrants(t, handle, rows, nil)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	grants, nextCursor, err := client.ListTableGrants(context.Background(), nil, "DB", "SCHEMA", "MYTABLE", testObjectKind, "")
	require.NoError(t, err)
	assert.Empty(t, nextCursor, "single partition should produce no next cursor")
	require.Len(t, grants, 2)
	assert.Equal(t, "SELECT", grants[0].Privilege)
	assert.Equal(t, "ANALYST", grants[0].GranteeName)
	assert.Equal(t, "OWNERSHIP", grants[1].Privilege)
	assert.Equal(t, "SYSADMIN", grants[1].GranteeName)
}

// TestListTableGrants_UnquotesGranteeName verifies the CXP-784 fix: Snowflake's SHOW GRANTS
// ON TABLE/VIEW renders grantee names that require quoting wrapped in double quotes, with
// embedded double quotes doubled. GranteeName must come back unquoted so it matches the
// canonical (unquoted) ID that SHOW ROLES/SHOW USERS produce for the same principal.
func TestListTableGrants_UnquotesGranteeName(t *testing.T) {
	const handle = "handle-quoted"

	rows := [][]string{
		tableGrantRow("SELECT", "ROLE", `"Data Engineer"`),
		tableGrantRow("SELECT", "USER", `"He said ""hi"""`),
		tableGrantRow("SELECT", "ROLE", "SYSADMIN"),
	}
	server := serveTableGrants(t, handle, rows, nil)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	grants, nextCursor, err := client.ListTableGrants(context.Background(), nil, "DB", "SCHEMA", "MYTABLE", testObjectKind, "")
	require.NoError(t, err)
	assert.Empty(t, nextCursor)
	require.Len(t, grants, 3)
	assert.Equal(t, "Data Engineer", grants[0].GranteeName, "quoted mixed-case name should be unquoted")
	assert.Equal(t, `He said "hi"`, grants[1].GranteeName, "embedded escaped quotes should be unescaped")
	assert.Equal(t, "SYSADMIN", grants[2].GranteeName, "already-unquoted system role should be unaffected")
}

func TestListTableGrants_MultiPartition(t *testing.T) {
	const handle = "handle-multi"

	partition0 := [][]string{
		tableGrantRow("SELECT", "ROLE", "ANALYST"),
	}
	partition1 := [][]string{
		tableGrantRow("OWNERSHIP", "ROLE", "SYSADMIN"),
	}
	server := serveTableGrants(t, handle, partition0, partition1)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	ctx := context.Background()

	// Page 1: empty cursor -> executes the query, returns partition 0 + a cursor.
	page1, cursor1, err := client.ListTableGrants(ctx, nil, "DB", "SCHEMA", "MYTABLE", testObjectKind, "")
	require.NoError(t, err)
	require.Len(t, page1, 1)
	assert.Equal(t, "SELECT", page1[0].Privilege)
	assert.NotEmpty(t, cursor1)

	// Page 2: cursor from page 1 -> fetches ?partition=1 and, crucially, must still be able to
	// parse the row even though this response has no resultSetMetadata of its own.
	page2, cursor2, err := client.ListTableGrants(ctx, nil, "DB", "SCHEMA", "MYTABLE", testObjectKind, cursor1)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, "OWNERSHIP", page2[0].Privilege)
	assert.Equal(t, "SYSADMIN", page2[0].GranteeName)
	assert.Empty(t, cursor2, "last partition should return empty cursor")
}

// TestListTableGrants_CachesCompleteResultAcrossCallers verifies the session-store bookkeeping:
// once pagination finishes, the full merged grant list is promoted to the "complete" cache entry so
// a second caller (e.g. Entitlements() after Grants() paginated through the same table) gets it back
// in a single call with no further HTTP requests - reproducing the two-caller pattern in tables.go.
func TestListTableGrants_CachesCompleteResultAcrossCallers(t *testing.T) {
	const handle = "handle-cache"

	partition0 := [][]string{
		tableGrantRow("SELECT", "ROLE", "ANALYST"),
	}
	partition1 := [][]string{
		tableGrantRow("OWNERSHIP", "ROLE", "SYSADMIN"),
	}
	server := serveTableGrants(t, handle, partition0, partition1)

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	ctx := context.Background()
	ss := newFakeSessionStore()

	page1, cursor1, err := client.ListTableGrants(ctx, ss, "DB", "SCHEMA", "MYTABLE", testObjectKind, "")
	require.NoError(t, err)
	require.Len(t, page1, 1)
	require.NotEmpty(t, cursor1)

	page2, cursor2, err := client.ListTableGrants(ctx, ss, "DB", "SCHEMA", "MYTABLE", testObjectKind, cursor1)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	require.Empty(t, cursor2)

	// The upstream server is gone; a cache miss here would fail the request.
	server.Close()

	cached, cachedCursor, err := client.ListTableGrants(ctx, ss, "DB", "SCHEMA", "MYTABLE", testObjectKind, "")
	require.NoError(t, err)
	assert.Empty(t, cachedCursor)
	require.Len(t, cached, 2, "complete cache entry should hold grants merged across every partition")
	assert.Equal(t, "SELECT", cached[0].Privilege)
	assert.Equal(t, "OWNERSHIP", cached[1].Privilege)
}

// TestListTableGrants_PropagatesPartialCacheWriteFailure verifies that a failure persisting
// partition 0's rows into the pagination-progress cache is NOT swallowed. Silently continuing
// here would let a later call's accumulation read reconstruct an incomplete "complete" grant
// list - the same silent-truncation bug class this pagination fix exists to close.
func TestListTableGrants_PropagatesPartialCacheWriteFailure(t *testing.T) {
	const handle = "handle-write-failure"

	partition0 := [][]string{tableGrantRow("SELECT", "ROLE", "ANALYST")}
	partition1 := [][]string{tableGrantRow("OWNERSHIP", "ROLE", "SYSADMIN")}
	server := serveTableGrants(t, handle, partition0, partition1)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	ss := &failingSessionStore{
		fakeSessionStore: newFakeSessionStore(),
		failSetForPrefix: namespacePrefix(tableGrantsPartialNamespace),
	}

	grants, cursor, err := client.ListTableGrants(context.Background(), ss, "DB", "SCHEMA", "MYTABLE", testObjectKind, "")
	require.Error(t, err)
	assert.Nil(t, grants)
	assert.Empty(t, cursor)
}

// TestListTableGrants_PropagatesPartialCacheReadFailure verifies that a failure reading back
// the pagination-progress cache on a later page is NOT swallowed - continuing with a silently
// empty "accumulated" would drop every earlier partition's rows from the eventual "complete"
// cache entry.
func TestListTableGrants_PropagatesPartialCacheReadFailure(t *testing.T) {
	const handle = "handle-read-failure"

	partition0 := [][]string{tableGrantRow("SELECT", "ROLE", "ANALYST")}
	partition1 := [][]string{tableGrantRow("OWNERSHIP", "ROLE", "SYSADMIN")}
	server := serveTableGrants(t, handle, partition0, partition1)
	defer server.Close()

	client, err := New(server.URL, JWTConfig{}, &http.Client{})
	require.NoError(t, err)

	ctx := context.Background()
	ss := &failingSessionStore{fakeSessionStore: newFakeSessionStore()}

	_, cursor1, err := client.ListTableGrants(ctx, ss, "DB", "SCHEMA", "MYTABLE", testObjectKind, "")
	require.NoError(t, err)
	require.NotEmpty(t, cursor1)

	ss.failGetForPrefix = namespacePrefix(tableGrantsPartialNamespace)

	grants, cursor2, err := client.ListTableGrants(ctx, ss, "DB", "SCHEMA", "MYTABLE", testObjectKind, cursor1)
	require.Error(t, err)
	assert.Nil(t, grants)
	assert.Empty(t, cursor2)
}
