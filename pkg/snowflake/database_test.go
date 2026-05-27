package snowflake

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mapSessionStore is a simple in-memory SessionStore for testing.
// It applies the prefix option by prepending it to the key.
// IMPORTANT: pkg/connector/tables_test.go contains an identical copy (Go does not allow
// importing _test.go symbols across packages). Keep the two in sync.
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

// --- IsSharedOrSystem tests ---

func TestDatabase_IsSharedOrSystem(t *testing.T) {
	cases := []struct {
		name   string
		db     Database
		expect bool
	}{
		{"standard", Database{Name: "X", Owner: "SYSADMIN", Kind: "STANDARD"}, false},
		{"shared", Database{Name: "X", Owner: "SYSADMIN", Kind: "SHARED"}, true},
		{"application", Database{Name: "X", Owner: "SYSADMIN", Kind: "APPLICATION"}, true},
		{"imported database", Database{Name: "X", Owner: "SYSADMIN", Kind: "IMPORTED DATABASE"}, true},
		{"catalog-linked uppercase", Database{Name: "X", Owner: "SYSADMIN", Kind: "CATALOG-LINKED DATABASE"}, true},
		{"catalog-linked lowercase", Database{Name: "X", Owner: "SYSADMIN", Kind: "catalog-linked database"}, true},
		{"catalog-linked with spaces", Database{Name: "X", Owner: "SYSADMIN", Kind: "  CATALOG-LINKED DATABASE  "}, true},
		{"snowflake owner", Database{Name: "X", Owner: "SNOWFLAKE", Kind: "STANDARD"}, true},
		{"snowflake owner lowercase", Database{Name: "X", Owner: "snowflake", Kind: "STANDARD"}, true},
		{"empty owner", Database{Name: "X", Owner: "", Kind: "STANDARD"}, true},
		{"with origin", Database{Name: "X", Owner: "SYSADMIN", Kind: "STANDARD", Origin: "myaccount.myshare"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, tc.db.IsSharedOrSystem())
		})
	}
}

// --- CacheDatabases / GetDatabase caching tests ---

func TestCacheDatabases_NilSession(t *testing.T) {
	c := &Client{}
	err := c.CacheDatabases(context.Background(), nil, []Database{{Name: "DB1"}})
	require.NoError(t, err)
}

func TestCacheDatabases_Empty(t *testing.T) {
	ss := newMapSessionStore()
	c := &Client{}
	err := c.CacheDatabases(context.Background(), ss, nil)
	require.NoError(t, err)
	assert.Empty(t, ss.data)
}

func TestCacheDatabases_PopulatesStore(t *testing.T) {
	ctx := context.Background()
	ss := newMapSessionStore()
	c := &Client{}

	dbs := []Database{
		{Name: "MYDB", Owner: "SYSADMIN", Kind: "STANDARD"},
		{Name: "SHAREDDB", Owner: "SYSADMIN", Kind: "SHARED"},
	}
	require.NoError(t, c.CacheDatabases(ctx, ss, dbs))

	// The store must contain both entries under the database namespace prefix.
	assert.Len(t, ss.data, 2)
	key := fmt.Sprintf("%s/%s", "database", "MYDB")
	assert.Contains(t, ss.data, key)
}

func TestGetDatabase_CacheMiss(t *testing.T) {
	ctx := context.Background()

	var requestCount int
	const dbBody = `{"resultSetMetadata":{"numRows":1,"rowType":[{"name":"name","type":"text"},{"name":"owner","type":"text"},{"name":"kind","type":"text"},{"name":"origin","type":"text"}]},"data":[["MYDB","SYSADMIN","STANDARD",""]],"statementHandle":"","code":"","message":""}`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(dbBody))
	}))
	defer ts.Close()

	client, err := New(ts.URL, JWTConfig{}, ts.Client())
	require.NoError(t, err)

	ss := newMapSessionStore()
	require.NoError(t, client.CacheDatabases(ctx, ss, []Database{{Name: "OTHER", Owner: "SYSADMIN", Kind: "STANDARD"}}))

	db, statusCode, err := client.GetDatabase(ctx, ss, "MYDB")
	require.NoError(t, err)
	assert.Equal(t, 1, requestCount, "server should have been called exactly once")
	require.NotNil(t, db)
	assert.Equal(t, "MYDB", db.Name)
	assert.Equal(t, http.StatusOK, statusCode)
}

func TestGetDatabase_CacheHit(t *testing.T) {
	ctx := context.Background()
	ss := newMapSessionStore()
	c := &Client{}

	// Seed the cache via CacheDatabases.
	seed := []Database{{Name: "MYDB", Owner: "SYSADMIN", Kind: "STANDARD"}}
	require.NoError(t, c.CacheDatabases(ctx, ss, seed))

	// GetDatabase with a populated cache must return the cached entry without
	// hitting the API (Client has no transport, so any real call would panic).
	db, statusCode, err := c.GetDatabase(ctx, ss, "MYDB")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, statusCode, "cache hit should return http.StatusOK")
	require.NotNil(t, db)
	assert.Equal(t, "MYDB", db.Name)
	assert.Equal(t, "SYSADMIN", db.Owner)
	assert.Equal(t, "STANDARD", db.Kind) // Kind drives IsSharedOrSystem — verify it survives cache round-trip
}

