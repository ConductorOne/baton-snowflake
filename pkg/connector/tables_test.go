package connector

import (
	"context"
	"fmt"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
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
