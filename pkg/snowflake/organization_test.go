package snowflake

import "testing"

// TestGetOrganizationAccountsOmitsRegionGroup is a regression test for CXH-2093.
// SHOW ORGANIZATION ACCOUNTS does not return a region_group column for organizations
// that do not span multiple region groups (the common single-region-group case).
// Parsing must succeed against that layout and must not require columns the connector
// does not consume. Before the fix, the OrganizationAccount struct carried a
// RegionGroup field, so ParseRow demanded a region_group column and returned
// "row type region_group not found", which aborted the entire sync.
func TestGetOrganizationAccountsOmitsRegionGroup(t *testing.T) {
	// Mirrors the real response column order, deliberately excluding region_group.
	columns := []string{
		"organization_name",
		"account_name",
		"snowflake_region",
		"edition",
		"account_url",
		"account_locator",
		"is_organization_account",
	}
	rowTypes := make([]RowType, len(columns))
	for i, name := range columns {
		rowTypes[i] = RowType{Name: name, Type: rowTypeString}
	}

	resp := &ListOrganizationAccountsRawResponse{
		StatementsApiResponseBase: StatementsApiResponseBase{
			ResultSetMetadata: ResultSetMetadata{NumRows: 1, RowTypes: rowTypes},
			Data: [][]string{
				{
					"EXAMPLE_ORG",
					"EXAMPLE_ACCOUNT",
					"AWS_US_EAST_1",
					"ENTERPRISE",
					"https://example.snowflakecomputing.com",
					"AB12345",
					"true",
				},
			},
		},
	}

	accounts, err := resp.GetOrganizationAccounts()
	if err != nil {
		t.Fatalf("GetOrganizationAccounts() returned an error for a response without region_group: %v", err)
	}
	if len(accounts) != 1 {
		t.Fatalf("expected 1 account, got %d", len(accounts))
	}
	if got := accounts[0].Edition; got != "ENTERPRISE" {
		t.Errorf("Edition = %q, want %q", got, "ENTERPRISE")
	}
	if got := accounts[0].AccountLocator; got != "AB12345" {
		t.Errorf("AccountLocator = %q, want %q", got, "AB12345")
	}
}
