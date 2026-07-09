package snowflake

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStatementsApiRequestBodyRole(t *testing.T) {
	withRole, err := json.Marshal(StatementsApiRequestBody{Statement: "SHOW ORGANIZATION ACCOUNTS;", Role: GlobalOrgAdminRole})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(withRole), `"role":"GLOBALORGADMIN"`) {
		t.Errorf("expected role in request body, got %s", withRole)
	}

	noRole, err := json.Marshal(StatementsApiRequestBody{Statement: "SHOW USERS;"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(noRole), "role") {
		t.Errorf("expected role omitted when empty, got %s", noRole)
	}
}
