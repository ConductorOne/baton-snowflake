package connector

import "testing"

func TestSeatsForOrgAccounts(t *testing.T) {
	tests := []struct {
		name         string
		accountCount int
		userCount    int64
		wantSeats    int64
		wantOK       bool
	}{
		{"single account with users", 1, 5, 5, true},
		{"single account no users", 1, 0, 0, false},
		{"multiple accounts", 3, 5, 0, false},
		{"no accounts", 0, 5, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSeats, gotOK := seatsForOrgAccounts(tt.accountCount, tt.userCount)
			if gotSeats != tt.wantSeats || gotOK != tt.wantOK {
				t.Errorf("seatsForOrgAccounts(%d, %d) = (%d, %v), want (%d, %v)",
					tt.accountCount, tt.userCount, gotSeats, gotOK, tt.wantSeats, tt.wantOK)
			}
		})
	}
}
