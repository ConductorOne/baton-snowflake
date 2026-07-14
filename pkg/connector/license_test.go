package connector

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

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

func TestIsOrgAccountsUnavailable(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		err        error
		want       bool
	}{
		{"no error", http.StatusOK, nil, false},
		{"422 status code", http.StatusUnprocessableEntity, errors.New("unprocessable entity"), true},
		{"422 in error message", 0, errors.New("rpc error: code = Unknown desc = 422 Unprocessable Entity"), true},
		{"500 propagates", http.StatusInternalServerError, errors.New("500 Internal Server Error"), false},
		{"context canceled propagates", 0, context.Canceled, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isOrgAccountsUnavailable(tt.statusCode, tt.err); got != tt.want {
				t.Errorf("isOrgAccountsUnavailable(%d, %v) = %v, want %v", tt.statusCode, tt.err, got, tt.want)
			}
		})
	}
}
