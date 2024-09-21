package config

import (
	"testing"

	"github.com/conductorone/baton-sdk/pkg/test"
	"github.com/conductorone/baton-sdk/pkg/ustrings"
)

func TestConfigSchema(t *testing.T) {
	test.ExerciseTestCasesFromExpressions(
		t,
		ConfigurationSchema,
		nil,
		ustrings.ParseFlags,
		[]test.TestCaseFromExpression{
			{
				"",
				false,
				"empty config",
			},
			{
				"--account-url 1 --account-identifier 1 --user-identifier --public-key-fingerprint 1",
				false,
				"missing private key",
			},
			{
				"--account-url 1 --account-identifier 1 --user-identifier --public-key-fingerprint 1 --private-key-path 1 --private-key 1",
				false,
				"both private key types",
			},
			{
				"--account-url 1 --account-identifier 1 --user-identifier --public-key-fingerprint 1 --private-key-path 1",
				true,
				"private key path",
			},
			{
				"--account-url 1 --account-identifier 1 --user-identifier --public-key-fingerprint 1 --private-key 1",
				true,
				"private key",
			},
		},
	)
}
