package main

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/spf13/cobra"
)

// config defines the external configuration required for the connector to run.
type config struct {
	cli.BaseConfig `mapstructure:",squash"` // Puts the base config options in the same place as the connector options

	AccountUrl           string `mapstructure:"account-url"`
	AccountIdentifier    string `mapstructure:"account-identifier"`
	UserIdentifier       string `mapstructure:"user-identifier"`
	PublicKeyFingerPrint string `mapstructure:"public-key-fingerprint"`
	PrivateKeyPath       string `mapstructure:"private-key-path"`
}

// validateConfig is run after the configuration is loaded, and should return an error if it isn't valid.
func validateConfig(ctx context.Context, cfg *config) error {
	if cfg.AccountUrl == "" {
		return fmt.Errorf("account-url is required")
	}
	if cfg.AccountIdentifier == "" {
		return fmt.Errorf("account-identifier is required")
	}
	if cfg.UserIdentifier == "" {
		return fmt.Errorf("user-identifier is required")
	}
	if cfg.PublicKeyFingerPrint == "" {
		return fmt.Errorf("public-key-fingerprint is required")
	}
	if cfg.PrivateKeyPath == "" {
		return fmt.Errorf("private-key-path is required")
	}

	return nil
}

func cmdFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("account-url", "", "Account URL")
	cmd.PersistentFlags().String("account-identifier", "", "Account Identifier")
	cmd.PersistentFlags().String("user-identifier", "", "User Identifier")
	cmd.PersistentFlags().String("public-key-fingerprint", "", "Public Key Fingerprint")
	cmd.PersistentFlags().String("private-key-path", "", "Private Key Path")
}
