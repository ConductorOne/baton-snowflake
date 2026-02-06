package main

import (
	"github.com/conductorone/baton-sdk/pkg/config"
	cfg "github.com/conductorone/baton-snowflake/pkg/config"
)

func main() {
	config.Generate("snowflake", cfg.Configuration)
}
