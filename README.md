![Baton Logo](./docs/images/baton-logo.png)

# `baton-snowflake` [![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/baton-snowflake.svg)](https://pkg.go.dev/github.com/conductorone/baton-snowflake) ![main ci](https://github.com/conductorone/baton-snowflake/actions/workflows/main.yaml/badge.svg)

`baton-snowflake` is a connector for Baton built using the [Baton SDK](https://github.com/conductorone/baton-sdk). It works with Snowflake V6 API.

Check out [Baton](https://github.com/conductorone/baton) to learn more about the project in general.

# Prerequisites

Connector uses [key-pair authentication](https://docs.snowflake.com/en/developer-guide/sql-api/authenticating#using-key-pair-authentication) to access Snowflake API. Generating of key-pair and assigning key-pair to the user is described in [this page](https://docs.snowflake.com/en/user-guide/key-pair-auth). 

To the connector you should pass path to the **NOT ENCRYPTED PRIVATE KEY in PEM format** using either `BATON_PRIVATE_KEY_PATH` or `--private-key-path` flag. Along with private key you have to pass public key fingerprint using either `BATON_PUBLIC_KEY_FINGERPRINT` or `--public-key-fingerprint` flag.

# Getting Started

Along with key-pair, you must specify Snowflake account URL, account identifier and user identigier using either env variable or CLI flags. How to get those value is described in [this page](https://docs.snowflake.com/en/user-guide/admin-account-identifier).

## brew

```
brew install conductorone/baton/baton conductorone/baton/baton-snowflake

BATON_ACCOUNT_URL=https://lz22289.eu-central-1.snowflakecomputing.com BATON_ACCOUNT_IDENTIFIER=YIZK123-CU12345 BATON_USER_IDENTIFIER=user1 BATON_PUBLIC_KEY_FINGERPRINT=s98YHSRV+12124142124124124c= BATON_PRIVATE_KEY_PATH=./my-private-key.pem baton-snowflake

baton resources
```

## docker

```
docker run --rm -v $(pwd):/out -e BATON_ACCOUNT_URL=https://lz22289.eu-central-1.snowflakecomputing.com BATON_ACCOUNT_IDENTIFIER=YIZK123-CU12345 BATON_USER_IDENTIFIER=user1 BATON_PUBLIC_KEY_FINGERPRINT=s98YHSRV+12124142124124124c= BATON_PRIVATE_KEY_PATH=./my-private-key.pem ghcr.io/conductorone/baton-snowflake:latest -f "/out/sync.c1z"
docker run --rm -v $(pwd):/out ghcr.io/conductorone/baton:latest -f "/out/sync.c1z" resources
```

## source

```
go install github.com/conductorone/baton/cmd/baton@main
go install github.com/conductorone/baton-snowflake/cmd/baton-snowflake@main

BATON_ACCOUNT_URL=https://lz22289.eu-central-1.snowflakecomputing.com BATON_ACCOUNT_IDENTIFIER=YIZK123-CU12345 BATON_USER_IDENTIFIER=user1 BATON_PUBLIC_KEY_FINGERPRINT=s98YHSRV+12124142124124124c= BATON_PRIVATE_KEY_PATH=./my-private-key.pem baton-snowflake
baton resources
```

# Data Model

`baton-snowflake` will fetch information about the following Baton resources:

- Users
- Account Roles
- Databases

# Contributing, Support and Issues

We started Baton because we were tired of taking screenshots and manually building spreadsheets. We welcome contributions, and ideas, no matter how small -- our goal is to make identity and permissions sprawl less painful for everyone. If you have questions, problems, or ideas: Please open a Github Issue!

See [CONTRIBUTING.md](https://github.com/ConductorOne/baton/blob/main/CONTRIBUTING.md) for more details.

# `baton-snowflake` Command Line Usage

```
baton-snowflake

Usage:
  baton-snowflake [flags]
  baton-snowflake [command]

Available Commands:
  capabilities       Get connector capabilities
  completion         Generate the autocompletion script for the specified shell
  help               Help about any command

Flags:
      --account-identifier string       Account Identifier
      --account-url string              Account URL
      --client-id string                The client ID used to authenticate with ConductorOne ($BATON_CLIENT_ID)
      --client-secret string            The client secret used to authenticate with ConductorOne ($BATON_CLIENT_SECRET)
  -f, --file string                     The path to the c1z file to sync with ($BATON_FILE) (default "sync.c1z")
  -h, --help                            help for baton-snowflake
      --log-format string               The output format for logs: json, console ($BATON_LOG_FORMAT) (default "json")
      --log-level string                The log level: debug, info, warn, error ($BATON_LOG_LEVEL) (default "info")
      --private-key-path string         Private Key Path
  -p, --provisioning                    This must be set in order for provisioning actions to be enabled. ($BATON_PROVISIONING)
      --public-key-fingerprint string   Public Key Fingerprint
      --user-identifier string          User Identifier
  -v, --version                         version for baton-snowflake
```