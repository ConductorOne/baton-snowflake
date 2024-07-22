![Baton Logo](./docs/images/baton-logo.png)

# `baton-snowflake` [![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/baton-snowflake.svg)](https://pkg.go.dev/github.com/conductorone/baton-snowflake) ![main ci](https://github.com/conductorone/baton-snowflake/actions/workflows/main.yaml/badge.svg)

`baton-snowflake` is a connector for Baton built using the [Baton SDK](https://github.com/conductorone/baton-sdk). It works with Snowflake V6 API.

Check out [Baton](https://github.com/conductorone/baton) to learn more about the project in general.

# Prerequisites

This connector uses 
[key-pair authentication](https://docs.snowflake.com/en/developer-guide/sql-api/authenticating#using-key-pair-authentication) 
to access the Snowflake API. The process of generating the key pair and then assigning those keys to a user is described in 
[the key-pair authentication documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth). 

The connector must be passed both the path to the **UNENCRYPTED PRIVATE KEY in 
PEM format** and the public key fingerprint. They can be passed as either CLI 
flags or as environment variables via the following variable names:

| As Environment Variables       | As CLI flags               |
|--------------------------------|----------------------------|
| `BATON_PRIVATE_KEY_PATH`       | `--private-key-path`       |
| `BATON_PUBLIC_KEY_FINGERPRINT` | `--public-key-fingerprint` |

# Getting Started

Alongside the key pair, you must specify the Snowflake account URL, account identifier, and user identifier using 
either environment variables or CLI flags. The process of obtaining the these values is described in 
[the account identifiers documentation](https://docs.snowflake.com/en/user-guide/admin-account-identifier).

To generate an unencrypted version, use the following command:
```
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
```

From the command line, generate the public key by referencing the private key. 
The following command assumes the private key is encrypted and contained in the file named rsa_key.p8.
```
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Execute an ALTER USER command to assign the public key to a Snowflake user.
user must be ACCOUNTADMIN
```
ALTER USER <SNOWFLAKEUSER> SET RSA_PUBLIC_KEY='MIIBIj...';
```

Execute the following command to retrieve the user’s public key fingerprint:
```
DESC USER <SNOWFLAKEUSER>;
SELECT SUBSTR((SELECT "value" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE "property" = 'RSA_PUBLIC_KEY_FP'), LEN('SHA256:') + 1);
```

Run the following command on the command line: writing RSA key
```
openssl rsa -pubin -in rsa_key.pub -outform DER | openssl dgst -sha256 -binary | openssl enc -base64
```

Compare both outputs. If both outputs match, the user correctly configured their public key.

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

We started Baton because we were tired of taking screenshots and manually building spreadsheets. We welcome 
contributions, and ideas, no matter how small -- our goal is to make identity and permissions sprawl less painful for 
everyone. If you have questions, problems, or ideas: Please open a GitHub Issue!

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
