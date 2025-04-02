![Baton Logo](./docs/images/baton-logo.png)

#

`baton-snowflake` [![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/baton-snowflake.svg)](https://pkg.go.dev/github.com/conductorone/baton-snowflake) ![main ci](https://github.com/conductorone/baton-snowflake/actions/workflows/main.yaml/badge.svg)

`baton-snowflake` is a connector for Baton built using the [Baton SDK](https://github.com/conductorone/baton-sdk). It
works with Snowflake V6 API.

Check out [Baton](https://github.com/conductorone/baton) to learn more about the project in general.

# Prerequisites

This connector uses
[key-pair authentication](https://docs.snowflake.com/en/developer-guide/sql-api/authenticating#using-key-pair-authentication)
to access the Snowflake API. The process of generating the key pair and then assigning those keys to a user is described
in
[the key-pair authentication documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth).

The connector must be passed both the path to the **UNENCRYPTED PRIVATE KEY in
PEM format** or the raw value by . They can be passed as either CLI
flags or as environment variables via the following variable names:

| As Environment Variables | As CLI flags         | Description           |
|--------------------------|----------------------|-----------------------|
| `BATON_PRIVATE_KEY_PATH` | `--private-key-path` | Path to private Key   |
| `BATON_PRIVATE_KEY`      | `--private-key`      | Raw private key value |

# Getting Started

Alongside the key pair, you must specify the Snowflake account URL, account identifier, and user identifier using
either environment variables or CLI flags. The process of obtaining the these values is described in
[the account identifiers documentation](https://docs.snowflake.com/en/user-guide/admin-account-identifier).

## Setup script for snowflake api key pair

To execute the setup script needs

- [snowflake-cli](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index)
- [jq](https://stedolan.github.io/jq/)

#### 1. Check connection on CLI [snowflake-cli](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index)

Configure your config.toml file

Example:

```toml
default_connection_name = "myconnection"

[connections]
[connections.myconnection]
account = "YOUR_ACCOUNT"
user = "YOUR_USER"
password = "YOUR_PASSWORD"
```

Check if you can connect to Snowflake

```
snow --config-file ./config.toml sql -q "SHOW DATABASES"
```

---

#### 2. Execute script

Will generate a key pair and assign it to a Snowflake user. The script will create rsa_key.p8 and rsa_key.pub if they do
not exist then output the public key fingerprint.

```bash
./scripts/setup.sh YOUR_USER
```

---

## Manual Steps to generate a key pair and assign it to a Snowflake user

See on [Docs](https://docs.snowflake.com/en/user-guide/key-pair-auth)

---

### 1. Login to Snowflake using the Snowflake CLI or any other Snowflake client.

---

#### 1.1 CLI

Install [snowflake-cli](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index)

Configure your config.toml file

Check if you can connect to Snowflake

```
snow --config-file ./config.toml sql -q "SHOW DATABASES"
```

---

### 2. Generate an unencrypted

Use the following command

```
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
```

---

### 3. Generate the public key by referencing the private key.

The following command assumes the private key is encrypted and contained in the file named rsa_key.p8.

```
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

---

### 4 Execute an ALTER USER command to assign the public key to a Snowflake user.

User must be ACCOUNTADMIN

```

ALTER USER <SNOWFLAKEUSER> SET RSA_PUBLIC_KEY='MIIBIj...';

```

---

### 5 Retrieve the user’s public key fingerprint:

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

### Sync Secrets

To sync secrets the account needs this role
permission https://docs.snowflake.com/en/sql-reference/sql/show-secrets#access-control-requirements

## brew

```

brew install conductorone/baton/baton conductorone/baton/baton-snowflake

BATON_ACCOUNT_URL=https://abcdsa-abcdsa123.snowflakecomputing.com
BATON_ACCOUNT_IDENTIFIER=abcdsa-abcdsa123
BATON_USER_IDENTIFIER=user1
BATON_PUBLIC_KEY_FINGERPRINT=s98YHSRV+12124142124124124c=
BATON_PRIVATE_KEY_PATH=./my-private-key.pem
baton-snowflake

baton resources

```

## docker

```

docker run --rm
-v $(pwd):/out -e BATON_ACCOUNT_URL=https://abcdsa-abcdsa123.snowflakecomputing.com BATON_ACCOUNT_IDENTIFIER=abcdsa-abcdsa123 BATON_USER_IDENTIFIER=user1 BATON_PUBLIC_KEY_FINGERPRINT=s98YHSRV+12124142124124124c= BATON_PRIVATE_KEY_PATH=./my-private-key.pem ghcr.io/conductorone/baton-snowflake:latest -f "/out/sync.c1z"
docker run --rm -v $(pwd):/out ghcr.io/conductorone/baton:latest -f "/out/sync.c1z" resources

```

## source

```

go install github.com/conductorone/baton/cmd/baton@main
go install github.com/conductorone/baton-snowflake/cmd/baton-snowflake@main

BATON_ACCOUNT_URL=https://abcdsa-abcdsa123.snowflakecomputing.com \
BATON_ACCOUNT_IDENTIFIER=abcdsa-abcdsa123 \
BATON_USER_IDENTIFIER=user1 \
BATON_PUBLIC_KEY_FINGERPRINT=s98YHSRV+12124142124124124c= \
BATON_PRIVATE_KEY_PATH=./my-private-key.pem \

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
capabilities Get connector capabilities
completion Generate the autocompletion script for the specified shell
help Help about any command

Flags:
--account-identifier string required: Account Identifier. ($BATON_ACCOUNT_IDENTIFIER)
--account-url string          required: Account URL. ($BATON_ACCOUNT_URL)
--client-id string The client ID used to authenticate with ConductorOne ($BATON_CLIENT_ID)
--client-secret string        The client secret used to authenticate with ConductorOne ($BATON_CLIENT_SECRET)
-f, --file string The path to the c1z file to sync with ($BATON_FILE) (default "sync.c1z")
-h, --help                        help for baton-snowflake
--log-format string           The output format for logs: json, console ($BATON_LOG_FORMAT) (default "json")
--log-level string The log level: debug, info, warn, error ($BATON_LOG_LEVEL) (default "info")
--private-key string          Private Key (PEM format). ($BATON_PRIVATE_KEY)
--private-key-path string Private Key Path. ($BATON_PRIVATE_KEY_PATH)
-p, --provisioning                This must be set in order for provisioning actions to be enabled ($BATON_PROVISIONING)
--skip-full-sync This must be set to skip a full sync ($BATON_SKIP_FULL_SYNC)
--ticketing                   This must be set to enable ticketing support ($BATON_TICKETING)
--user-identifier string required: User Identifier. ($BATON_USER_IDENTIFIER)
-v, --version version for baton-snowflake

Use "baton-snowflake [command] --help" for more information about a command.

```
