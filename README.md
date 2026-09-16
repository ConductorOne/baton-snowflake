![Baton Logo](./docs/images/baton-logo.png)

#

`baton-snowflake` [![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/baton-snowflake.svg)](https://pkg.go.dev/github.com/conductorone/baton-snowflake) ![ci](https://github.com/conductorone/baton-snowflake/actions/workflows/ci.yaml/badge.svg)

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

| As Environment Variables    | As CLI flags              | Description                                      |
|-----------------------------|---------------------------|--------------------------------------------------|
| `BATON_PRIVATE_KEY_PATH`    | `--private-key-path`      | Path to private key                              |
| `BATON_PRIVATE_KEY`         | `--private-key`           | Raw private key value                            |
| `BATON_EXCLUDED_DATABASES`  | `--excluded-databases`    | Database names to skip during sync (repeatable)  |

# Getting Started

Alongside the key pair, you must specify the Snowflake account URL, account identifier, and user identifier using
either environment variables or CLI flags. The process of obtaining the these values is described in
[the account identifiers documentation](https://docs.snowflake.com/en/user-guide/admin-account-identifier).

Connect to Tool in UI under Account Icon on lower right can give you the account identifier.

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

### License data

The connector can sync a `license` resource that reports the Snowflake edition
(Standard, Enterprise, or Business Critical) and, for single-account
organizations, the account's user count as consumed seats. This resource type is
opt-in and requires connecting with an account that can read organization-level
details (`GLOBALORGADMIN` on the organization account). When that access is not
available, license sync is skipped and the rest of the sync is unaffected.

### Excluding Databases from Sync

Use `--excluded-databases` (or `BATON_EXCLUDED_DATABASES`) to skip one or more databases entirely. Excluded databases and all of their tables are omitted from every sync. Matching is case-insensitive.

**CLI flag** (repeatable):
```bash
baton-snowflake \
  --excluded-databases "MY_INTERNAL_DB" \
  --excluded-databases "ANOTHER_DB"
```

**Environment variable** (comma-separated):
```bash
BATON_EXCLUDED_DATABASES="MY_INTERNAL_DB,ANOTHER_DB" baton-snowflake
```

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
- Integrations

## Users

`baton-snowflake` syncs users via `SHOW USERS`, or from `SNOWFLAKE.ACCOUNT_USAGE.USERS` under
`--discovery-mode=account_usage`. Users with a Snowflake `TYPE` of `SERVICE`, `SERVICE_AGENT`, or
`LEGACY_SERVICE` are marked as non-human identities (app registrations), since they authenticate
with a self-custodied standing credential the account holds and rotates. `PERSON` and untyped users
carry no non-human-identity tag.

A user whose `SHOW USERS` row came back with its columns suppressed (see
[Privilege model](#privilege-model)) has no readable `TYPE`, so its account type is reported as
unspecified rather than defaulted to human, and the connector logs a warning naming the user.

## Integrations

`baton-snowflake` syncs account-level integrations via `SHOW INTEGRATIONS` and marks them as
non-human identities by integration type: EXTERNAL OAUTH security integrations are tagged as app
registrations, while STORAGE and API integrations are tagged as assumable roles (they assume a
cloud IAM role).

`SHOW INTEGRATIONS` returns only the integrations the connector's current role has been granted at
least one privilege on. A role holding `MANAGE GRANTS` (e.g. `ACCOUNTADMIN` or `SECURITYADMIN`)
sees every integration in the account; a more restricted role simply sees a smaller set. No
integrations are returned (and the sync is unaffected) if the role can see none. There is no
`ACCOUNT_USAGE` view for integrations, so this resource type stays on the `SHOW` path in both
discovery modes.

# Privilege model

`baton-snowflake` does **not** require the account-level `MANAGE GRANTS` privilege. `GRANT MANAGE
GRANTS ON ACCOUNT` is an account-wide global privilege — Snowflake defines it as the ability to
grant or revoke privileges on any object in the account as if the granting role owned it, and
documents that a role holding it can grant further privileges to itself. It is not scoped to a
warehouse or a namespace.

Pick one of two discovery paths with `--discovery-mode`.

## `--discovery-mode=account_usage` (least privilege)

Inventory is read from the `SNOWFLAKE.ACCOUNT_USAGE` schema, which is account-wide by construction
and gated by two database roles rather than by per-object grants:

```sql
GRANT DATABASE ROLE SNOWFLAKE.SECURITY_VIEWER TO ROLE <connector role>;  -- users, roles, role grants
GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER   TO ROLE <connector role>;  -- databases, schemas, tables, object grants
GRANT USAGE ON WAREHOUSE <warehouse>          TO ROLE <connector role>;
```

Trade-offs, both documented by Snowflake: `ACCOUNT_USAGE` views lag the live account by roughly 90
minutes to 3 hours depending on the view, and the reads are `SELECT` statements so they need a
warehouse that can resume. `SHOW` commands are metadata-only and need neither.

## `--discovery-mode=show` (default, live data)

Every `SHOW` command returns only the objects the session role holds at least one privilege on, so
visibility is granted object by object:

| Statement | Minimum privilege |
| --- | --- |
| `SHOW USERS` | `OWNERSHIP` on each user. Without it every column but `name` comes back NULL. There is no read-only alternative in Snowflake's access control model |
| `SHOW ROLES`, `SHOW GRANTS OF ROLE` | `OWNERSHIP` of each role |
| `SHOW DATABASES` / `SHOW SCHEMAS` / `SHOW TABLES` | `USAGE` on the database and schema, plus at least one privilege on the object. `REFERENCES` is sufficient and is metadata-only — it grants visibility of an object's structure but never its data |
| `SHOW GRANTS ON TABLE` / `ON VIEW` | Same as above |
| `SHOW INTEGRATIONS` | `USAGE` on each integration |
| `DESCRIBE USER` (RSA public key timestamps, `--sync-secrets`) | `OWNERSHIP` on the user. There is no `MONITOR` privilege on a user object |
| `SHOW USER PROGRAMMATIC ACCESS TOKENS` | `MODIFY PROGRAMMATIC AUTHENTICATION METHODS` or `OWNERSHIP`, per user |

Under this mode, users the connector's role does not own sync with a blank login, email, and
`TYPE`. The connector detects this per user, logs a warning naming the user, and reports the
account type as unspecified rather than guessing — so a service account with a suppressed `TYPE`
is not silently classified as a person. If no sampled user's attributes are readable at all,
startup fails with a named, actionable error instead of syncing blank users.

Snowflake also supports granting `MANAGE GRANTS` on a single database or schema rather than on the
account. That is a customer-side configuration choice and needs no connector setting; see
Snowflake's [container-level MANAGE GRANTS](https://docs.snowflake.com/en/user-guide/container-manage-grants-using)
documentation.

## Narrowing the scope

`--sync-object-resources=false` turns off the database, schema, and table resource types as a
group, so a users-roles-and-grants-only sync needs none of the object-level privileges above. This
is different from `--excluded-databases`, which is a connector-side filter applied after the
privileges have already been granted and which requires every database you want skipped to be
named. Snowflake secrets are database-scoped, so the `secret` resource type is not synced while
object resources are off; RSA public keys are user-scoped and are unaffected.

## Provisioning privileges

User lifecycle (create, delete, enable, disable) and programmatic access token operations run as a
dedicated write role, configurable with `--write-role` and defaulting to Snowflake's `USERADMIN`
system role. To avoid granting `USERADMIN` to the service account:

```sql
GRANT CREATE USER ON ACCOUNT                                  TO ROLE <write role>;
GRANT MODIFY ON USER <user>                                    TO ROLE <write role>;
GRANT MODIFY PROGRAMMATIC AUTHENTICATION METHODS ON USER <user> TO ROLE <write role>;
```

Account role grant and revoke run as the session's default role, not the write role, and require
`OWNERSHIP` of the role being granted.

License sync reads `SHOW ORGANIZATION ACCOUNTS` as a dedicated organization role, configurable with
`--organization-role` and defaulting to `GLOBALORGADMIN`. It also counts users via
`SELECT ... FROM SNOWFLAKE.ACCOUNT_USAGE.USERS`, so it needs `SNOWFLAKE.SECURITY_VIEWER` (or an
equivalent `SELECT` grant) and a running warehouse in both discovery modes. License sync is opt-in;
none of this is needed unless you enable it.

`--write-role`, `--organization-role`, and `--discovery-mode` all default to exactly what the
connector did before they existed, and `--sync-object-resources` defaults to true, so an existing
configuration's behavior is unchanged until one of them is set.

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
--account-identifier string   required: Account Identifier. ($BATON_ACCOUNT_IDENTIFIER)
--account-url string          required: Account URL. ($BATON_ACCOUNT_URL)
--client-id string            The client ID used to authenticate with ConductorOne ($BATON_CLIENT_ID)
--client-secret string        The client secret used to authenticate with ConductorOne ($BATON_CLIENT_SECRET)
--discovery-mode string       How the connector discovers inventory: "show" or "account_usage". ($BATON_DISCOVERY_MODE) (default "show")
--excluded-databases strings  Database names to exclude from sync, case-insensitive. Can be specified multiple times. ($BATON_EXCLUDED_DATABASES)
-f, --file string             The path to the c1z file to sync with ($BATON_FILE) (default "sync.c1z")
-h, --help                    help for baton-snowflake
--issue-credentials           Enable issuing Snowflake programmatic access tokens for existing users. ($BATON_ISSUE_CREDENTIALS)
--log-format string           The output format for logs: json, console ($BATON_LOG_FORMAT) (default "json")
--log-level string            The log level: debug, info, warn, error ($BATON_LOG_LEVEL) (default "info")
--organization-role string    Snowflake role used for organization-scoped reads. ($BATON_ORGANIZATION_ROLE) (default "GLOBALORGADMIN")
--private-key string          Private Key (PEM format). ($BATON_PRIVATE_KEY)
--private-key-path string     Private Key Path. ($BATON_PRIVATE_KEY_PATH)
-p, --provisioning            This must be set in order for provisioning actions to be enabled ($BATON_PROVISIONING)
--skip-full-sync              This must be set to skip a full sync ($BATON_SKIP_FULL_SYNC)
--sync-object-resources       Sync database, schema, and table resources and their grants. ($BATON_SYNC_OBJECT_RESOURCES) (default true)
--sync-secrets                Enable synchronization of Snowflake secrets. ($BATON_SYNC_SECRETS)
--ticketing                   This must be set to enable ticketing support ($BATON_TICKETING)
--user-identifier string      required: User Identifier. ($BATON_USER_IDENTIFIER)
-v, --version                 version for baton-snowflake
--write-role string           Snowflake role used for user lifecycle and token operations. ($BATON_WRITE_ROLE) (default "USERADMIN")

Use "baton-snowflake [command] --help" for more information about a command.

```
