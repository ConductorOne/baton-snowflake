#!/usr/bin/env bash

USER_NAME=$1

if [ -z "$USER_NAME" ]; then
    echo "Please provide a username as first argument"
    exit 1
fi

# Check if snow is installed
if [ -z "$(which snow)" ]; then
    echo "Snow is not installed. Please install it first. check https://docs.snowflake.com/en/developer-guide/snowflake-cli/index.html"
    exit 1
fi

# Check if jq is installed
if [ -z "$(which jq)" ]; then
    echo "jq is not installed. Please install it first. check https://github.com/jqlang/jq"
    exit 1
fi

# Check if config.toml exists
if [ ! -f "config.toml" ]; then
    echo "config.toml file not found. Please create one. Check https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-cli"
    exit 1
fi

# Generate an unencrypted

# if file does not exist generate a new one

if [ ! -f "rsa_key.p8" ]; then
    echo "Generating a new unencrypted private key"
    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
fi

if [ ! -f "rsa_key.pub" ]; then
    echo "Generating a new public key"
    openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
fi


# List databases
snow --config-file ./config.toml sql -q "SHOW DATABASES"

# get public Key value without the header and footer

echo "Setting the public key for the user $USER_NAME"

PUBLIC_KEY=$(< rsa_key.pub sed '1d;$d' | tr -d '\n')
snow --config-file ./config.toml sql -q "ALTER USER $USER_NAME SET RSA_PUBLIC_KEY='$PUBLIC_KEY';"

echo "Checking if the public key is set correctly"

echo "Fetching the public key fingerprint from Snowflake"
SNOW_FLAKE_FINGERPRINT=$(snow --config-file ./config.toml sql -q "DESC USER $USER_NAME; SELECT SUBSTR((SELECT \"value\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))WHERE \"property\" = 'RSA_PUBLIC_KEY_FP'), LEN('SHA256:') + 1);" --format json | jq -r '.[1][0] | to_entries[0].value')

echo "Generating the local public key fingerprint"
LOCAL_FINGERPRINT=$(openssl rsa -pubin -in rsa_key.pub -outform DER | openssl dgst -sha256 -binary | openssl enc -base64)

if [ "$SNOW_FLAKE_FINGERPRINT" == "$LOCAL_FINGERPRINT" ]; then
    echo "Fingerprints match"
    exit 0
else
    echo "Fingerprints do not match"
    exit 1
fi