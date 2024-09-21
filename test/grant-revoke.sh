#!/usr/bin/env bash

set -exo pipefail

 # CI test for use with CI AWS account
if [ -z "$COMMAND" ]; then
  echo "COMMAND not set. using baton-salesforce"
  COMMAND=baton-aws
fi
if [ -z "$BATON" ]; then
  echo "BATON not set. using baton"
  BATON=baton
fi

# Error on unbound variables now that we've set BATON & COMMAND
set -u

# Sync
$COMMAND

# Grant entitlement
$COMMAND --grant-entitlement="$BATON_ENTITLEMENT" --grant-principal="$BATON_PRINCIPAL" --grant-principal-type="$BATON_PRINCIPAL_TYPE"

# Check for grant before revoking
$COMMAND
$BATON grants --entitlement="$BATON_ENTITLEMENT" --output-format=json | jq --exit-status ".grants[] | select( .principal.id.resource == \"$BATON_PRINCIPAL\" )"

# Grant already-granted entitlement
$COMMAND --grant-entitlement="$BATON_ENTITLEMENT" --grant-principal="$BATON_PRINCIPAL" --grant-principal-type="$BATON_PRINCIPAL_TYPE"

# Get grant ID
BATON_GRANT=$($BATON grants --entitlement="$BATON_ENTITLEMENT" --output-format=json | jq --raw-output --exit-status ".grants[] | select( .principal.id.resource == \"$BATON_PRINCIPAL\" ).grant.id")

# Revoke grant
$COMMAND --revoke-grant="$BATON_GRANT"

# Revoke already-revoked grant
$COMMAND --revoke-grant="$BATON_GRANT"

# Check grant was revoked
$COMMAND
$BATON grants --entitlement="$BATON_ENTITLEMENT" --output-format=json | jq --exit-status "if .grants then [ .grants[] | select( .principal.id.resource == \"$BATON_PRINCIPAL\" ) ] | length == 0 else . end"

# Re-grant entitlement
$COMMAND --grant-entitlement="$BATON_ENTITLEMENT" --grant-principal="$BATON_PRINCIPAL" --grant-principal-type="$BATON_PRINCIPAL_TYPE"

# Check grant was re-granted
$COMMAND
$BATON grants --entitlement="$BATON_ENTITLEMENT" --output-format=json | jq --exit-status ".grants[] | select( .principal.id.resource == \"$BATON_PRINCIPAL\" )"
