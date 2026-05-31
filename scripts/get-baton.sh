#!/usr/bin/env bash

set -euxo pipefail

# conductorone/baton is archived and its releases/latest is frozen at v0.4.5,
# which cannot resolve the NonHumanIdentityTrait proto this connector now emits.
# Pull the baton CLI from baton-sdk instead, which ships the NHI-aware protos.
BATON_VERSION=v0.11.0

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)
if [ "${ARCH}" = "x86_64" ]; then
  ARCH="amd64"
fi

FILENAME="baton-${BATON_VERSION}-${OS}-${ARCH}.tar.gz"

curl -fsSL -O "https://github.com/conductorone/baton-sdk/releases/download/${BATON_VERSION}/${FILENAME}"
tar xzf "${FILENAME}"
