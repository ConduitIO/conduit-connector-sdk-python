#!/usr/bin/env bash
# Regenerates the vendored gRPC/protobuf stubs in src/conduit/_grpc/ from the
# current conduit-connector-protocol (v2 only) and conduit-commons (opencdc/config
# only) BSR modules. See docs/design/20260707-python-connector-sdk.md §1.5 and
# src/conduit/_grpc/__init__.py for why this is scoped with --path rather than
# generating whole modules (v1 connector protocol is deprecated at the source;
# conduit-commons carries unrelated surfaces like schema registry/metadata
# constants this SDK doesn't touch in v0.19 scope).
#
# Usage: ./tools/generate-stubs.sh
# Requires: buf (https://buf.build/docs/installation) on PATH.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "==> connector/v2 (SourcePlugin/DestinationPlugin/SpecifierPlugin)"
buf generate buf.build/conduitio/conduit-connector-protocol --path connector/v2

echo "==> opencdc/v1 + config/v1 (record + parameter types, from conduit-commons)"
buf generate buf.build/conduitio/conduit-commons --path opencdc/v1 --path config/v1

echo "==> done. Review the diff in src/conduit/_grpc/ before committing."
