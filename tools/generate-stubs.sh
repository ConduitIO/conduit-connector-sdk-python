#!/usr/bin/env bash
# Regenerates the vendored gRPC/protobuf stubs in src/conduit/_grpc/ from the
# current conduit-connector-protocol (v2 only), conduit-commons
# (opencdc/config only), and conduit control-plane API (api/v1 + its transitive
# google/protoc_gen_openapiv2 deps) BSR modules. See
# docs/design/20260707-python-connector-sdk.md §1.5 and
# src/conduit/_grpc/__init__.py for why this is scoped with --path rather than
# generating whole modules (v1 connector protocol is deprecated at the source;
# conduit-commons carries unrelated surfaces; a bare `buf generate` against
# buf.build/conduitio/conduit would pull in unrelated proto packages too).
#
# Usage: ./tools/generate-stubs.sh
# Requires: buf (https://buf.build/docs/installation) on PATH.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "==> connector/v2 (SourcePlugin/DestinationPlugin/SpecifierPlugin)"
buf generate buf.build/conduitio/conduit-connector-protocol --path connector/v2

echo "==> api/v1 (PipelineService/ConnectorService/ProcessorService/InformationService)"
buf generate buf.build/conduitio/conduit --path api/v1

echo "==> config/v1 + opencdc/v1 (parameter + record types, from conduit-commons)"
buf generate buf.build/conduitio/conduit-commons --path config/v1 --path opencdc/v1

echo "==> google/api (annotations/field_behavior/http, used as message/method options)"
buf generate buf.build/googleapis/googleapis \
  --path google/api/annotations.proto \
  --path google/api/field_behavior.proto \
  --path google/api/http.proto

echo "==> protoc-gen-openapiv2/options (grpc-gateway OpenAPI annotations)"
buf generate buf.build/grpc-ecosystem/grpc-gateway --path protoc-gen-openapiv2/options

echo "==> done. Review the diff in src/conduit/_grpc/ before committing."
