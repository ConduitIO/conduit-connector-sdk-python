#!/usr/bin/env bash
# Regenerates the vendored gRPC/protobuf stubs in src/conduit/_grpc/ from the
# current ConduitIO/conduit control-plane API (proto/api/v1/api.proto) plus its
# transitive dependencies (conduit-commons opencdc/config, googleapis
# annotations, grpc-gateway openapiv2 options). See
# docs/design/20260724-embed-grpc-client-libraries.md and
# src/conduit/_grpc/__init__.py for why each of these is generated (api.proto
# imports all of them; the Python protobuf runtime needs a generated module
# for every transitively imported .proto file to resolve descriptors, even
# for types this client never constructs directly).
#
# Usage: ./tools/generate-stubs.sh
# Requires: buf (https://buf.build/docs/installation) on PATH.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

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
