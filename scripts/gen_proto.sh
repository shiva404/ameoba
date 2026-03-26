#!/usr/bin/env bash
# Generate Python gRPC stubs from .proto files.
#
# Prerequisites:
#   pip install grpcio grpcio-tools
#
# Usage:
#   bash scripts/gen_proto.sh
#
# Output:
#   src/ameoba/api/grpc/pb/  — generated *_pb2.py and *_pb2_grpc.py files

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
PROTO_ROOT="$REPO_ROOT/proto"
OUT_DIR="$REPO_ROOT/src/ameoba/api/grpc/pb"

# Verify grpcio-tools is available
if ! python -c "import grpc_tools.protoc" 2>/dev/null; then
    echo "ERROR: grpcio-tools is not installed." >&2
    echo "       Run: pip install grpcio grpcio-tools" >&2
    exit 1
fi

mkdir -p "$OUT_DIR"

# Write a package marker so Python can import the generated stubs
touch "$OUT_DIR/__init__.py"

echo "Generating stubs from $PROTO_ROOT → $OUT_DIR"

# Find all .proto files and compile them
find "$PROTO_ROOT" -name "*.proto" | while read -r proto_file; do
    echo "  Compiling: $proto_file"
    python -m grpc_tools.protoc \
        --proto_path="$PROTO_ROOT" \
        --python_out="$OUT_DIR" \
        --grpc_python_out="$OUT_DIR" \
        "$proto_file"
done

# Fix relative imports in generated files (grpc_tools generates absolute imports)
# This sed replaces `import ameoba.v1.X_pb2` with `from . import X_pb2`
if [[ "$OSTYPE" == "darwin"* ]]; then
    SED="sed -i ''"
else
    SED="sed -i"
fi

find "$OUT_DIR" -name "*_pb2_grpc.py" | while read -r grpc_file; do
    $SED 's/^import \(.*\)_pb2 as/from . import \1_pb2 as/g' "$grpc_file"
done

echo ""
echo "Done. Generated files:"
find "$OUT_DIR" -name "*.py" | sort | sed "s|$REPO_ROOT/||"
echo ""
echo "Next step: replace placeholder servicer registration in"
echo "  src/ameoba/api/grpc/server.py"
echo "with:"
echo "  from ameoba.api.grpc.pb.ingest_pb2_grpc import add_IngestServicerToServer"
echo "  from ameoba.api.grpc.pb.query_pb2_grpc  import add_QueryServicerToServer"
echo "  from ameoba.api.grpc.pb.audit_pb2_grpc  import add_AuditServicerToServer"
echo "  add_IngestServicerToServer(IngestServicer(kernel), server)"
echo "  add_QueryServicerToServer(QueryServicer(kernel), server)"
echo "  add_AuditServicerToServer(AuditServicer(kernel), server)"
