#!/usr/bin/env bash
# Start the Ameoba HTTP API (FastAPI) and serve the debug web UI in-process.
#
# The E2E harness lives at /debug/e2e; OpenAPI at /docs.
#
# Usage:
#   ./scripts/local_server.sh
#   ./scripts/local_server.sh --reload
#   HOST=0.0.0.0 PORT=8000 ./scripts/local_server.sh
#
# Env:
#   HOST           bind address (default: 127.0.0.1)
#   PORT           listen port (default: 8000)
#   OPEN_BROWSER   1=open /debug/e2e in a browser (default: 1 on macOS, 0 elsewhere)
#   PYTHON         python executable (default: .venv/bin/python if present, else python3)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
export PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"

HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8000}"

if [[ -n "${PYTHON:-}" ]]; then
  :
elif [[ -x "${REPO_ROOT}/.venv/bin/python" ]]; then
  PYTHON="${REPO_ROOT}/.venv/bin/python"
else
  PYTHON="python3"
fi

if ! "$PYTHON" -c "import uvicorn" 2>/dev/null; then
  echo "ERROR: uvicorn is not installed for ${PYTHON}." >&2
  echo "       Run: pip install -e .   (or pip install uvicorn)" >&2
  exit 1
fi

case "$(uname -s 2>/dev/null || true)" in
  Darwin) _open_browser_default=1 ;;
  *) _open_browser_default=0 ;;
esac
OPEN_BROWSER="${OPEN_BROWSER:-$_open_browser_default}"

base_url="http://${HOST}:${PORT}"
if [[ "${HOST}" == "0.0.0.0" ]] || [[ "${HOST}" == "::" ]]; then
  base_url="http://127.0.0.1:${PORT}"
fi

echo "Ameoba local server"
echo "  Base URL:    ${base_url}/"
echo "  API docs:    ${base_url}/docs"
echo "  Investor demo: ${base_url}/debug/e2e  (same UI: /debug/demo)"
echo ""

if [[ "${OPEN_BROWSER}" == "1" ]]; then
  if command -v open >/dev/null 2>&1; then
    ( sleep 1.5 && open "${base_url}/debug/e2e" ) &
  elif command -v xdg-open >/dev/null 2>&1; then
    ( sleep 1.5 && xdg-open "${base_url}/debug/e2e" ) &
  fi
fi

exec "$PYTHON" -m uvicorn ameoba.api.http.app:create_app \
  --factory \
  --host "${HOST}" \
  --port "${PORT}" \
  "$@"
