#!/usr/bin/env bash
set -euo pipefail

echo "================================================================"
echo "FabricFs Release Check"
echo "================================================================"

echo ""
echo ">>> [1/4] Running workspace hygiene checks..."
just check

echo ""
echo ">>> [2/4] Running coverage gate..."
just ci

echo ""
echo ">>> [3/4] Running data-plane smoke test..."
if command -v nats-server >/dev/null 2>&1; then
  ./smoke.sh
else
  echo "WARNING: nats-server not found. Skipping ./smoke.sh."
fi

echo ""
echo ">>> [4/4] Running session smoke test..."
if command -v nats-server >/dev/null 2>&1; then
  ./smoke-sessions.sh
else
  echo "WARNING: nats-server not found. Skipping ./smoke-sessions.sh."
fi

echo ""
echo "================================================================"
echo "Release Check Complete"
echo "================================================================"
