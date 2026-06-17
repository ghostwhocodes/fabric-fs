#!/usr/bin/env bash
set -euo pipefail

just check \
  && just ci \
  && ./smoke.sh \
  && ./smoke-sessions.sh
