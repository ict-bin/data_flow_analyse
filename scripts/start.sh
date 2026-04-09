#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] && set -a && source .env && set +a
mkdir -p output sessions workspace

command -v pi &>/dev/null || { echo "❌ pi 未找到。npm install -g @mariozechner/pi-coding-agent"; exit 1; }
python3 -c "import fastapi" 2>/dev/null || pip install -r requirements.txt

case "${1:---server}" in
  --cli)  shift; exec python3 cli.py "$@" ;;
  --dev)  DEV=1 exec python3 main.py ;;
  *)      exec python3 main.py ;;
esac
