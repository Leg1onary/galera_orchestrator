#!/bin/bash
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"
echo "🚀 Galera Orchestrator — setup"
command -v python3 >/dev/null || { echo "❌ python3 not found"; exit 1; }
[ ! -d venv ] && python3 -m venv venv
source venv/bin/activate
pip install -q --upgrade pip
pip install -q -r backend/requirements.txt
echo "✅ Done. Edit config/nodes.yaml then run: ./run.sh"
