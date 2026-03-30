#!/bin/bash
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"
[ ! -d venv ] && { echo "❌ Run ./deploy.sh first"; exit 1; }
source venv/bin/activate
echo "🚀 http://localhost:8000  |  API docs: http://localhost:8000/docs"
cd backend && python main.py
