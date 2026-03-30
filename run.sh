#!/bin/bash
# Galera Orchestrator — запуск backend
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# Проверяем venv
if [ ! -d venv ]; then
    echo "[ERROR] venv не найден. Сначала запусти: ./deploy.sh"
    exit 1
fi

# Проверяем конфиг
if [ ! -f config/nodes.yaml ]; then
    echo "[ERROR] config/nodes.yaml не найден. Сначала запусти: ./deploy.sh"
    exit 1
fi

source venv/bin/activate

# Параметры запуска
HOST="${GALERA_HOST:-0.0.0.0}"
PORT="${GALERA_PORT:-8000}"

# Определяем внешний IP для удобного вывода
LOCAL_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "localhost")

echo "=== Galera Orchestrator ==="
echo "UI:      http://${LOCAL_IP}:${PORT}"
echo "Swagger: http://${LOCAL_IP}:${PORT}/docs"
echo "Лог:     logs/galera-events.log"
echo "Остановить: Ctrl+C"
echo "==========================="
echo ""

cd backend
exec uvicorn main:app --host "$HOST" --port "$PORT"
