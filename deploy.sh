#!/bin/bash
# Galera Orchestrator — установка зависимостей и первоначальная настройка
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "=== Galera Orchestrator — deploy ==="

# 1. Python3
if ! command -v python3 &>/dev/null; then
    echo "[ERROR] python3 не найден. Установите: sudo apt install python3 python3-venv -y"
    exit 1
fi
echo "[OK] Python: $(python3 --version)"

# 2. venv
if [ ! -d venv ]; then
    echo "[...] Создаём виртуальное окружение..."
    python3 -m venv venv
fi
source venv/bin/activate

# 3. Зависимости
echo "[...] Устанавливаем зависимости..."
pip install -q --upgrade pip
pip install -q -r backend/requirements.txt
echo "[OK] Зависимости установлены"

# 4. Конфиг
if [ ! -f config/nodes.yaml ]; then
    echo "[...] Создаём config/nodes.yaml из шаблона..."
    cp config/nodes.example.yaml config/nodes.yaml
    echo "[!] Отредактируй config/nodes.yaml перед запуском:"
    echo "      - Укажи IP нод (host:)"
    echo "      - Укажи путь к SSH-ключу (ssh_key:)"
    echo "      - Укажи пароль monitor_user (db.password:)"
else
    echo "[OK] config/nodes.yaml уже существует — не перезаписываем"
fi

echo ""
echo "=== Готово ==="
echo "Следующий шаг: отредактируй config/nodes.yaml"
echo "Затем запускай:  ./run.sh"
echo ""
echo "Или как systemd-сервис — см. README.md"
