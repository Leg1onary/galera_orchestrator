#!/bin/bash
# Galera Orchestrator — установка как systemd-сервиса
# Использование: sudo ./install-service.sh [--uninstall]

set -e

SERVICE_NAME="galera-orchestrator"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Цвета ──────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[OK]${NC}    $*"; }
info() { echo -e "${CYAN}[...]${NC}   $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

echo ""
echo -e "${CYAN}=== Galera Orchestrator — установка сервиса ===${NC}"
echo ""

# ── Проверки ───────────────────────────────────────────────────
[[ $EUID -ne 0 ]] && err "Запусти с sudo: sudo ./install-service.sh"
command -v systemctl &>/dev/null || err "systemd не найден — этот скрипт только для Linux с systemd"

# ── Удаление сервиса ──────────────────────────────────────────
if [[ "$1" == "--uninstall" ]]; then
    info "Останавливаем и удаляем сервис ${SERVICE_NAME}..."
    systemctl stop  "${SERVICE_NAME}" 2>/dev/null && ok "Сервис остановлен"   || warn "Сервис уже был остановлен"
    systemctl disable "${SERVICE_NAME}" 2>/dev/null && ok "Сервис отключён"   || warn "Сервис уже был отключён"
    rm -f "$SERVICE_FILE"
    systemctl daemon-reload
    ok "Файл ${SERVICE_FILE} удалён"
    echo ""
    echo -e "${GREEN}Сервис ${SERVICE_NAME} успешно удалён.${NC}"
    exit 0
fi

# ── Проверяем что venv и конфиг есть ──────────────────────────
[[ -d "${DIR}/venv" ]]              || err "venv не найден. Сначала запусти: ./deploy.sh"
[[ -f "${DIR}/config/nodes.yaml" ]] || err "config/nodes.yaml не найден. Сначала запусти: ./deploy.sh"
[[ -f "${DIR}/backend/main.py" ]]   || err "backend/main.py не найден. Проект повреждён?"

UVICORN="${DIR}/venv/bin/uvicorn"
[[ -f "$UVICORN" ]] || err "uvicorn не найден в venv. Переустанови зависимости: ./deploy.sh"

# ── Параметры сервиса ─────────────────────────────────────────
HOST="${GALERA_HOST:-0.0.0.0}"
PORT="${GALERA_PORT:-8000}"

# Определяем пользователя (тот кто вызвал sudo, или root)
RUN_USER="${SUDO_USER:-root}"
RUN_GROUP=$(id -gn "$RUN_USER" 2>/dev/null || echo "root")

info "Директория проекта : ${DIR}"
info "Запуск от имени    : ${RUN_USER}:${RUN_GROUP}"
info "Адрес              : ${HOST}:${PORT}"
info "Файл сервиса       : ${SERVICE_FILE}"
echo ""

# ── Генерируем .service файл ──────────────────────────────────
cat > "$SERVICE_FILE" << UNIT
[Unit]
Description=Galera Orchestrator UI — MariaDB/Galera Cluster Monitor
Documentation=https://github.com/Leg1onary/galera_orchestrator
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
Group=${RUN_GROUP}
WorkingDirectory=${DIR}/backend
ExecStart=${UVICORN} main:app --host ${HOST} --port ${PORT}
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

# Безопасность
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
UNIT

ok "Файл ${SERVICE_FILE} создан"

# ── Активация ──────────────────────────────────────────────────
systemctl daemon-reload
ok "systemd daemon перезагружен"

systemctl enable "${SERVICE_NAME}"
ok "Сервис включён (autostart при загрузке)"

systemctl restart "${SERVICE_NAME}"
sleep 2

# ── Проверка статуса ──────────────────────────────────────────
if systemctl is-active --quiet "${SERVICE_NAME}"; then
    ok "Сервис запущен и работает"
else
    warn "Сервис не запустился. Проверь логи:"
    echo "      journalctl -u ${SERVICE_NAME} -n 30 --no-pager"
    exit 1
fi

# ── Итог ──────────────────────────────────────────────────────
LOCAL_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "localhost")
echo ""
echo -e "${GREEN}=== Готово! ===${NC}"
echo -e "UI:      ${CYAN}http://${LOCAL_IP}:${PORT}${NC}"
echo ""
echo "Управление сервисом:"
echo "  sudo systemctl status  ${SERVICE_NAME}"
echo "  sudo systemctl stop    ${SERVICE_NAME}"
echo "  sudo systemctl start   ${SERVICE_NAME}"
echo "  sudo systemctl restart ${SERVICE_NAME}"
echo "  journalctl -u ${SERVICE_NAME} -f          # follow логи"
echo ""
echo "Удалить сервис:"
echo "  sudo ./install-service.sh --uninstall"
echo ""
