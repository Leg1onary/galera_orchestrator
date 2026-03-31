#!/bin/bash
# Galera Orchestrator — обновление до последней версии из репозитория
# Использование: sudo ./update.sh
#           или: ./update.sh (если сервис запущен от текущего пользователя)

set -e

SERVICE_NAME="galera-orchestrator"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[OK]${NC}    $*"; }
info() { echo -e "${CYAN}[...]${NC}   $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

echo ""
echo -e "${CYAN}=== Galera Orchestrator — обновление ===${NC}"
echo ""

# ── Проверяем git ──────────────────────────────────────────────
command -v git &>/dev/null || err "git не установлен"
[[ -d "${DIR}/.git" ]] || err "Директория ${DIR} — не git-репозиторий. Клонируй заново."

# ── Показываем текущую версию ─────────────────────────────────
CURRENT=$(git -C "$DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")
info "Текущий коммит : ${CURRENT}"
info "Ветка          : $(git -C "$DIR" branch --show-current 2>/dev/null || echo 'unknown')"
echo ""

# ── Проверяем, нет ли несохранённых изменений ─────────────────
if ! git -C "$DIR" diff --quiet 2>/dev/null; then
    warn "Есть незакоммиченные изменения в рабочей директории."
    warn "Они будут перезаписаны. Если хочешь сохранить — сначала сделай git stash."
    echo ""
    read -r -p "Продолжить? [y/N] " CONFIRM
    [[ "${CONFIRM,,}" == "y" ]] || { echo "Отменено."; exit 0; }
fi

# ── git pull ──────────────────────────────────────────────────
info "Получаем обновления из репозитория..."
git -C "$DIR" pull --ff-only origin "$(git -C "$DIR" branch --show-current)" \
    || git -C "$DIR" pull origin main \
    || err "git pull завершился с ошибкой. Проверь подключение к интернету и права."

NEW=$(git -C "$DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")
if [[ "$CURRENT" == "$NEW" ]]; then
    ok "Уже актуальная версия (${NEW}). Обновлять нечего."
    echo ""
    exit 0
fi
ok "Обновлено: ${CURRENT} → ${NEW}"

# ── Обновляем зависимости Python (если изменился requirements.txt) ────────────
if git -C "$DIR" diff --name-only "${CURRENT}" "${NEW}" 2>/dev/null | grep -q "requirements.txt"; then
    info "requirements.txt изменился — обновляем зависимости..."
    VENV="${DIR}/venv"
    if [[ -f "${VENV}/bin/pip" ]]; then
        "${VENV}/bin/pip" install -q -r "${DIR}/backend/requirements.txt"
        ok "Зависимости обновлены"
    else
        warn "venv не найден — пропускаем обновление зависимостей"
    fi
fi

# ── Перезапускаем сервис ──────────────────────────────────────
if systemctl is-active --quiet "${SERVICE_NAME}" 2>/dev/null; then
    info "Перезапускаем сервис ${SERVICE_NAME}..."
    # Нужны права sudo для restart
    if [[ $EUID -ne 0 ]]; then
        sudo systemctl restart "${SERVICE_NAME}" \
            || warn "Не удалось перезапустить через sudo. Запусти вручную: sudo systemctl restart ${SERVICE_NAME}"
    else
        systemctl restart "${SERVICE_NAME}"
    fi
    sleep 1
    if systemctl is-active --quiet "${SERVICE_NAME}"; then
        ok "Сервис перезапущен и работает"
    else
        warn "Сервис не запустился после обновления!"
        echo "      Логи: journalctl -u ${SERVICE_NAME} -n 30 --no-pager"
        exit 1
    fi
else
    warn "Сервис ${SERVICE_NAME} не запущен. Запусти вручную: sudo systemctl start ${SERVICE_NAME}"
fi

# ── Показываем changelog ──────────────────────────────────────
echo ""
info "Изменения в этом обновлении:"
git -C "$DIR" log --oneline "${CURRENT}..${NEW}" 2>/dev/null || true

echo ""
echo -e "${GREEN}=== Обновление завершено (${NEW}) ===${NC}"
echo ""
echo "Фронтенд обновится в браузере после Ctrl+Shift+R (жёсткий перезагрузка)."
echo ""
