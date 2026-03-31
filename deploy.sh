#!/bin/bash
# Galera Orchestrator — установка зависимостей и первоначальная настройка
# Поддерживаемые дистрибутивы: Astra Linux, Debian, Ubuntu, RHEL, CentOS, AlmaLinux, RED OS, Rocky
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# ── Цвета ──────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[OK]${NC}    $*"; }
info() { echo -e "${CYAN}[...]${NC}   $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

echo ""
echo -e "${CYAN}=== Galera Orchestrator — установка ===${NC}"
echo ""

# ── 1. Определяем дистрибутив ──────────────────────────────────
detect_distro() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        DISTRO_ID="${ID,,}"       # lowercase: astra, debian, ubuntu, rhel, centos...
        DISTRO_LIKE="${ID_LIKE,,}" # debian, rhel, fedora...
        DISTRO_NAME="${PRETTY_NAME:-$NAME}"
    elif command -v lsb_release &>/dev/null; then
        DISTRO_ID=$(lsb_release -si | tr '[:upper:]' '[:lower:]')
        DISTRO_LIKE=""
        DISTRO_NAME=$(lsb_release -sd)
    else
        DISTRO_ID="unknown"
        DISTRO_LIKE=""
        DISTRO_NAME="Unknown Linux"
    fi
}

detect_distro
info "Дистрибутив: ${DISTRO_NAME}"

# Определяем семейство пакетного менеджера
is_debian_based() {
    [[ "$DISTRO_ID" =~ ^(debian|ubuntu|astra|linuxmint|raspbian|kali|pop)$ ]] || \
    [[ "$DISTRO_LIKE" =~ debian ]] || \
    command -v apt-get &>/dev/null
}

is_rhel_based() {
    [[ "$DISTRO_ID" =~ ^(rhel|centos|almalinux|rocky|fedora|ol|redos|red)$ ]] || \
    [[ "$DISTRO_LIKE" =~ (rhel|fedora) ]] || \
    command -v dnf &>/dev/null || command -v yum &>/dev/null
}

# ── 2. Проверяем Python3 и устанавливаем если нужно ──────────
info "Проверяем Python3..."

PYTHON3_BIN=""
for candidate in python3.12 python3.11 python3.10 python3.9 python3; do
    if command -v "$candidate" &>/dev/null; then
        PYTHON3_BIN="$candidate"
        break
    fi
done

install_python_and_venv() {
    if is_debian_based; then
        info "Устанавливаем python3 и python3-venv (apt)..."
        if [[ $EUID -ne 0 ]]; then
            sudo apt-get update -qq
            sudo apt-get install -y python3 python3-venv python3-pip
        else
            apt-get update -qq
            apt-get install -y python3 python3-venv python3-pip
        fi
        # Перенаходим python3 после установки
        for candidate in python3.12 python3.11 python3.10 python3.9 python3; do
            if command -v "$candidate" &>/dev/null; then
                PYTHON3_BIN="$candidate"
                break
            fi
        done
    elif is_rhel_based; then
        info "Устанавливаем python3 (dnf/yum)..."
        PKG_MGR=$(command -v dnf || command -v yum)
        if [[ $EUID -ne 0 ]]; then
            sudo "$PKG_MGR" install -y python3 python3-pip
        else
            "$PKG_MGR" install -y python3 python3-pip
        fi
        PYTHON3_BIN="python3"
    else
        err "Не удалось определить пакетный менеджер. Установите Python 3.9+ вручную."
    fi
}

if [ -z "$PYTHON3_BIN" ]; then
    warn "Python3 не найден. Устанавливаем..."
    install_python_and_venv
fi

[ -z "$PYTHON3_BIN" ] && err "Python3 не найден даже после установки. Установите вручную."

PYTHON_VER=$("$PYTHON3_BIN" -c "import sys; print('%d.%d' % sys.version_info[:2])")
ok "Python: $("$PYTHON3_BIN" --version) (бинарник: $PYTHON3_BIN)"

# Минимальная версия Python 3.9
PYTHON_MAJOR=$("$PYTHON3_BIN" -c "import sys; print(sys.version_info.major)")
PYTHON_MINOR=$("$PYTHON3_BIN" -c "import sys; print(sys.version_info.minor)")
if [[ $PYTHON_MAJOR -lt 3 ]] || [[ $PYTHON_MAJOR -eq 3 && $PYTHON_MINOR -lt 9 ]]; then
    err "Требуется Python 3.9+, найден ${PYTHON_VER}. Обновите Python."
fi

# ── 3. Проверяем/устанавливаем модуль venv ────────────────────
info "Проверяем модуль venv..."

if ! "$PYTHON3_BIN" -c "import venv" 2>/dev/null; then
    warn "Модуль venv не найден. Устанавливаем..."
    if is_debian_based; then
        # На Astra/Debian пакет называется python3-venv
        # Для конкретных версий Python — pythonX.Y-venv
        VENV_PKG="python3-venv"
        # Если python3 — это конкретная версия, ищем pythonX.Y-venv
        if [[ "$PYTHON3_BIN" != "python3" ]]; then
            VERSIONED_PKG="python${PYTHON_VER}-venv"
            # Проверяем доступен ли versioned пакет
            if apt-cache show "$VERSIONED_PKG" &>/dev/null 2>&1; then
                VENV_PKG="$VERSIONED_PKG"
            fi
        fi
        info "Устанавливаем ${VENV_PKG}..."
        if [[ $EUID -ne 0 ]]; then
            sudo apt-get install -y "$VENV_PKG"
        else
            apt-get install -y "$VENV_PKG"
        fi
    elif is_rhel_based; then
        # На RHEL/CentOS/RED OS venv входит в python3-libs, но иногда нужен python3-virtualenv
        PKG_MGR=$(command -v dnf || command -v yum)
        if [[ $EUID -ne 0 ]]; then
            sudo "$PKG_MGR" install -y python3-virtualenv 2>/dev/null || \
            sudo "$PKG_MGR" install -y python3-pip && sudo pip3 install virtualenv
        else
            "$PKG_MGR" install -y python3-virtualenv 2>/dev/null || \
            "$PKG_MGR" install -y python3-pip && pip3 install virtualenv
        fi
    fi

    # Повторная проверка
    "$PYTHON3_BIN" -c "import venv" 2>/dev/null || \
        err "Модуль venv всё ещё недоступен. Попробуйте: sudo apt install python3-venv"
fi
ok "Модуль venv доступен"

# ── 4. Создаём virtualenv ─────────────────────────────────────
if [ ! -d venv ]; then
    info "Создаём виртуальное окружение (venv)..."
    "$PYTHON3_BIN" -m venv venv || err "Не удалось создать venv"
    ok "venv создан"
else
    ok "venv уже существует — пропускаем создание"
fi

# Активируем venv
# shellcheck disable=SC1091
source venv/bin/activate || err "Не удалось активировать venv (venv/bin/activate не найден)"
ok "venv активирован"

# ── 5. Обновляем pip и устанавливаем зависимости ──────────────
info "Обновляем pip..."
pip install -q --upgrade pip

info "Устанавливаем зависимости из backend/requirements.txt..."
pip install -q -r backend/requirements.txt
ok "Зависимости установлены"

# ── 6. Создаём конфиг если его нет ────────────────────────────
if [ ! -f config/nodes.yaml ]; then
    info "Создаём config/nodes.yaml из шаблона..."
    cp config/nodes.example.yaml config/nodes.yaml
    echo ""
    echo -e "${YELLOW}[!] Отредактируй config/nodes.yaml перед запуском:${NC}"
    echo "      - nodes[].host       — IP-адрес каждой ноды"
    echo "      - nodes[].ssh_key    — абсолютный путь к SSH-ключу"
    echo "      - db.password        — пароль monitor_user в MariaDB"
    echo ""
    echo "    Редактор: nano config/nodes.yaml"
else
    ok "config/nodes.yaml уже существует — не перезаписываем"
fi

# ── 7. Делаем скрипты исполняемыми ────────────────────────────
chmod +x run.sh 2>/dev/null && ok "run.sh — исполняемый" || true
chmod +x install-service.sh 2>/dev/null && ok "install-service.sh — исполняемый" || true

# ── Итог ──────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}=== Установка завершена ===${NC}"
echo ""
echo "Следующие шаги:"
echo "  1. Отредактируй конфиг:   nano config/nodes.yaml"
echo "  2. Запусти вручную:       ./run.sh"
echo "  3. Или как сервис:        sudo ./install-service.sh"
echo ""
