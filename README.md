# Galera Orchestrator

Веб-интерфейс для мониторинга и управления кластером **MariaDB + Galera Cluster**.  
Устанавливается на одну из нод кластера, мониторит её и «соседние».

---

## Установка

### 1. Клонировать репозиторий

```bash
cd /opt
git clone https://github.com/Leg1onary/galera_orchestrator.git
cd galera_orchestrator
```

### 2. Запустить установщик

```bash
./deploy.sh
```

Скрипт автоматически:
- проверит наличие Python 3
- создаст виртуальное окружение `venv/`
- установит все зависимости
- создаст `config/nodes.yaml` из шаблона (если ещё не существует)

### 3. Отредактировать конфигурацию

```bash
nano config/nodes.yaml
```

Что обязательно заполнить:

| Поле | Описание |
|------|----------|
| `nodes[].host` | IP-адрес каждой ноды |
| `nodes[].ssh_key` | Абсолютный путь к SSH-ключу |
| `db.password` | Пароль monitor_user в MariaDB |

> Полный пример с комментариями — `config/nodes.example.yaml`

### 4. Запустить

```bash
./run.sh
```

Скрипт выведет адрес UI, например:
```
UI:  http://192.168.1.10:8000
```

Открыть в браузере на любой машине в сети.

---

## Настройка monitor_user в MariaDB

Выполнить **на каждой ноде** кластера перед переключением в Real-режим:

```sql
mysql -u root -p

CREATE USER 'monitor_user'@'%' IDENTIFIED BY 'strong_password';
GRANT SELECT, PROCESS, REPLICATION CLIENT ON *.* TO 'monitor_user'@'%';
FLUSH PRIVILEGES;
```

Минимально необходимые права:
- `SELECT` — для `SHOW STATUS LIKE 'wsrep%'`
- `PROCESS` — для `SHOW PROCESSLIST`
- `REPLICATION CLIENT` — для `SHOW MASTER STATUS`

---

## Настройка SSH-доступа между нодами

Backend ходит по SSH на ноды для управляющих команд (Start/Stop/Restart/Bootstrap).  
Ключ должен быть **без пароля**:

```bash
# На ноде где установлен оркестратор:
ssh-keygen -t ed25519 -f /root/.ssh/galera_orch -N ""

# Скопировать ключ на каждую ноду:
ssh-copy-id -i /root/.ssh/galera_orch.pub root@192.168.1.10
ssh-copy-id -i /root/.ssh/galera_orch.pub root@192.168.1.11

# Проверить:
ssh -i /root/.ssh/galera_orch root@192.168.1.11 "echo ok"
```

В `config/nodes.yaml` указать абсолютный путь:
```yaml
ssh_key: /root/.ssh/galera_orch
```

---

## Автозапуск через systemd

```bash
sudo nano /etc/systemd/system/galera-orchestrator.service
```

```ini
[Unit]
Description=Galera Orchestrator UI
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/galera_orchestrator/backend
ExecStart=/opt/galera_orchestrator/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always
User=root

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now galera-orchestrator
sudo systemctl status galera-orchestrator
```

---

## Использование

### Режимы данных

Переключатель **MOCK | REAL** находится в шапке. Выбранный режим сохраняется между перезагрузками страницы.

| Режим | Описание |
|-------|----------|
| **MOCK** | Симуляция — данные генерируются в браузере. Для демонстрации без кластера. |
| **REAL** | Реальный кластер — опрашивает ноды через MariaDB TCP + SSH. |

При переключении **REAL → MOCK** приложение покажет предупреждение, что данные мониторинга будут сброшены.

### Контуры

Переключатель **TEST | PROD** в шапке.

| Контур | Описание |
|--------|----------|
| **TEST** | 2 ноды без арбитра |
| **PROD** | 2 ноды + garbd арбитр (в Настройках появится раздел арбитра) |

### Действия на нодах

Доступны прямо с карточки ноды на странице «Обзор»:

| Кнопка | Команда на ноде |
|--------|----------------|
| **Start** | `systemctl start mariadb.service` |
| **Stop** | `systemctl stop mariadb.service` |
| **Restart** | `systemctl restart mariadb.service` |
| **Rejoin** | Перезапуск + ожидание IST/SST синхронизации |
| **R/O** | `SET GLOBAL read_only = ON` |
| **R/W** | `SET GLOBAL read_only = OFF` |
| **Ping** | SSH-проверка доступности + `systemctl is-active mariadb` |

---

## Структура проекта

```
galera_orchestrator/
├── backend/
│   ├── main.py              # FastAPI — все HTTP endpoints
│   ├── galera_client.py     # Подключение к MariaDB, сбор wsrep-метрик
│   ├── mock_data.py         # Генерация mock-данных и сценариев
│   ├── config.py            # Загрузка/сохранение nodes.yaml
│   └── requirements.txt
├── config/
│   ├── nodes.yaml           # Конфигурация — НЕ в git (содержит пароли)
│   └── nodes.example.yaml   # Шаблон с комментариями
├── frontend/
│   └── index.html           # Весь UI — одностраничное SPA
├── deploy.sh                # Установка зависимостей
├── run.sh                   # Запуск
└── README.md
```

---

## Требования

- Python 3.9+
- Git
- Доступ по SSH к нодам кластера (без пароля)
- Пользователь `monitor_user` в MariaDB на каждой ноде
