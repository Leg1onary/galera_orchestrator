# Galera Orchestrator

Веб-интерфейс для мониторинга и управления кластером **MariaDB + Galera Cluster**.  
Устанавливается на одну из нод кластера, мониторит её и «соседние».

---

## Быстрый старт

### 1. Клонировать и настроить окружение

```bash
git clone https://github.com/Leg1onary/galera_orchestrator.git
cd galera_orchestrator
python3 -m venv venv
source venv/bin/activate          # Linux
# venv\Scripts\activate           # Windows
pip install -r backend/requirements.txt
```

### 2. Настроить конфигурацию

```bash
cp config/nodes.example.yaml config/nodes.yaml
nano config/nodes.yaml
```

Заполнить:
- `nodes` — список нод кластера (ID, IP, SSH-ключ)
- `db.user` / `db.password` — credentials для monitor-пользователя MariaDB
- `cluster.environment` — `test` или `prod`

### 3. Запустить backend

```bash
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Открыть в браузере: `http://<IP_ноды>:8000`

---

## Настройка monitor_user в MariaDB

Перед переключением в **Real-режим** нужно создать пользователя для мониторинга на **каждой ноде**:

```sql
-- Подключиться к MariaDB на каждой ноде:
mysql -u root -p

-- Создать пользователя (заменить 'strong_password'):
CREATE USER 'monitor_user'@'%' IDENTIFIED BY 'strong_password';

-- Выдать необходимые права:
GRANT SELECT, PROCESS, REPLICATION CLIENT ON *.* TO 'monitor_user'@'%';

-- Применить:
FLUSH PRIVILEGES;

-- Проверить:
SHOW GRANTS FOR 'monitor_user'@'%';
```

> **Минимально необходимые права:**
> - `SELECT` — для `SHOW STATUS LIKE 'wsrep%'`
> - `PROCESS` — для `SHOW PROCESSLIST`  
> - `REPLICATION CLIENT` — для `SHOW MASTER STATUS`

---

## Использование

### Режимы данных

| Режим | Описание |
|-------|----------|
| **MOCK** | Симуляция — данные генерируются в браузере. Для разработки и демонстрации. |
| **REAL** | Реальный кластер — backend опрашивает ноды через TCP (MariaDB) и SSH. |

Переключатель **MOCK \| REAL** находится в шапке приложения.  
Выбранный режим сохраняется и не сбрасывается при перезагрузке страницы.

### Контуры

| Контур | Описание |
|--------|----------|
| **TEST** | 2 ноды без арбитра |
| **PROD** | 2 ноды + garbd арбитр |

При переключении на PROD — в Настройках появляется раздел арбитра, в Топологии отображается garbd.

### Действия на нодах

Доступны прямо с карточки ноды:

| Кнопка | Действие |
|--------|---------|
| **Start** | `systemctl start mariadb.service` |
| **Stop** | `systemctl stop mariadb.service` |
| **Restart** | `systemctl restart mariadb.service` |
| **Rejoin** | Перезапуск для переподключения к кластеру (IST/SST) |
| **R/O** | `SET GLOBAL read_only = ON` |
| **R/W** | `SET GLOBAL read_only = OFF` |
| **Ping** | Проверка SSH-доступности + `systemctl is-active mariadb` |

---

## Структура проекта

```
galera_orchestrator/
├── backend/
│   ├── main.py              # FastAPI — HTTP endpoints
│   ├── galera_client.py     # Подключение к MariaDB, сбор wsrep-метрик
│   ├── mock_data.py         # Генерация mock-данных
│   ├── config.py            # Загрузка/сохранение nodes.yaml
│   └── requirements.txt
├── config/
│   ├── nodes.yaml           # Конфигурация (НЕ в git — содержит пароли)
│   └── nodes.example.yaml   # Шаблон конфигурации
├── frontend/
│   └── index.html           # Весь UI — SPA
└── README.md
```

---

## Linux: systemd unit

```ini
# /etc/systemd/system/galera-orchestrator.service
[Unit]
Description=Galera Orchestrator UI
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/galera-orchestrator/backend
ExecStart=/opt/galera-orchestrator/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always
User=galera-orch

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable --now galera-orchestrator
```

---

## SSH-ключ для доступа к нодам

Backend использует SSH для выполнения команд (`systemctl`, `galera_new_cluster`).  
Ключ должен быть без парольной защиты:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/galera_orch -N ""
ssh-copy-id -i ~/.ssh/galera_orch.pub root@<IP_ноды>
```

В `nodes.yaml` указать абсолютный путь:
```yaml
ssh_key: /home/user/.ssh/galera_orch
```
