```
  ____       _                  ___          _               _             _
 / ___| __ _| | ___ _ __ __ _ / _ \ _ __ __| |__  ___  ___| |_ _ __ __ _| |_ ___  _ __
| |  _ / _` | |/ _ \ '__/ _` | | | | '__/ _` '_ \/ _ \/ __| __| '__/ _` | __/ _ \| '__|
| |_| | (_| | |  __/ | | (_| | |_| | | | (_| | | |  __/\__ \ |_| | | (_| | || (_) | |
 \____|\__,_|_|\___|_|  \__,_|\___/|_|  \__,_|_| |_\___||___/\__|_|  \__,_|\__\___/|_|
```

**Self-hosted web UI for monitoring and managing MariaDB + Galera Cluster**

[![Python](https://img.shields.io/badge/python-3.9%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Last Commit](https://img.shields.io/github/last-commit/Leg1onary/galera_orchestrator)](https://github.com/Leg1onary/galera_orchestrator/commits/main)

---

## ✨ Features

- **Real-time dashboard** — WebSocket-powered cluster status with pulsing health indicators in the sidebar
- **Multi-cluster / multi-contour** — separate test and prod contours, multiple clusters per contour, switchable from the header
- **Bootstrap Wizard** — automatically selects the best bootstrap candidate by `wsrep_last_committed` (seqno); one-click cluster recovery
- **Maintenance Wizard** — guided 7-step workflow: set R/O → drain write queue → stop node → perform work → start node → wait for sync → restore R/W
- **Topology page** — wsrep variable comparison table across all nodes, with descriptive tooltips for 15 key variables
- **Node cards with sparklines** — live `flow_control` and `recv_queue` trend charts per node
- **Search on Nodes page** — filter nodes by name, host, or status in real time
- **Browser notifications** — desktop alerts on cluster degradation events
- **Mock mode** — simulate a full cluster in the browser with no real connections; includes preset failure scenarios
- **SSH + MySQL** — real mode connects via PyMySQL to MariaDB and via Paramiko SSH to `garbd` arbitrators
- **Optional authentication** — JWT + bcrypt login page; disabled by default for quick self-hosted use
- **55 REST API endpoints** — full Swagger UI at `/docs`
- **8 pages**: Overview, Nodes, Topology, Recovery, Maintenance, Diagnostics, Settings, Docs
- **Zero frontend build step** — single ~7800-line `index.html`, vanilla JS, no Node.js required

---

## 📸 Screenshots

Screenshots coming soon — run locally to see the UI.

---

## 🚀 Quick Start

**Requirements:**
- Python 3.9+
- Linux (Debian/Ubuntu/Astra, RHEL/CentOS/AlmaLinux/Rocky, RED OS)
- SSH key access to cluster nodes (no passphrase)

```bash
git clone https://github.com/Leg1onary/galera_orchestrator.git
cd galera_orchestrator
./deploy.sh
```

`deploy.sh` creates a Python virtualenv, installs all dependencies, and copies `nodes.example.yaml` → `nodes.yaml`.

Edit the config with your node details:

```bash
nano config/nodes.yaml   # fill in node IPs, SSH key paths, DB credentials
```

Then start the server:

```bash
./run.sh
```

Open **http://localhost:8000** in your browser.

> **Optional: run as a systemd service**
> ```bash
> sudo ./install-service.sh
> ```

---

## ⚙️ Configuration

All configuration lives in `config/nodes.yaml`. The file is YAML with three top-level sections:

### Contours and clusters

```yaml
contours:
  test:
    clusters:
      - name: test-cluster-1
        nodes:
          - id: gc01
            host: 192.168.1.10
            port: 3306
            ssh_port: 22
            ssh_user: root
            ssh_key: /root/.ssh/id_rsa   # absolute path, no passphrase
            dc: DC1                       # datacenter label (informational)
            enabled: true
        arbitrators:
          - id: arb-test-01
            host: 192.168.1.12
            ssh_port: 22
            ssh_user: root
            ssh_key: /root/.ssh/id_rsa
            dc: DC1
            enabled: true
  prod:
    clusters:
      - name: prod-cluster-1
        nodes: [ ... ]
```

You can add multiple clusters inside a contour — the header dropdown lets you switch between them at runtime.

### Database credentials

```yaml
db:
  user: monitor_user
  password: CHANGE_ME
```

Create the MariaDB monitor user:

```sql
GRANT SELECT, PROCESS, REPLICATION CLIENT ON *.* 
  TO 'monitor_user'@'%' IDENTIFIED BY 'strong_password';
```

Per-node overrides (`db_user`, `db_password`) are supported on individual node entries.

### Poll interval

```yaml
settings:
  poll_interval: 5   # seconds between real-mode polling cycles
```

---

## 🏗️ Architecture

```
galera_orchestrator/
├── backend/
│   ├── main.py           # FastAPI app — all 55 REST endpoints + WebSocket
│   ├── galera_client.py  # PyMySQL → MariaDB, Paramiko → garbd SSH
│   ├── mock_data.py      # In-memory cluster simulation
│   ├── auth.py           # JWT + bcrypt authentication
│   └── config.py         # Pydantic v2 settings, nodes.yaml loader
├── frontend/
│   └── index.html        # Single-file SPA (~7800 lines, vanilla JS)
├── config/
│   ├── nodes.example.yaml
│   └── ui_prefs.json     # Persisted UI preferences (contour, cluster, mode)
├── deploy.sh             # First-run setup (venv + deps + config scaffold)
├── run.sh                # Start uvicorn server
└── install-service.sh    # Install as systemd service
```

**Data flow:**

```
Browser  ←─WebSocket──┐
   │                  │
   └──HTTP/REST──► FastAPI (uvicorn)
                       ├── PyMySQL ──► MariaDB nodes
                       └── Paramiko ─► garbd arbitrators (SSH)
```

The frontend is a single HTML file served as a static asset. No build toolchain, no package manager — just open `index.html` or serve it through FastAPI.

---

## 🎭 Mock Mode

Mock mode simulates an entire Galera cluster entirely in the browser — no database connections are made. It is the default mode on a fresh install and is ideal for exploration, demos, and UI development.

**Built-in scenarios** (selectable from the UI):

| Scenario | Description |
|---|---|
| `gc01_down` | Node gc01 is offline; remaining nodes quorate |
| `gc02_down` | Node gc02 is offline |
| `flow_control` | Cluster under write pressure; flow control active |
| `sst_in_progress` | A new node is performing State Snapshot Transfer |

Switch between mock and real mode from the Settings page or the sidebar toggle — the choice is saved in `config/mode.json`.

---

## 🔐 Authentication

Authentication is **disabled by default**. Enable it in `config/nodes.yaml`:

```yaml
auth:
  enabled: true
  username: admin
  password_hash: ""        # bcrypt hash — generate with the command below
  token_expire_hours: 24
  secret_key: "change-me-to-a-random-32+-char-string"
```

Generate a bcrypt password hash:

```bash
python3 -c "import bcrypt; print(bcrypt.hashpw(b'mypassword', bcrypt.gensalt()).decode())"
```

Paste the output into `password_hash`. Restart the server. The UI will show a login page and issue a JWT token stored in `localStorage`.

> **Note:** Always replace `secret_key` with a strong random value before enabling auth in production.

---

## 🛠️ Development

```bash
# Clone and set up
git clone https://github.com/Leg1onary/galera_orchestrator.git
cd galera_orchestrator
./deploy.sh

# Start with auto-reload
source venv/bin/activate
cd backend
uvicorn main:app --reload --port 8000
```

The full REST API is documented at **http://localhost:8000/docs** (Swagger UI) and **http://localhost:8000/redoc** (ReDoc).

**Stack:**

| Layer | Technology |
|---|---|
| Backend | FastAPI 0.109, Uvicorn |
| Database client | PyMySQL 1.1 |
| SSH client | Paramiko 3.4 |
| Schema validation | Pydantic v2 |
| Auth | python-jose (JWT), bcrypt |
| Frontend | Vanilla JS, single HTML file |
| Config | PyYAML |

No Docker image is provided — the deploy script handles virtualenv setup natively on the target host.

---

## 📄 License

[MIT](LICENSE) — free to use, modify, and distribute.
