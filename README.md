# Galera Orchestrator

UI + FastAPI backend for MariaDB Galera Cluster monitoring and management.

## Quick start (mock mode — no SSH needed)

```bash
git clone https://github.com/<you>/galera-orchestrator.git
cd galera-orchestrator
./deploy.sh       # install deps
./run.sh          # start backend
# open http://localhost:8000
```

## Configure nodes

Edit `config/nodes.yaml`:
- Add/remove nodes under `nodes:`
- Toggle `enabled: true/false`
- Enable arbitrator block if you have garbd

Then hit **"Reload nodes.yaml"** button in the UI (no restart needed).

## Connecting to real VMs

1. In `backend/galera_client.py` set `USE_MOCK_DATA = False`
2. Implement `_real_node_status()` via paramiko SSH (see TODO comments)
3. Fill in real IPs, SSH keys and MySQL credentials in `config/nodes.yaml`

## Project layout

```
galera-orchestrator/
├── backend/
│   ├── main.py           # FastAPI API + WebSocket polling
│   ├── config.py         # YAML config loader
│   ├── galera_client.py  # mock/real abstraction
│   └── mock_data.py      # wsrep mock scenarios
├── config/
│   └── nodes.yaml        # ← edit this
├── frontend/
│   └── index.html        # web UI
├── deploy.sh
└── run.sh
```
