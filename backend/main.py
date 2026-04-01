import asyncio, json, logging, os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional


from config import load_config, save_config
from galera_client import get_cluster_status
from mock_data import set_scenario, get_scenario

# ── Shared SSH helper ────────────────────────────────────────
def ssh_run(node: dict, *cmds: str, timeout: int = 30) -> list:
    """Open ONE SSH connection, run all cmds sequentially.

    Returns list of (exit_code, stdout, stderr) tuples.
    Raises ``paramiko.SSHException`` / ``socket.error`` on connection failure.
    """
    try:
        import paramiko
    except ImportError:
        raise RuntimeError("paramiko not installed. Run: pip install paramiko")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        node.get("host"), port=int(node.get("ssh_port", 22)),
        username=node.get("ssh_user", "root"),
        key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
        timeout=10,
    )
    results = []
    try:
        for cmd in cmds:
            _, so, se = client.exec_command(cmd, timeout=timeout)
            out = so.read().decode(errors="replace").strip()
            err = se.read().decode(errors="replace").strip()
            ec  = so.channel.recv_exit_status()
            results.append((ec, out, err))
    finally:
        client.close()
    return results


# ── Persistent event log ─────────────────────────────────────────
_LOG_DIR  = Path(__file__).parent.parent / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_LOG_FILE = _LOG_DIR / "events.log"

_event_log: deque = deque(maxlen=500)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("galera_orchestrator")


def _push_event(level: str, msg: str, source: str = "system"):
    entry = {
        "ts":     datetime.utcnow().isoformat() + "Z",
        "level":  level.upper(),
        "msg":    msg,
        "source": source,
    }
    _event_log.append(entry)
    try:
        with open(_LOG_FILE, "a") as fh:
            fh.write(json.dumps(entry) + "\n")
    except Exception:
        pass
    # Broadcast via WebSocket manager stored in app.state (set during lifespan).
    # Falls back gracefully when called before startup (e.g. import-time code).
    try:
        mgr = app.state.ws_manager
        loop = asyncio.get_running_loop()
        loop.create_task(mgr.broadcast({"type": "event", **entry}))
    except RuntimeError:
        pass
    except Exception:
        pass


# ── WebSocket manager ────────────────────────────────────────────
class _WsManager:
    """
    Tracks active WebSocket connections and broadcasts JSON messages.
    All public methods are coroutines and must be called from an async context.

    This class intentionally holds no reference to ``app`` or any module-level
    hidden global dependency — the manager is injected via FastAPI's
    application state during lifespan startup.
    """
    def __init__(self):
        self._connections: list = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._connections.append(ws)

    def disconnect(self, ws: WebSocket):
        self._connections = [c for c in self._connections if c is not ws]

    async def broadcast(self, data: dict):
        dead = []
        for ws in list(self._connections):
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    async def shutdown(self):
        for ws in list(self._connections):
            try:
                await ws.close()
            except Exception:
                pass
        self._connections.clear()


# ── Rate limiter for SSH actions ─────────────────────────────────
import time as _time
from collections import defaultdict

_action_calls: dict = defaultdict(list)
_RATE_LIMIT_MAX    = 5   # max SSH action requests per node
_RATE_LIMIT_WINDOW = 60  # seconds

def _check_rate_limit(node_id: str):
    now = _time.monotonic()
    window_start = now - _RATE_LIMIT_WINDOW
    _action_calls[node_id] = [t for t in _action_calls[node_id] if t > window_start]
    if len(_action_calls[node_id]) >= _RATE_LIMIT_MAX:
        raise HTTPException(
            429,
            f"Rate limit exceeded for node '{node_id}': "
            f"max {_RATE_LIMIT_MAX} SSH actions per {_RATE_LIMIT_WINDOW}s. Try again later."
        )
    _action_calls[node_id].append(now)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Attach WebSocket manager to app.state so every part of the codebase
    # can reach it via ``app.state.ws_manager`` without touching module globals.
    app.state.ws_manager = _WsManager()

    cfg   = load_config()
    nodes = [n["id"] for n in cfg.get("nodes", []) if n.get("enabled")]
    arbs  = [a for a in cfg.get("arbitrators", []) if a.get("enabled", True)]
    log.info(
        f"Starting Galera Orchestrator | nodes={len(nodes)} | "
        f"arbitrators={len(arbs)}"
    )
    _push_event("info", f"Galera Orchestrator started | nodes={nodes} | arbitrators={len(arbs)}", "system")
    yield
    # Graceful shutdown — disconnect all WebSocket clients
    await app.state.ws_manager.shutdown()


app = FastAPI(title="Galera Orchestrator", lifespan=lifespan)

FRONTEND = Path(__file__).parent.parent / "frontend"
ASSETS   = FRONTEND / "assets"
ASSETS.mkdir(parents=True, exist_ok=True)
app.mount("/assets", StaticFiles(directory=str(ASSETS)), name="assets")


@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse(str(FRONTEND / "index.html"))

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return FileResponse(str(FRONTEND / "favicon.ico"))

@app.get("/api/status")
async def api_status():
    global _prev_status
    try:
        cfg  = load_config()
        mode = cfg.get("settings", {}).get("use_mock", True)
        data = await asyncio.get_event_loop().run_in_executor(
            None, get_cluster_status, cfg
        )
        status = data.get("cluster", {}).get("status", "unknown")
        if status != _prev_status:
            _push_event("info", f"Cluster status changed: {_prev_status} → {status}", "monitor")
            _prev_status = status
        return data
    except Exception as e:
        log.error(f"api_status error: {e}")
        raise HTTPException(500, str(e))

_prev_status = None


@app.post("/api/scenario/{name}")
async def set_scenario_api(name: str):
    set_scenario(name)
    _push_event("info", f"Mock scenario set: {name}", "ui")
    return {"ok": True, "scenario": name}

@app.get("/api/scenario")
async def get_scenario_api():
    return {"scenario": get_scenario()}

@app.get("/api/config")
async def get_config():
    return load_config()

@app.get("/api/nodes")
async def list_nodes():
    cfg = load_config()
    nodes = cfg.get("nodes", [])
    return {"nodes": nodes}


@app.get("/api/node/{node_id}/test-connection")
async def test_node_connection(node_id: str):
    """SSH + DB connectivity check.

    Returns ``{ok, ssh: {ok, message}, db: {ok, message}}`` so the UI can
    display separate SSH and MariaDB status lines.
    """
    cfg   = load_config()
    nodes = cfg.get("nodes", [])
    node  = next((n for n in nodes if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    # ── SSH check ────────────────────────────────────────────
    try:
        [(ec, out, err)] = ssh_run(node, "echo ok", timeout=8)
        ssh_ok  = ec == 0 and out.strip() == "ok"
        ssh_msg = "Connected" if ssh_ok else (err or out or "Failed")
    except Exception as e:
        ssh_ok  = False
        ssh_msg = str(e)

    # ── DB check ─────────────────────────────────────────────
    db_ok  = False
    db_msg = "Not tested"
    try:
        import pymysql
        db_cfg   = cfg.get("db", {})
        db_port  = int(node.get("port") or node.get("db_port") or 3306)
        db_user  = node.get("db_user")     or db_cfg.get("user",     "root")
        db_pass  = node.get("db_password") or node.get("db_pass") or db_cfg.get("password", "")
        conn = pymysql.connect(
            host=node.get("host"), port=db_port,
            user=db_user, password=db_pass,
            connect_timeout=4,
        )
        conn.close()
        db_ok  = True
        db_msg = "Connected"
    except Exception as e:
        db_msg = str(e)

    return {
        "ok":  ssh_ok,
        "ssh": {"ok": ssh_ok, "message": ssh_msg},
        "db":  {"ok": db_ok,  "message": db_msg},
    }


class NodeActionRequest(BaseModel):
    action: str

@app.post("/api/node/{node_id}/action")
async def node_action(node_id: str, body: NodeActionRequest):
    """Execute a predefined SSH action on a single Galera node."""
    _check_rate_limit(node_id)

    cfg   = load_config()
    nodes = cfg.get("nodes", [])
    node  = next((n for n in nodes if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    msgs = {
        "stop":           "systemctl stop mariadb",
        "start":          "systemctl start mariadb",
        "restart":        "systemctl restart mariadb",
        "set_read_only":  "mysql -e \"SET GLOBAL read_only=1;\"",
        "set_read_write": "mysql -e \"SET GLOBAL read_only=0;\"",
    }
    cmd = msgs.get(body.action)
    if not cmd:
        raise HTTPException(400, f"Unknown action '{body.action}'. Allowed: {list(msgs)}")

    try:
        [(ec, out, err)] = ssh_run(node, cmd, timeout=30)
        ok = ec == 0
        msg = out or err or ("ok" if ok else "error")
        _push_event(
            "info" if ok else "error",
            f"Action '{body.action}' on {node_id}: {msg}",
            "ui",
        )
        return {"ok": ok, "msg": msg}
    except Exception as e:
        _push_event("error", f"Action '{body.action}' on {node_id} failed: {e}", "ui")
        raise HTTPException(500, str(e))


@app.get("/api/config/mode")
async def get_mode():
    cfg = load_config()
    return {"use_mock": cfg.get("settings", {}).get("use_mock", True)}

@app.post("/api/config/mode")
async def set_mode(request: Request):
    body = await request.json()
    use_mock = bool(body.get("use_mock", True))
    cfg = load_config()
    cfg.setdefault("settings", {})["use_mock"] = use_mock
    save_config(cfg)
    _push_event("info", f"Data mode changed to {'mock' if use_mock else 'real'}", "ui")
    return {"ok": True, "use_mock": use_mock}


class NodeConfig(BaseModel):
    id: str
    label: str
    host: str
    ssh_port: int = 22
    ssh_user: str = "root"
    ssh_key: str = "~/.ssh/id_rsa"
    db_port: int = 3306
    db_user: str = "root"
    db_pass: str = ""
    enabled: bool = True

@app.post("/api/config/node")
async def add_node(node: NodeConfig):
    cfg = load_config()
    nodes = cfg.get("nodes", [])
    if any(n["id"] == node.id for n in nodes):
        raise HTTPException(409, f"Node '{node.id}' already exists")
    node_dict = {
        "id":          node.id,
        "name":        node.label,        # NodeConfig.label  → YAML name
        "host":        node.host,
        "port":        node.db_port,      # NodeConfig.db_port → YAML port
        "ssh_port":    node.ssh_port,
        "ssh_user":    node.ssh_user,
        "ssh_key":     node.ssh_key,
        "db_user":     node.db_user,
        "db_password": node.db_pass,      # NodeConfig.db_pass → YAML db_password
        "enabled":     node.enabled,
    }
    nodes.append(node_dict)
    cfg["nodes"] = nodes
    save_config(cfg)
    _push_event("info", f"Node added: {node.id} ({node.host})", "ui")
    return {"ok": True, "node": node_dict}

@app.delete("/api/config/node/{node_id}")
async def delete_node(node_id: str):
    cfg = load_config()
    nodes = cfg.get("nodes", [])
    new_nodes = [n for n in nodes if n["id"] != node_id]
    if len(new_nodes) == len(nodes):
        raise HTTPException(404, f"Node '{node_id}' not found")
    cfg["nodes"] = new_nodes
    save_config(cfg)
    _push_event("info", f"Node removed: {node_id}", "ui")
    return {"ok": True}


class ArbitratorConfig(BaseModel):
    id: str
    label: str
    host: str
    ssh_port: int = 22
    ssh_user: str = "root"
    ssh_key: str = "~/.ssh/id_rsa"
    garbd_port: int = 4567
    enabled: bool = True

@app.post("/api/config/arbitrator")
async def add_arbitrator(arb: ArbitratorConfig):
    cfg = load_config()
    arbs = cfg.get("arbitrators", [])
    if any(a["id"] == arb.id for a in arbs):
        raise HTTPException(409, f"Arbitrator '{arb.id}' already exists")
    arbs.append(arb.dict())
    cfg["arbitrators"] = arbs
    save_config(cfg)
    _push_event("info", f"Arbitrator added: {arb.id} ({arb.host})", "ui")
    return {"ok": True, "arbitrator": arb.dict()}

@app.delete("/api/config/arbitrator/{arb_id}")
async def delete_arbitrator(arb_id: str):
    cfg = load_config()
    arbs = cfg.get("arbitrators", [])
    new_arbs = [a for a in arbs if a["id"] != arb_id]
    if len(new_arbs) == len(arbs):
        raise HTTPException(404, f"Arbitrator '{arb_id}' not found")
    cfg["arbitrators"] = new_arbs
    save_config(cfg)
    _push_event("info", f"Arbitrator removed: {arb_id}", "ui")
    return {"ok": True}

@app.delete("/api/config/arbitrator")
async def delete_all_arbitrators():
    cfg = load_config()
    cfg["arbitrators"] = []
    save_config(cfg)
    _push_event("info", "All arbitrators removed", "ui")
    return {"ok": True}

@app.put("/api/config/arbitrator/{arb_id}")
async def update_arbitrator(arb_id: str, request: Request):
    body = await request.json()
    cfg  = load_config()
    arbs = cfg.get("arbitrators", [])
    # Accept both list and single-object configs
    if isinstance(arbs, dict):
        arbs = [arbs]
    idx = next((i for i, a in enumerate(arbs) if a.get("id") == arb_id), None)
    if idx is None:
        raise HTTPException(404, f"Arbitrator '{arb_id}' not found")
    arbs[idx].update(body)
    cfg["arbitrators"] = arbs
    save_config(cfg)
    _push_event("info", f"Arbitrator updated: {arb_id}", "ui")
    return {"ok": True, "arbitrator": arbs[idx]}


class DBCredentials(BaseModel):
    db_user: str
    db_pass: str

@app.post("/api/config/db")
async def update_db_credentials(creds: DBCredentials):
    cfg   = load_config()
    nodes = cfg.get("nodes", [])
    for node in nodes:
        node["db_user"] = creds.db_user
        node["db_pass"] = creds.db_pass
    cfg["nodes"] = nodes
    save_config(cfg)
    _push_event("info", "DB credentials updated for all nodes", "ui")
    return {"ok": True}


@app.post("/api/reload")
async def reload_config_legacy():
    return {"ok": True, "msg": "Config reloaded (legacy endpoint)"}

@app.post("/api/config/reload")
async def reload_config():
    cfg = load_config()
    _push_event("info", "Config reloaded via API", "ui")
    return {"ok": True, "nodes": len(cfg.get("nodes", []))}

@app.get("/api/prefs")
async def get_prefs():
    cfg = load_config()
    return cfg.get("prefs", {})

@app.post("/api/prefs")
async def save_prefs(request: Request):
    body = await request.json()
    cfg  = load_config()
    cfg["prefs"] = body
    save_config(cfg)
    return {"ok": True}


@app.get("/api/garbd/{arb_id}/log")
async def garbd_log(arb_id: str, lines: int = 100):
    """SSH: tail the garbd log from the arbitrator host."""
    cfg  = load_config()
    arbs = cfg.get("arbitrators", [])
    arb  = next((a for a in arbs if a["id"] == arb_id), None)
    if not arb:
        raise HTTPException(404, f"Arbitrator '{arb_id}' not found")

    cmd = (
        f"journalctl -u garbd --no-pager -n {lines} 2>/dev/null "
        f"|| tail -n {lines} /var/log/garbd.log 2>/dev/null "
        f"|| echo 'Log not found'"
    )
    try:
        [(ec, out, err)] = ssh_run(arb, cmd, timeout=15)
        return {"ok": True, "log": out or err}
    except Exception as e:
        return {"ok": False, "log": str(e)}


class WsrepRecoverRequest(BaseModel):
    node_id: str

@app.post("/api/node/{node_id}/wsrep-recover")
async def wsrep_recover(node_id: str):
    """SSH: run galera_recovery / mysqld --wsrep-recover on a single node."""
    cfg   = load_config()
    nodes = cfg.get("nodes", [])
    node  = next((n for n in nodes if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    recover_cmd = (
        "galera_recovery 2>/dev/null "
        "|| mysqld --wsrep-recover 2>&1 | grep 'Recovered position' "
        "|| mariadbd --wsrep-recover 2>&1 | grep 'Recovered position'"
    )
    try:
        [(ec, out, err)] = ssh_run(node, recover_cmd, timeout=60)
        text = out or err
        import re
        m = re.search(r'Recovered position.*?(\d+:\d+)', text) or \
            re.search(r'position:\s*(\S+)',               text)
        seqno_str = m.group(1) if m else "unknown"
        return {"ok": True, "node_id": node_id, "seqno": seqno_str, "raw": text}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/bootstrap")
async def do_bootstrap(request: Request):
    """Bootstrap the Galera cluster from the node with the highest seqno.

    Pre-checks:
    1. Systemd service check: MariaDB must NOT be active on any non-candidate node.
    2. Candidate node must be determined (highest seqno or explicit node_id).
    """
    body = await request.json()
    node_id = body.get("node_id")

    cfg   = load_config()
    nodes = [n for n in cfg.get("nodes", []) if n.get("enabled")]
    if not nodes:
        raise HTTPException(400, "No enabled nodes in config")

    candidate = next((n for n in nodes if n["id"] == node_id), None) if node_id else None
    if not candidate:
        raise HTTPException(404, f"Node '{node_id}' not found")

    # ── Pre-check: ensure MariaDB is NOT running on other nodes ──────────
    other_nodes = [n for n in nodes if n["id"] != node_id]
    active_others = []
    for n in other_nodes:
        try:
            [(ec, out, _)] = ssh_run(n, "systemctl is-active mariadb.service 2>/dev/null || echo inactive", timeout=8)
            if out.strip() == "active":
                active_others.append(n["id"])
        except Exception:
            pass  # SSH failure — skip the check for this node

    if active_others:
        raise HTTPException(
            409,
            f"Cannot bootstrap: MariaDB is still active on node(s): {', '.join(active_others)}. "
            f"Stop MariaDB on those nodes first (systemctl stop mariadb)."
        )

    # ── Bootstrap ────────────────────────────────────────────────────────
    try:
        [(ec, out, err)] = ssh_run(
            candidate,
            "galera_new_cluster 2>&1 || systemctl start mariadb@bootstrap.service 2>&1",
            timeout=60,
        )
        ok  = ec == 0
        msg = out or err or ("Bootstrap started" if ok else "Bootstrap failed")
        _push_event(
            "info" if ok else "error",
            f"Bootstrap on {node_id}: {msg}", "ui"
        )
        return {"ok": ok, "msg": msg, "node_id": node_id}
    except Exception as e:
        _push_event("error", f"Bootstrap on {node_id} failed: {e}", "ui")
        raise HTTPException(500, str(e))


@app.post("/api/node/{node_id}/rejoin")
async def do_rejoin(node_id: str):
    """SSH: stop + start MariaDB on the given node to re-join the cluster."""
    cfg   = load_config()
    nodes = cfg.get("nodes", [])
    node  = next((n for n in nodes if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")
    try:
        results = ssh_run(
            node,
            "systemctl stop mariadb 2>&1",
            "sleep 2",
            "systemctl start mariadb 2>&1",
            timeout=60,
        )
        ok  = all(r[0] == 0 for r in results)
        msg = " | ".join(r[1] or r[2] or "ok" for r in results)
        _push_event("info" if ok else "error", f"Rejoin {node_id}: {msg}", "ui")
        return {"ok": ok, "msg": msg}
    except Exception as e:
        _push_event("error", f"Rejoin {node_id} failed: {e}", "ui")
        raise HTTPException(500, str(e))


@app.post("/api/node/{node_id}/sst-donor")
async def force_sst_donor(node_id: str, request: Request):
    """SSH: set wsrep_sst_donor on the recipient node."""
    body      = await request.json()
    donor_id  = body.get("donor_id")
    cfg       = load_config()
    nodes     = cfg.get("nodes", [])
    node      = next((n for n in nodes if n["id"] == node_id),  None)
    donor     = next((n for n in nodes if n["id"] == donor_id), None) if donor_id else None
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")
    donor_host = donor.get("host", donor_id) if donor else donor_id
    try:
        [(ec, out, err)] = ssh_run(
            node,
            f"mysql -e \"SET GLOBAL wsrep_sst_donor='{donor_host}'\"",
            timeout=15,
        )
        ok  = ec == 0
        msg = out or err or ("ok" if ok else "failed")
        _push_event("info" if ok else "error", f"SST donor set {donor_host} → {node_id}: {msg}", "ui")
        return {"ok": ok, "msg": msg}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/node/{node_id}/sst-status")
async def sst_status(node_id: str):
    """SSH + DB: monitor SST progress on the given node."""
    cfg   = load_config()
    nodes = cfg.get("nodes", [])
    node  = next((n for n in nodes if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    result = {
        "node_id":    node_id,
        "state":      "unknown",
        "recv_queue": 0,
        "send_queue": 0,
        "sst_method": None,
        "progress_pct": 0,
        "message":    "",
    }

    # DB query
    try:
        import pymysql
        conn = pymysql.connect(
            host=node.get("host"), port=int(node.get("db_port", 3306)),
            user=node.get("db_user", "root"), password=node.get("db_pass", ""),
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            for var, key in [
                ("wsrep_local_state_comment", "state"),
                ("wsrep_local_recv_queue",    "recv_queue"),
                ("wsrep_local_send_queue",    "send_queue"),
            ]:
                cur.execute(f"SHOW STATUS LIKE '{var}'")
                row = cur.fetchone()
                if row:
                    result[key] = row[1] if key == "state" else int(row[1])
        conn.close()
    except Exception:
        pass

    # SSH: detect active SST process via shared ssh_run()
    try:
        [(_, proc_out, _)] = ssh_run(
            node,
            "pgrep -la rsync 2>/dev/null || pgrep -la mariabackup 2>/dev/null || echo none",
            timeout=8
        )
        if "rsync" in proc_out:         result["sst_method"] = "rsync"
        elif "mariabackup" in proc_out: result["sst_method"] = "mariabackup"
    except Exception:
        pass

    state_progress = {
        "Synced": 100, "Joined": 95, "Donor/Desynced": 50,
        "Joining": 15, "Open": 5, "unknown": 0
    }
    result["progress_pct"] = state_progress.get(result["state"], 10)
    result["message"] = f"{node_id}: {result['state']} (recv_queue={result['recv_queue']})"
    return result


@app.get("/api/node/{node_id}/processlist")
async def get_processlist(node_id: str, min_time: int = 0):
    """DB: SHOW FULL PROCESSLIST filtered by minimum query time."""
    cfg   = load_config()
    nodes = cfg.get("nodes", [])
    node  = next((n for n in nodes if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")
    try:
        import pymysql
        conn = pymysql.connect(
            host=node.get("host"), port=int(node.get("db_port", 3306)),
            user=node.get("db_user", "root"), password=node.get("db_pass", ""),
            connect_timeout=5, cursorclass=pymysql.cursors.DictCursor,
        )
        with conn.cursor() as cur:
            cur.execute("SHOW FULL PROCESSLIST")
            rows = cur.fetchall()
        conn.close()
        if min_time:
            rows = [r for r in rows if (r.get("Time") or 0) >= min_time]
        return {"ok": True, "node_id": node_id, "processes": rows}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/api/node/{node_id}/kill-query")
async def kill_query(node_id: str, request: Request):
    """DB: KILL QUERY <id> on the given node."""
    body    = await request.json()
    proc_id = body.get("process_id")
    if not proc_id:
        raise HTTPException(400, "process_id required")
    cfg   = load_config()
    nodes = cfg.get("nodes", [])
    node  = next((n for n in nodes if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")
    try:
        import pymysql
        conn = pymysql.connect(
            host=node.get("host"), port=int(node.get("db_port", 3306)),
            user=node.get("db_user", "root"), password=node.get("db_pass", ""),
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute(f"KILL QUERY {int(proc_id)}")
        conn.close()
        _push_event("info", f"KILL QUERY {proc_id} on {node_id}", "ui")
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/config/compare-galera-cnf")
async def compare_galera_cnf():
    """SSH: read galera.cnf (or wsrep settings from my.cnf) from all nodes
    and return a diff-friendly structure for the topology comparison UI.
    """
    cfg   = load_config()
    nodes = [n for n in cfg.get("nodes", []) if n.get("enabled")]
    if not nodes:
        return {"ok": True, "nodes": [], "params": {}}

    import concurrent.futures, re as _re

    def _read_cnf(node):
        cmd = (
            "cat /etc/mysql/conf.d/galera.cnf 2>/dev/null "
            "|| cat /etc/mysql/mariadb.conf.d/galera.cnf 2>/dev/null "
            "|| grep -A 200 '\\[galera\\]' /etc/mysql/my.cnf 2>/dev/null "
            "|| grep -r 'wsrep' /etc/mysql/ 2>/dev/null | head -60"
        )
        try:
            [(ec, out, err)] = ssh_run(node, cmd, timeout=12)
            return node["id"], out or err, None
        except Exception as e:
            return node["id"], "", str(e)

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        for nid, raw, err in ex.map(_read_cnf, nodes):
            params = {}
            for line in raw.splitlines():
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, _, v = line.partition("=")
                    params[k.strip()] = v.strip()
            results[nid] = {"params": params, "raw": raw, "error": err}

    all_keys = set()
    for v in results.values():
        all_keys.update(v["params"])

    params_matrix = {}
    for key in sorted(all_keys):
        params_matrix[key] = {nid: results[nid]["params"].get(key, "") for nid in results}

    return {
        "ok":     True,
        "nodes":  [n["id"] for n in nodes],
        "params": params_matrix,
        "raw":    {nid: results[nid]["raw"]   for nid in results},
        "errors": {nid: results[nid]["error"] for nid in results if results[nid]["error"]},
        "cnf_path": "/etc/mysql/conf.d/galera.cnf",
    }


@app.get("/api/diagnostics/check-all")
async def check_all():
    """Run a comprehensive cluster health check across all nodes."""
    cfg   = load_config()
    nodes = [n for n in cfg.get("nodes", []) if n.get("enabled")]
    mode  = cfg.get("settings", {}).get("use_mock", True)

    results = []
    warnings = []
    errors   = []

    if mode:
        # Mock diagnostic results
        import random
        for node in nodes:
            nid = node["id"]
            results.append({
                "node_id":        nid,
                "status":         "ok",
                "wsrep_connected": True,
                "wsrep_ready":    True,
                "wsrep_state":    "Synced",
                "seqno":          random.randint(1000, 9999),
                "recv_queue":     0,
                "flow_control":   0.0,
            })
        return {
            "ok":       True,
            "mode":     "mock",
            "nodes":    results,
            "warnings": warnings,
            "errors":   errors,
            "summary":  f"Mock check: {len(nodes)} nodes OK",
        }

    # Real mode: DB + SSH checks
    import concurrent.futures
    try:
        import pymysql
    except ImportError:
        raise HTTPException(500, "pymysql not installed")

    wsrep_vars = [
        "wsrep_connected", "wsrep_ready", "wsrep_local_state_comment",
        "wsrep_last_committed", "wsrep_local_recv_queue", "wsrep_flow_control_paused",
    ]

    def _check_node(node):
        nid = node["id"]
        try:
            conn = pymysql.connect(
                host=node.get("host"), port=int(node.get("db_port", 3306)),
                user=node.get("db_user", "root"), password=node.get("db_pass", ""),
                connect_timeout=5,
            )
            row_data = {}
            with conn.cursor() as cur:
                for var in wsrep_vars:
                    cur.execute(f"SHOW STATUS LIKE '{var}'")
                    r = cur.fetchone()
                    if r:
                        row_data[var.replace("wsrep_", "")] = r[1]
            conn.close()
            return {"node_id": nid, "status": "ok", **row_data}
        except Exception as e:
            return {"node_id": nid, "status": "error", "error": str(e)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_check_node, nodes))

    for r in results:
        if r.get("status") == "error":
            errors.append(f"{r['node_id']}: {r.get('error', 'unknown')}")
        elif r.get("local_state_comment") not in ("Synced", None):
            warnings.append(f"{r['node_id']}: state={r.get('local_state_comment')}")

    return {
        "ok":       len(errors) == 0,
        "mode":     "real",
        "nodes":    results,
        "warnings": warnings,
        "errors":   errors,
        "summary":  f"{len(nodes)} nodes checked: {len(errors)} errors, {len(warnings)} warnings",
    }


@app.get("/api/node/{node_id}/innodb-status")
async def innodb_status(node_id: str):
    """DB: SHOW ENGINE INNODB STATUS — returns raw output for deadlock analysis."""
    cfg   = load_config()
    nodes = cfg.get("nodes", [])
    node  = next((n for n in nodes if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")
    try:
        import pymysql
        conn = pymysql.connect(
            host=node.get("host"), port=int(node.get("db_port", 3306)),
            user=node.get("db_user", "root"), password=node.get("db_pass", ""),
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute("SHOW ENGINE INNODB STATUS")
            row = cur.fetchone()
        conn.close()
        raw = row[2] if row and len(row) >= 3 else ""
        return {"ok": True, "node_id": node_id, "status": raw}
    except Exception as e:
        raise HTTPException(500, str(e))


# ── Module-level sentinel: used only during import/startup before lifespan runs.
# After lifespan startup ``app.state.ws_manager`` is the authoritative instance.
_ws_manager_sentinel = None


@app.websocket("/ws/cluster")
async def ws_cluster(websocket: WebSocket):
    """WebSocket endpoint — streams cluster events to connected browsers.
    Uses ``app.state.ws_manager``
    (set during lifespan) instead of a module-level global.
    """
    mgr = websocket.app.state.ws_manager
    await mgr.connect(websocket)
    try:
        # Send buffered log on connect
        await websocket.send_json({
            "type":   "log_snapshot",
            "events": list(_event_log),
        })
        while True:
            # Keep connection alive; actual pushes come from _push_event
            await asyncio.sleep(30)
            await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        mgr.disconnect(websocket)
    except Exception:
        mgr.disconnect(websocket)


# ── EVENT LOG API ─────────────────────────────────────────────
@app.get("/api/log")
async def get_log(limit: int = 200, level: str = ""):
    """Return recent events from the in-memory ring buffer."""
    entries = list(_event_log)
    if level:
        entries = [e for e in entries if e["level"] == level.upper()]
    return {"events": entries[:limit], "total": len(_event_log)}

@app.delete("/api/log")
async def clear_log():
    _event_log.clear()
    _push_event("info", "Event log cleared by user", "ui")
    return {"ok": True}


# ── VERSION / UPDATE CHECK ────────────────────────────────────
@app.get("/api/version")
async def api_version():
    """Return current local commit SHA and check GitHub for the latest commit.
    Uses a 5-minute server-side cache so we don't hammer the GitHub API.
    """
    import subprocess, time, urllib.request

    # ── local commit ──────────────────────────────────────────
    base_dir = Path(__file__).parent.parent
    try:
        local_sha = subprocess.check_output(
            ["git", "-C", str(base_dir), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL, timeout=5
        ).decode().strip()
        local_short = local_sha[:7]
        branch = subprocess.check_output(
            ["git", "-C", str(base_dir), "branch", "--show-current"],
            stderr=subprocess.DEVNULL, timeout=5
        ).decode().strip() or "master"
    except Exception:
        local_sha = "unknown"
        local_short = "unknown"
        branch = "master"

    # ── remote commit (cached 5 min) ──────────────────────────
    cache = getattr(api_version, "_cache", None)
    now   = time.time()
    if cache is None or (now - cache.get("ts", 0)) > 300:
        remote_sha   = None
        remote_short = None
        error        = None
        try:
            url = f"https://api.github.com/repos/Leg1onary/galera_orchestrator/commits/{branch}"
            req = urllib.request.Request(
                url,
                headers={"Accept": "application/vnd.github.v3+json",
                         "User-Agent": "galera-orchestrator"},
            )
            with urllib.request.urlopen(req, timeout=8) as resp:
                import json as _json
                data = _json.loads(resp.read())
                remote_sha   = data["sha"]
                remote_short = remote_sha[:7]
        except Exception as e:
            error = str(e)
        api_version._cache = {"ts": now, "remote_sha": remote_sha,
                               "remote_short": remote_short, "error": error}
    else:
        remote_sha   = cache["remote_sha"]
        remote_short = cache["remote_short"]
        error        = cache.get("error")

    up_to_date = (local_sha == remote_sha) if (local_sha != "unknown" and remote_sha) else None

    return {
        "local_sha":       local_sha,
        "local_short":     local_short,
        "remote_sha":      remote_sha,
        "remote_short":    remote_short,
        "branch":          branch,
        "up_to_date":      up_to_date,
        "update_available": (up_to_date is False),
        "github_url":      "https://github.com/Leg1onary/galera_orchestrator",
        "error":           error,
    }


# ── DISK / SYSTEM HEALTH ─────────────────────────────────────
@app.get("/api/diagnostics/system-health")
async def diagnostics_system_health():
    """SSH: collect df/free/uptime for every node in parallel.
    Returns per-node disk, memory, and load metrics with threshold flags.
    Thresholds: disk_warn=80%, disk_crit=90%; mem_warn=85%, mem_crit=95%.
    """
    import concurrent.futures, re as _re

    cfg   = load_config()
    nodes = [n for n in cfg.get("nodes", []) if n.get("enabled")]
    if not nodes:
        return {"ok": True, "nodes": []}

    DISK_WARN = 80; DISK_CRIT = 90
    MEM_WARN  = 85; MEM_CRIT  = 95

    def _collect(node):
        nid = node["id"]
        try:
            results = ssh_run(
                node,
                "df -h / --output=pcent 2>/dev/null | tail -1",
                "free -m 2>/dev/null | awk '/^Mem/{print $2, $3}'",
                "uptime 2>/dev/null",
                timeout=12,
            )
            disk_raw  = results[0][1].strip().rstrip("%") if results[0][1] else None
            mem_raw   = results[1][1].strip()             if results[1][1] else None
            uptime_raw = results[2][1].strip()            if results[2][1] else None

            disk_pct = int(disk_raw) if disk_raw and disk_raw.isdigit() else None
            mem_total, mem_used = (None, None)
            if mem_raw:
                parts = mem_raw.split()
                if len(parts) >= 2:
                    try:
                        mem_total = int(parts[0]); mem_used = int(parts[1])
                    except ValueError:
                        pass
            mem_pct = round(mem_used / mem_total * 100) if mem_total and mem_used else None

            load_avg = None
            if uptime_raw:
                m = _re.search(r'load average[s]?:\s*([\d.]+)', uptime_raw)
                if m:
                    load_avg = float(m.group(1))

            return {
                "node_id":   nid,
                "ok":        True,
                "disk_pct":  disk_pct,
                "disk_status": (
                    "crit" if (disk_pct and disk_pct >= DISK_CRIT) else
                    "warn" if (disk_pct and disk_pct >= DISK_WARN) else "ok"
                ) if disk_pct is not None else "unknown",
                "mem_pct":   mem_pct,
                "mem_used_mb":  mem_used,
                "mem_total_mb": mem_total,
                "mem_status": (
                    "crit" if (mem_pct and mem_pct >= MEM_CRIT) else
                    "warn" if (mem_pct and mem_pct >= MEM_WARN) else "ok"
                ) if mem_pct is not None else "unknown",
                "load_avg":  load_avg,
                "uptime":    uptime_raw,
            }
        except Exception as e:
            return {"node_id": nid, "ok": False, "error": str(e)}

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        node_results = list(ex.map(_collect, nodes))

    return {"ok": True, "nodes": node_results}


@app.get("/api/node/{node_id}/sst-status")
async def sst_status_2(node_id: str):
    """Alias for /api/node/{node_id}/sst-status (duplicate kept for backwards compat)."""
    return await sst_status(node_id)


# ── NODE SSH PING ─────────────────────────────────────────────
@app.get("/api/node/{node_id}/ping")
async def node_ping(node_id: str):
    """Quick SSH reachability check + systemctl is-active mariadb.service."""
    import time as _t
    cfg      = load_config()
    use_mock = cfg.get("settings", {}).get("use_mock", True)
    node     = next((n for n in cfg.get("nodes", []) if n["id"] == node_id), None)
    if not node:
        raise HTTPException(404, f"Node '{node_id}' not found")

    if use_mock:
        return {
            "ok":         True,
            "mock":       True,
            "node_id":    node_id,
            "reachable":  True,
            "latency_ms": 2,
            "service":    "active",
        }

    t0 = _t.monotonic()
    try:
        import paramiko
    except ImportError:
        raise HTTPException(500, "paramiko not installed")
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            node.get("host"), port=int(node.get("ssh_port", 22)),
            username=node.get("ssh_user", "root"),
            key_filename=str(Path(node.get("ssh_key", "~/.ssh/id_rsa")).expanduser()),
            timeout=6, banner_timeout=6,
        )
        _, so, _ = client.exec_command("systemctl is-active mariadb.service", timeout=5)
        service_state = so.read().decode(errors="replace").strip()
        client.close()
        latency = int((_t.monotonic() - t0) * 1000)
        return {
            "ok":         True,
            "mock":       False,
            "node_id":    node_id,
            "reachable":  True,
            "latency_ms": latency,
            "service":    service_state,
        }
    except Exception as e:
        latency = int((_t.monotonic() - t0) * 1000)
        return {
            "ok":         False,
            "mock":       False,
            "node_id":    node_id,
            "reachable":  False,
            "latency_ms": latency,
            "service":    "unknown",
            "error":      str(e),
        }


# ── ENTRYPOINT ────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
